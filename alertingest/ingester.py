"""
A worker which copies alerts and schemas into an object store backend.
"""

import asyncio
import io
import logging
import ssl
import struct
from dataclasses import dataclass
from typing import Tuple

from aiokafka import AIOKafkaConsumer, ConsumerRecord
from aiokafka.helpers import create_ssl_context

from alertingest.schema_registry import SchemaRegistryClient
from alertingest.storage import AlertDatabaseBackend

logger = logging.getLogger(__name__)


@dataclass
class KafkaConnectionParams:
    """
    A bundle of data required to connect to Kafka.
    """

    host: str
    topics: list[str]
    group: str

    auth_mechanism: str

    username: str
    password: str

    client_key_path: str
    client_crt_path: str
    server_ca_crt_path: str

    @classmethod
    def with_scram(
        cls, host: str, topics: list[str], group: str, username: str, password: str
    ):
        """Instantiate a new param bundle using SCRAM auth."""
        return cls(
            host=host,
            topics=topics,
            group=group,
            auth_mechanism="scram",
            username=username,
            password=password,
            client_key_path="",
            client_crt_path="",
            server_ca_crt_path="",
        )

    @classmethod
    def with_mtls(
        cls,
        host: str,
        topics: list[str],
        group: str,
        client_key_path: str,
        client_crt_path: str,
        server_ca_crt_path: str,
    ):
        """Instantiate a new param bundle using mTLS auth."""
        return cls(
            host=host,
            topics=topics,
            group=group,
            auth_mechanism="mtls",
            client_key_path=client_key_path,
            client_crt_path=client_crt_path,
            server_ca_crt_path=server_ca_crt_path,
            username="",
            password="",
        )

    def _create_ssl_context(self) -> ssl.SSLContext:
        """
        Bundles the KafkaConnectionParams' SSL-related attributes into an
        SSL context.
        """
        assert self.auth_mechanism == "mtls"
        return create_ssl_context(
            cafile=self.server_ca_crt_path,
            certfile=self.client_crt_path,
            keyfile=self.client_key_path,
        )


class IngestWorker:
    def __init__(
        self,
        kafka_params: KafkaConnectionParams,
        backend: AlertDatabaseBackend,
        registry: SchemaRegistryClient,
        message_timeout: int = 1800,
        log_check_timeout: int = 3600,
    ):
        """
        Copies Rubin alert data from a Kafka broker to a database backend,
        using a schema registry to make sense of the alert data.

        All alert data is expected to be encoded in Confluent Wire Format.
        """
        self.kafka_params = kafka_params
        self.backend = backend
        self.schema_registry = registry
        self.message_timeout = message_timeout
        self.log_check_timeout = log_check_timeout

    async def run(
        self,
        limit: int = -1,
        commit_interval: int = 100,
        auto_offset_reset: str = "latest",
    ):
        """
        Run the consumer, copying messages from Kafka to the IngestWorker's
        backend.

        Parameters
        ----------
        limit : int
            Maximum number of messages to copy. If this value is less than 1,
            no limit is used. The default is -1.
        commit_interval : int
            Interval (measured in messages) between committing the offset of
            the worker. Higher values will require more repeated work if the
            IngestWorker crashes or backends are unavailable, while lower
            values will cost more overhead communicating with Kafka.
        auto_offset_reset : str
            When reading from a new topic, where should the worker start?
            Options are "latest" and "earliest".
        """
        consumer = self._create_consumer(auto_offset_reset)
        await consumer.start()

        try:
            # Combine all state information together to make things clearer
            state_tracker = {
                "commit_interval_counter": 0,  # Counter used to keep track of the submit interval
                "limit_n": 0,  # Counts messages if we are limiting number to send.
                "last_message_time": asyncio.get_event_loop().time(),
                "new_messages": True,  # Check if we are reading new messages
            }

            logger.info("ingest worker run loop start")
            while True:
                try:
                    logger.info("waiting for message")
                    msg = await asyncio.wait_for(
                        consumer.__anext__(), timeout=self.message_timeout
                    )

                    # Process messages and update the state tracker. Will set
                    # new_messages to True if new messages have been read.
                    state_tracker.update(
                        await self.process_message(
                            msg, consumer, **state_tracker, worker=self, limit=limit
                        )
                    )

                    # Check if the commit_interval has been reached and submit
                    # messages if it has
                    if state_tracker["commit_interval_counter"] == commit_interval:
                        state_tracker["commit_interval_counter"] = (
                            await self.handle_commit(
                                consumer, state_tracker["commit_interval_counter"]
                            )
                        )

                    # Check message limit
                    if limit > 0 and state_tracker["limit_n"] >= limit:
                        logger.info("limit reached - returning")
                        await self.handle_commit(
                            consumer, state_tracker["commit_interval_counter"]
                        )
                        return

                except asyncio.TimeoutError:
                    logger.info("waiting timed out, checking for new messages...")
                    # Check if we are reading new messages. If new messages
                    # is set to false, don't try and read the partitions.
                    # If new messages is set to true, we will try and read
                    # any remaining messages from the partitions.
                    current_time = asyncio.get_event_loop().time()
                    (
                        state_tracker["new_messages"],
                        state_tracker["commit_interval_counter"],
                    ) = await self.process_timeout(
                        consumer,
                        current_time,
                        state_tracker["last_message_time"],
                        state_tracker["commit_interval_counter"],
                        state_tracker["new_messages"],
                    )

        except Exception as e:
            logger.error(f"Error during message processing: {e}")
            raise

        finally:
            await consumer.stop()

    async def process_message(
        self,
        msg,
        consumer,
        last_message_time,
        commit_interval_counter,
        limit_n,
        worker,
        new_messages,
        limit=-1,
    ):
        """
        Process a single Kafka message.

        The function reads a single kafka message and updates the state
        tracker.

        Parameters
        ----------
        commit_interval_counter: int
            The number of messages since the last commit.

        limit_n : int
            Counts the number of messages which have been processed
            since the last commit. Will commit once the required number
            of messages has been reached and end the loop. Not tracked if
            limit is less than 1.

        worker : IngestWorker
            The ingester worker which is handling the message.

        new_messages : bool
            Track if we have received new messages, but keep the current state
            (to be updated upon timeout) if we have not.

        limit : int
            The maximum number of messages to process. If this value is less
            than 1, we do not track the number of messages processed.

        """
        logger.info(
            f"process_message called: topic={msg.topic}, partition={msg.partition}, "
            f"offset={msg.offset}, commit_counter={commit_interval_counter}"
        )

        worker.handle_kafka_message(msg)
        logger.info("handle_kafka_message completed successfully")

        if limit > 0:
            limit_n += 1
        will_return = True if msg else new_messages

        return {
            "last_message_time": asyncio.get_event_loop().time(),
            "commit_interval_counter": commit_interval_counter + 1,
            "limit_n": limit_n,
            "new_messages": will_return,
        }

    async def handle_commit(self, consumer, commit_interval_counter):
        """Handle committing of consumer offsets.

        Once the consumer has committed the new messages,
        commit_interval_counter is reset to 0 and we start counting again.

        Parameters
        ----------
        consumer : AIOKafkaConsumer
            The active kafka consumer reading the alert stream

        commit_interval_counter : int
            The number of messages since the last commit.
        """
        if commit_interval_counter > 0:
            # Log what we're about to commit
            logger.info(f"committing {commit_interval_counter} messages")
            for partition in consumer.assignment():
                position = await consumer.position(partition)
                logger.info(
                    f"committing partition {partition.partition} "
                    f"topic {partition.topic} at position {position}"
                )

            await consumer.commit()
            logger.info("commit complete")
            return 0
        return commit_interval_counter

    async def process_timeout(
        self,
        consumer,
        current_time,
        last_message_time,
        commit_interval_counter,
        new_messages,
    ):
        """Handle timeout scenario and check for remaining messages.

        If the timeout has been reached, the function checks if the new message
        flag is set to true. If new messages is false and the time between the
        commits is less than the difference interval, the function will return
        and we will bit check the partitions.

        Parameters
        ----------

        consumer : AIOKafkaConsumer
            The active kafka consumer reading the alert stream

        current_time : float
            The current time in seconds since the epoch

        last_message_time : float
            The time of the last message in seconds since the epoch

        commit_interval_counter : int
            The number of messages since the last commit
        """
        # If the interval hasn't been reached OR if there have been no new
        # messages, return immediately and don't try to read from the
        # partitions.
        if current_time - last_message_time >= 300 and not new_messages:
            logger.info("No new messages received, waiting for new messages...")
            return new_messages, commit_interval_counter

        commit_interval_counter = await self.handle_commit(
            consumer, commit_interval_counter
        )

        all_caught_up = True
        for partition in consumer.assignment():
            if not await self.check_caught_up(consumer, partition):
                all_caught_up = False
                break

        if all_caught_up:
            logger.info("All partitions are caught up, waiting for new messages...")
            new_messages = False

        return new_messages, commit_interval_counter

    async def check_caught_up(self, consumer, partition):
        """Check if a partition is caught up with its end offset.

        We then return True if the end offset is equal or greater than the
        end offset (maybe possible if end_offset isn't updating correctly)
        and False if the position is less than the end offset.

         Parameters
         ----------

         consumer : AIOKafkaConsumer
            The active kafka consumer reading the alert stream

        partition : int
            The partition to check.
        """
        try:
            logger.info("Checking offset positions.")
            position = await consumer.position(partition)
            end_offset = (await consumer.end_offsets([partition]))[partition]
            logger.info(
                f"Position: {position}, End offset: {end_offset}, Partition: {partition}"
            )
            return position >= end_offset
        except Exception as e:
            logger.warning(f"Error checking partition {partition}: {e}")
            return False

    def _create_consumer(self, auto_offset_reset: str = "latest"):
        if self.kafka_params.auth_mechanism == "scram":
            return self._create_scram_consumer(auto_offset_reset)
        elif self.kafka_params.auth_mechanism == "mtls":
            return self._create_mtls_consumer(auto_offset_reset)
        else:
            raise ValueError("invalid auth mechanism")

    def _create_scram_consumer(self, auto_offset_reset):
        ssl_ctx = ssl.SSLContext()
        ssl_ctx.load_default_certs()
        consumer = AIOKafkaConsumer(
            bootstrap_servers=self.kafka_params.host,
            group_id=self.kafka_params.group,
            sasl_plain_username=self.kafka_params.username,
            sasl_plain_password=self.kafka_params.password,
            sasl_mechanism="SCRAM-SHA-512",
            security_protocol="SASL_PLAINTEXT",
            ssl_context=None,
            enable_auto_commit=False,
            auto_offset_reset=auto_offset_reset,
        )
        consumer.subscribe(topics=self.kafka_params.topics)
        return consumer

    def _create_mtls_consumer(self, auto_offset_reset):
        consumer = AIOKafkaConsumer(
            bootstrap_servers=self.kafka_params.host,
            group_id=self.kafka_params.group,
            security_protocol="SSL",
            ssl_context=self.kafka_params._create_ssl_context(),
            enable_auto_commit=False,
            auto_offset_reset=auto_offset_reset,
        )
        consumer.subscribe(topics=self.kafka_params.topics)
        return consumer

    def handle_kafka_message(self, msg: ConsumerRecord):
        """
        Handle a single Kafka message.

        Parses out the schema ID and alert ID from the message. Stores the
        schema in the backend if it is not already present. Stores the alert
        packet in the backend always.
        """
        logger.info(
            f"Processing Kafka message from topic={msg.topic}, partition={msg.partition}, offset={msg.offset}"
        )
        raw_msg = msg.value
        schema_id, alert_id = self._parse_alert_msg(raw_msg)
        logger.info(f"Parsed message: schema_id={schema_id}, alert_id={alert_id}")

        if not self.backend.schema_exists(schema_id):
            logger.info("%s is a new schema ID - storing it", schema_id)
            encoded_schema = self.schema_registry.get_raw_schema(schema_id)
            self.backend.store_schema(schema_id, encoded_schema)
        else:
            logger.debug(f"Schema {schema_id} already exists, skipping storage")

        logger.info(f"Storing alert {alert_id} to backend")
        self.backend.store_alert(alert_id, raw_msg)
        logger.info(f"Alert {alert_id} stored successfully")

    def _parse_alert_msg(self, raw_msg: bytes) -> Tuple[int, int]:
        # return schema_id, alert_id from alert payload
        schema_id = _read_confluent_wire_format_header(raw_msg)

        logger.debug("read schema ID %s, getting decoder", schema_id)
        decoder = self.schema_registry.get_schema_decoder(schema_id)

        decoded = decoder(io.BytesIO(raw_msg[5:]))
        return schema_id, decoded["diaSourceId"]


def _read_confluent_wire_format_header(raw_msg: bytes) -> int:
    if len(raw_msg) < 5:
        raise ValueError("malformed message: too short")
    if raw_msg[0] != 0:
        raise ValueError("malformed message: incorrect magic byte")
    schema_id = struct.unpack(">I", raw_msg[1:5])[0]
    return schema_id
