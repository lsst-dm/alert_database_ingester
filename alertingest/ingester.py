"""
A worker which copies alerts and schemas into an object store backend.
"""

import asyncio
import datetime
import io
import logging
import ssl
import struct
from collections import deque
from dataclasses import dataclass
from typing import Any, Dict, Tuple

from aiokafka import AIOKafkaConsumer, ConsumerRecord
from aiokafka.helpers import create_ssl_context

from alertingest.schema_registry import SchemaRegistryClient
from alertingest.storage import AlertDatabaseBackend

logger = logging.getLogger(__name__)


@dataclass
class KafkaConnectionParams:
    """A bundle of data required to connect to Kafka."""

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
        """Bundles the KafkaConnectionParams' SSL-related attributes into an
        SSL context.
        """
        assert self.auth_mechanism == "mtls"
        return create_ssl_context(
            cafile=self.server_ca_crt_path,
            certfile=self.client_crt_path,
            keyfile=self.client_key_path,
        )


def _last_noon() -> datetime.datetime:
    """Return the most recent noon (12:00 local server time) as a datetime."""
    now = datetime.datetime.now()
    today_noon = now.replace(hour=12, minute=0, second=0, microsecond=0)
    return today_noon if now >= today_noon else today_noon - datetime.timedelta(days=1)


class IngestWorker:
    def __init__(
        self,
        kafka_params: KafkaConnectionParams,
        backend: AlertDatabaseBackend,
        registry: SchemaRegistryClient,
        message_timeout: int = 1800,
        log_check_timeout: int = 3600,
        prefix_idle_timeout: int = 7600,
        max_logged_prefixes: int = 30,
    ):
        """Copies Rubin alert data from a Kafka broker to a database backend,
        using a schema registry to make sense of the alert data.

        All alert data is expected to be encoded in Confluent Wire Format.
        """
        self.kafka_params = kafka_params
        self.backend = backend
        self.schema_registry = registry
        self.message_timeout = message_timeout
        self.log_check_timeout = log_check_timeout
        self.prefix_idle_timeout = prefix_idle_timeout
        self.max_logged_prefixes = max_logged_prefixes

    async def run(
        self,
        limit: int = -1,
        commit_interval: int = 100,
        auto_offset_reset: str = "latest",
    ):
        """Run the consumer, copying messages from Kafka to the IngestWorker's
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
            state_tracker: Dict[str, Any] = {
                "commit_interval_counter": 0,  # Counter used to keep track of the submit interval
                "limit_n": 0,  # Counts messages if we are limiting number to send.
                "last_message_time": asyncio.get_event_loop().time(),
                "new_messages": True,  # Check if we are reading new messages
                "daily_stored": 0,  # Alerts written to S3 in the current 24h period
                "day_start_time": _last_noon(),  # Aligned to most recent noon
                "prefix_counts": {},  # Per-prefix alert counts {prefix: count}
                "prefix_last_write": {},  # Per-prefix last write timestamp {prefix: time}
                "logged_prefixes": deque(
                    maxlen=self.max_logged_prefixes
                ),  # Prefixes whose idle summaries have been logged (max 30)
            }

            logger.info("Ingest worker run loop start.")
            while True:
                try:
                    logger.debug("Waiting for message.")
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
                        logger.info(
                            "Alerts stored today: %s", state_tracker["daily_stored"]
                        )
                        self._check_daily_reset(datetime.datetime.now(), state_tracker)

                    # Check message limit
                    if limit > 0 and state_tracker["limit_n"] >= limit:
                        logger.info("limit reached - returning")
                        await self.handle_commit(
                            consumer, state_tracker["commit_interval_counter"]
                        )
                        self._log_final_summary(state_tracker)
                        return

                except asyncio.TimeoutError:
                    logger.info("Waiting timed out, checking for new messages...")
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
                    self._check_idle_prefixes(
                        current_time,
                        state_tracker["prefix_counts"],
                        state_tracker["prefix_last_write"],
                        state_tracker["logged_prefixes"],
                    )
                    self._check_daily_reset(datetime.datetime.now(), state_tracker)

        except Exception as e:
            logger.error("Error during message processing: %s", e)
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
        daily_stored=0,
        day_start_time=None,
        prefix_counts=None,
        prefix_last_write=None,
        logged_prefixes=None,
    ):
        """Process a single Kafka message.

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
        if prefix_counts is None:
            prefix_counts = {}
        if prefix_last_write is None:
            prefix_last_write = {}
        if logged_prefixes is None:
            logged_prefixes = deque(maxlen=self.max_logged_prefixes)

        try:
            alert_id = worker.handle_kafka_message(msg)
        except Exception as e:
            logger.error("Error processing message at offset %s: %s", msg.offset, e)
            logger.exception("full traceback")
            raise

        logger.debug("handle complete")
        if limit > 0:
            limit_n += 1
        will_return = True if msg else new_messages

        now = asyncio.get_event_loop().time()
        daily_stored += 1
        alert_prefix = str(alert_id)[:6]
        prefix_counts[alert_prefix] = prefix_counts.get(alert_prefix, 0) + 1
        prefix_last_write[alert_prefix] = now

        return {
            "last_message_time": now,
            "commit_interval_counter": commit_interval_counter + 1,
            "limit_n": limit_n,
            "new_messages": will_return,
            "daily_stored": daily_stored,
            "day_start_time": day_start_time,
            "prefix_counts": prefix_counts,
            "prefix_last_write": prefix_last_write,
            "logged_prefixes": logged_prefixes,
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
            logger.info("Committing %s messages", commit_interval_counter)
            for partition in consumer.assignment():
                position = await consumer.position(partition)
                logger.info(
                    "Committing partition %s, topic %s at position %s",
                    partition.partition,
                    partition.topic,
                    position,
                )

            await consumer.commit()
            logger.info("Commit complete.")
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
                "Position: %s, End offset: %s, Partition: %s",
                position,
                end_offset,
                partition,
            )
            return position >= end_offset
        except Exception as e:
            logger.warning("Error checking partition %s: %s", partition, e)
            return False

    def _check_idle_prefixes(
        self, current_time, prefix_counts, prefix_last_write, logged_prefixes
    ):
        """Log a summary for any alert prefix that has been idle for a number
        of seconds.

        Parameters
        ----------
        current_time : float
            Current time in seconds.
        prefix_counts : dict
           Count of alerts stored under a specific prefix.
        prefix_last_write : dict
            Time last alert was written for a specific prefix in seconds.
        logged_prefixes : set
            Set of prefixes whose idle summaries have already logged.
            Updated in-place as new summaries are logged.
        """
        for prefix in list(prefix_counts):
            if prefix in logged_prefixes:
                continue
            idle_seconds = current_time - prefix_last_write[prefix]
            if idle_seconds >= self.prefix_idle_timeout:
                logger.info(
                    "Alert prefix %s: %d alert(s) stored to S3 "
                    "(no new alerts for %.0f seconds for this prefix.)",
                    prefix,
                    prefix_counts[prefix],
                    idle_seconds,
                )
                logged_prefixes.append(prefix)
                del prefix_counts[prefix]
                del prefix_last_write[prefix]

    def _check_daily_reset(self, current_time, state_tracker):
        """Log and reset the daily alert counter when 24 hours have elapsed.

        If at least 86400 seconds have passed since ``day_start_time``, emits
        an INFO log with the count for that window, resets ``daily_stored`` to
        0, and advances ``day_start_time`` by 86400 seconds so consecutive
        rollovers are handled correctly.

        Parameters
        ----------
        current_time : datetime.datetime
            Current time.
        state_tracker : dict
            The run-loop state dictionary (mutated in place).
        """
        one_day = datetime.timedelta(days=1)
        while current_time - state_tracker["day_start_time"] >= one_day:
            logger.info(
                "Alerts stored to S3 in the last 24 hours: %d",
                state_tracker["daily_stored"],
            )
            state_tracker["daily_stored"] = 0
            state_tracker["day_start_time"] += one_day

    def _log_final_summary(self, state_tracker):
        """Log the current-period alert count and per-prefix summaries for any
        prefix not yet reported.

        Parameters
        ----------
        state_tracker : dict
            The run-loop state dictionary.
        """
        logger.info(
            "Alerts stored to S3 in current period: %d",
            state_tracker["daily_stored"],
        )
        for prefix, count in state_tracker["prefix_counts"].items():
            if prefix not in state_tracker["logged_prefixes"]:
                logger.info(
                    "Alert prefix %s: %d alert(s) stored to S3",
                    prefix,
                    count,
                )

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
        """Handle a single Kafka message.

        Parses out the schema ID and alert ID from the message. Stores the
        schema in the backend if it is not already present. Stores the alert
        packet in the backend always.

        Parameters
        ----------
        msg : ConsumerRecord
            Kafka message to process and send to the alert archive.
        """
        # Logging important during debugging
        logger.debug(
            "Processing Kafka message from topic=%s, partition=%s, offset=%s",
            msg.topic,
            msg.partition,
            msg.offset,
        )
        raw_msg = msg.value
        schema_id, alert_id = self._parse_alert_msg(raw_msg)
        logger.debug("Parsed message: schema_id=%s, alert_id=%s", schema_id, alert_id)

        if not self.backend.schema_exists(schema_id):
            logger.info("%s is a new schema ID - storing it", schema_id)
            encoded_schema = self.schema_registry.get_raw_schema(schema_id)
            self.backend.store_schema(schema_id, encoded_schema)
        else:
            logger.debug("Schema %s already exists, skipping storage", schema_id)

        logger.debug("Storing alert %s to backend.", alert_id)
        self.backend.store_alert(alert_id, raw_msg)
        logger.debug("Alert %s stored successfully.", alert_id)
        return alert_id

    def _parse_alert_msg(self, raw_msg: bytes) -> Tuple[int, int]:
        # return schema_id, alert_id from alert payload
        schema_id = _read_confluent_wire_format_header(raw_msg)

        logger.debug("Read schema ID %s, getting decoder.", schema_id)
        decoder = self.schema_registry.get_schema_decoder(schema_id)

        decoded = decoder(io.BytesIO(raw_msg[5:]))
        return schema_id, decoded["diaSourceId"]


def _read_confluent_wire_format_header(raw_msg: bytes) -> int:
    if len(raw_msg) < 5:
        raise ValueError("Malformed message: too short.")
    if raw_msg[0] != 0:
        raise ValueError("Malformed message: incorrect magic byte.")
    schema_id = struct.unpack(">I", raw_msg[1:5])[0]
    return schema_id
