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
    ):
        """
        Copies Rubin alert data from a Kafka broker to a database backend,
        using a schema registry to make sense of the alert data.

        All alert data is expected to be encoded in Confluent Wire Format.
        """
        self.kafka_params = kafka_params
        self.backend = backend
        self.schema_registry = registry

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
            since_last_commit = 0
            n = 0
            last_message_time = asyncio.get_event_loop().time()
            logger.info("ingest worker run loop start")
            while True:
                try:
                    msg = await asyncio.wait_for(consumer.__anext__(), timeout=30)

                    self.handle_kafka_message(msg)
                    logger.debug("handle complete")
                    last_message_time = asyncio.get_event_loop().time()
                    since_last_commit += 1
                    new_messages = True

                    if since_last_commit == commit_interval:
                        logger.info("committing position in stream")
                        await consumer.commit()
                        since_last_commit = 0

                    n += 1
                    if limit > 0 and n >= limit:
                        logger.info("limit reached - returning")
                        if since_last_commit > 0:
                            await consumer.commit()
                        return

                except asyncio.TimeoutError:
                    logger.info("waiting timed out, checking for new messages...")
                    current_time = asyncio.get_event_loop().time()
                    # Only check partitions if we've processed messages
                    # since last check
                    if current_time - last_message_time > 3600 and new_messages:
                        if since_last_commit > 0:
                            logger.info(
                                "no new messages for 1 hour, checking for "
                                "any remaining messages in log"
                            )
                            await consumer.commit()
                            since_last_commit = 0

                        caught_up = True
                        # Check if we're caught up on all partitions
                        for partition in consumer.assignment():
                            try:
                                logger.info("Checking offset positions.")
                                position = await consumer.position(partition)
                                end_offset = (await consumer.end_offsets([partition]))[partition]  # fmt: skip
                                logger.info(position)
                                logger.info(end_offset)
                                logger.info(partition)
                                if position < end_offset:
                                    caught_up = False
                                    break
                            except Exception as e:
                                logger.warning(
                                    f"Error checking partition {partition}: {e}"
                                )
                                caught_up = False
                                break

                        if caught_up:
                            new_messages = False
                            logger.info(
                                "Caught up with all partitions, no more messages to process."
                            )
                            continue
        finally:
            await consumer.stop()

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
        logger.debug("handle start")
        raw_msg = msg.value
        schema_id, alert_id = self._parse_alert_msg(raw_msg)
        logger.debug("handling msg schema_id=%s alert_id=%s", schema_id, alert_id)
        if not self.backend.schema_exists(schema_id):
            logger.info("%s is a new schema ID - storing it", schema_id)
            encoded_schema = self.schema_registry.get_raw_schema(schema_id)
            self.backend.store_schema(schema_id, encoded_schema)
        logger.debug("storing alert")
        self.backend.store_alert(alert_id, raw_msg)

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
