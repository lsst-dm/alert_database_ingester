"""
A worker which copies alerts and schemas into an object store backend.
"""
from typing import Tuple
import ssl
import struct
from dataclasses import dataclass
from aiokafka import AIOKafkaConsumer
from alertingest.storage import AlertDatabaseBackend
from alertingest.schema_registry import SchemaRegistryClient


@dataclass
class KafkaConnectionParams:
    host: str
    topic: str
    group: str
    username: str
    password: str


class IngestWorker:
    def __init__(self,
                 kafka_params: KafkaConnectionParams,
                 backend: AlertDatabaseBackend,
                 registry: SchemaRegistryClient):
        self.kafka_params = kafka_params
        self.backend = backend
        self.schema_registry = registry

    async def run(self):
        ssl_ctx = ssl.SSLContext()
        ssl_ctx.load_default_certs()
        consumer = AIOKafkaConsumer(
            self.kafka_params.topic,
            bootstrap_servers=self.kafka_params.host,
            group_id=self.kafka_params.group,
            sasl_plain_username=self.kafka_params.username,
            sasl_plain_password=self.kafka_params.password,
            sasl_mechanism="SCRAM-SHA-256",
            security_protocol="SASL_SSL",
            ssl_context=ssl_ctx,
        )
        await consumer.start()
        try:
            since_last_commit = 0
            async for msg in consumer:
                self.handle_kafka_message(msg)
                since_last_commit += 1
                if since_last_commit == 100:
                    await consumer.commit()
                    since_last_commit = 0
        finally:
            await consumer.stop()

    def handle_kafka_message(self, raw_msg):
        schema_id, alert_id = self._parse_alert_msg(raw_msg)
        if not self.backend.schema_exists(schema_id):
            encoded_schema = self.schema_registry.get_raw_schema(schema_id)
            self.backend.store_schema(schema_id, encoded_schema)
        self.backend.store_alert(alert_id, raw_msg)

    def _parse_alert_msg(self, raw_msg: bytes) -> Tuple[int, str]:
        # return schema_id, alert_id from alert payload
        schema_id = _read_confluent_wire_format_header(raw_msg)
        decoder = self.schema_registry.get_schema_decoder(schema_id)
        decoded = decoder(raw_msg)
        return schema_id, decoded["alertId"]


def _read_confluent_wire_format_header(raw_msg: bytes) -> int:
    if len(raw_msg) < 5:
        raise ValueError("malformed message: too short")
    if raw_msg[0] != 0:
        raise ValueError("malformed message: incorrect magic byte")
    schema_id = struct.unpack(">I", raw_msg[1:5])
    return schema_id
