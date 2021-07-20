"""
A worker which copies alerts and schemas into an object store backend.
"""
from typing import Tuple
import struct

from aiokafka import AIOKafkaConsumer
from alertingest.storage import AlertDatabaseBackend
from alertingest.schema_registry import SchemaRegistryClient


class IngestWorker:
    def __init__(self,
                 kafka_consumer: AIOKafkaConsumer,
                 backend: AlertDatabaseBackend,
                 registry: SchemaRegistryClient):
        self.kafka_consumer = kafka_consumer
        self.backend = backend
        self.schema_registry = registry

    async def run(self):
        await self.kafka_consumer.start()
        try:
            since_last_commit = 0
            async for msg in self.kafka_consumer:
                self.handle_kafka_message(msg)
                since_last_commit += 1
                if since_last_commit == 100:
                    await self.kafka_consumer.commit()
                    since_last_commit = 0
        finally:
            await self.kafka_consumer.stop()

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
