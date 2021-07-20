from typing import Callable, IO, Any

import avroc
import json
import requests

Decoder = Callable[[IO[bytes]], Any]


class SchemaRegistryClient:
    def __init__(self, address: str):
        self.address = address
        self._cached_decoder = {}

    def get_raw_schema(self, schema_id: int) -> bytes:
        response = requests.get(f"https://{self.address}/schemas/ids/{schema_id}")
        response.raise_for_status()
        return response.content

    def get_schema_decoder(self, schema_id: int) -> Decoder:
        # If we've already constructed a decoder, use it.
        if schema_id in self._cached_decoders:
            return self._cached_decoders[schema_id]

        # We need to make a new one. Start by downloading the schema JSON from
        # the registry.
        schema_bytes = self.get_raw_schema(schema_id)
        writer_schema = json.loads(schema_bytes)

        # Now, construct a reader schema which only reads the alert ID.
        reader_schema = writer_schema.copy()
        reader_schema["fields"] = [{"name": "alertId", "type": "long"}]

        # Compile the decoder, so we correctly skip unused fields.
        decoder = avroc.compile_decoder(writer_schema, reader_schema)
        self._cached_decoders[schema_id] = decoder
        return decoder
