from typing import Callable, IO, Any, Dict

import avroc
import json
import requests
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


Decoder = Callable[[IO[bytes]], Any]


class SchemaRegistryClient:
    def __init__(self, address: str):
        self.address = address
        self._cached_decoders: Dict[int, Decoder] = {}

    def get_raw_schema(self, schema_id: int) -> bytes:
        url = f"https://{self.address}/schemas/ids/{schema_id}"
        logger.debug("making request to %s", url)
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        parsed_body = json.loads(response.content)
        return parsed_body["schema"]

    def get_schema_decoder(self, schema_id: int) -> Decoder:
        # If we've already constructed a decoder, use it.
        if schema_id in self._cached_decoders:
            logger.debug("using cached decoder")
            return self._cached_decoders[schema_id]

        logger.info("constructing new decoder")
        # We need to make a new one. Start by downloading the schema JSON from
        # the registry.
        schema_bytes = self.get_raw_schema(schema_id)
        logger.debug("parsing JSON bytes")
        writer_schema = json.loads(schema_bytes)

        # Now, construct a reader schema which only reads the alert ID.
        reader_schema = writer_schema.copy()
        reader_schema["fields"] = [{"name": "alertId", "type": "long"}]

        # Compile the decoder, so we correctly skip unused fields.
        logger.debug("compiling decoder")
        decoder: Decoder = avroc.compile_decoder(writer_schema, reader_schema)

        self._cached_decoders[schema_id] = decoder
        logger.debug("decoder construction done")
        return decoder
