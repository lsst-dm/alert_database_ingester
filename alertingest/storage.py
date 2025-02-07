"""
Implementations of backend storage systems for the alert database server.
"""

import abc
import gzip
import logging
from typing import Any, Set

import boto3
from botocore.config import Config


class AlertDatabaseBackend(abc.ABC):
    """
    An abstract interface representing a storage backend for alerts and
    schemas.
    """

    @abc.abstractmethod
    def store_alert(self, alert_id: int, alert_payload: bytes):
        """
        Store a single alert's payload, in compressed Confluent Wire Format.

        Confluent Wire Format is described here:
          https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#wire-format

        Parameters
        ----------
        alert_id : int
            The ID of the alert to be retrieved.

        alert_payload : bytes
            The alert contents in uncompressed Confluent Wire Format.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def store_schema(self, schema_id: int, encoded_schema: bytes):
        """
        Store a single alert schema JSON document in its JSON-serialized form.

        Parameters
        ----------
        schema_id : int
            The ID of the schema to be retrieved.
        encoded_schema : bytes
            A JSON-encoded Avro schema document.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def schema_exists(self, schema_id: int) -> bool:
        """Returns True if the schema already exists in the backend.

        Parameters
        ----------
        schema_id : int
            The ID of the schema to look for.

        Returns
        -------
        bool
            Whether the schema has already been stored.
        """
        raise NotImplementedError()


class USDFObjectStorageBackend(AlertDatabaseBackend):
    """
    Stores alerts and schemas in a Google Cloud Storage bucket.

    The path for alert and schema objects follows the scheme in DMTN-183.

    Queries to check whether a schema exists are cached forever. This code
    assumes that schemas are never deleted from the bucket.
    """

    def __init__(
        self,
        alert_bucket_name: str,
        schema_bucket_name: str,
        s3_client: Any | None = None,
        endpoint_url: str | None = None,
    ):

        # Provided s3_client used during unit testing.
        self.object_store_client = s3_client or self.create_client(
            endpoint_url=endpoint_url
        )
        self.packet_bucket = alert_bucket_name
        self.schema_bucket = schema_bucket_name
        self.known_schemas: Set[int] = set()

    def create_client(self, endpoint_url: str | None = None):
        s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            config=Config(
                request_checksum_calculation="when_required",
                response_checksum_validation="when_required",
            ),
        )
        return s3_client

    def store_alert(
        self, alert_id: int, alert_payload: bytes, compression: bool = False
    ):

        if compression:
            alert_payload = gzip.compress(alert_payload)
            alert_key = f"/v1/alerts/{alert_id}.avro.gz"
        else:
            alert_key = f"/v1/alerts/{alert_id}.avro"
        try:
            response = self.object_store_client.put_object(
                Bucket=self.packet_bucket, Key=alert_key, Body=alert_payload
            )
            return response
        except self.object_store_client.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "409":
                logging.warning(
                    "Alert archive bucket is full. Contact USDF for more space."
                )
            if e.response["Error"]["Code"] == "404":
                logging.warning(
                    "Cannot reach alert archive. Check alert archive servr status."
                )
            raise e

    def store_schema(self, schema_id: int, encoded_schema: bytes):
        schema_key = f"/v1/schemas/{schema_id}.json"

        try:
            self.object_store_client.put_object(
                Bucket=self.schema_bucket, Key=schema_key, Body=encoded_schema
            )
        except self.object_store_client.exceptions.ClientError as e:
            logging.warning("Schema could not be stored.")
            raise e
        self.known_schemas.add(schema_id)

    def schema_exists(self, schema_id: int):
        if schema_id in self.known_schemas:
            return True
        schema_key = f"/v1/schemas/{schema_id}.json"
        try:
            self.object_store_client.get_object(
                Bucket=self.schema_bucket, Key=schema_key
            )
            self.known_schemas.add(schema_id)
            return True
        except self.object_store_client.exceptions.ClientError:
            logging.warning("Schema not in schema bucket.")
            return False
