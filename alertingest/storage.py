"""
Implementations of backend storage systems for the alert database server.
"""
import abc
import gzip
from typing import Set

import google.cloud.storage as gcs


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


class GoogleObjectStorageBackend(AlertDatabaseBackend):
    """
    Stores alerts and schemas in a Google Cloud Storage bucket.

    The path for alert and schema objects follows the scheme in DMTN-183.

    Queries to check whether a schema exists are cached forever. This code
    assumes that schemas are never deleted from the bucket.
    """

    def __init__(self, gcp_project: str, bucket_name: str):
        self.object_store_client = gcs.Client(project=gcp_project)
        self.bucket = self.object_store_client.bucket(bucket_name)
        self.known_schemas: Set[int] = set()

    def store_alert(self, alert_id: int, alert_payload: bytes):
        compressed_payload = gzip.compress(alert_payload)
        blob = self.bucket.blob(f"/alert_archive/v1/alerts/{alert_id}.avro.gz")
        blob.upload_from_string(compressed_payload)

    def store_schema(self, schema_id: int, encoded_schema: bytes):
        blob = self.bucket.blob(f"/alert_archive/v1/schemas/{schema_id}.json")
        blob.upload_from_string(encoded_schema)
        self.known_schemas.add(schema_id)

    def schema_exists(self, schema_id: int):
        if schema_id in self.known_schemas:
            return True
        blob = self.bucket.blob(f"/alert_archive/v1/schemas/{schema_id}.json")
        if blob.exists():
            self.known_schemas.add(schema_id)
            return True
        return False
