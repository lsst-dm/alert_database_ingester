"""
Implementations of backend storage systems for the alert database server.
"""

import abc
import gzip
import logging
from typing import Any, Set, Union

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


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

    def create_client(self, endpoint_url: Union[str, None] = None):
        s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            region_name="us-east-1",
            config=Config(
                request_checksum_calculation="when_required",
                response_checksum_validation="when_required",
            ),
        )
        test_aws_credentials(s3_client)
        return s3_client

    def store_alert(
        self, alert_id: int, alert_payload: bytes, compression: bool = False
    ):

        if compression:
            alert_payload = gzip.compress(alert_payload)
            alert_key = f"v1/alerts/{alert_id}.avro.gz"
        else:
            alert_key = f"v1/alerts/{alert_id}.avro"

        logging.info(
            f"Storing alert to bucket: {self.packet_bucket}, path: {alert_key}"
        )

        try:
            response = self.object_store_client.put_object(
                Bucket=self.packet_bucket, Key=alert_key, Body=alert_payload
            )
            return response
        except self.object_store_client.exceptions.ClientError as e:
            handle_s3_error(e, "alert", self.packet_bucket, alert_id, alert_key)

    def store_schema(self, schema_id: int, encoded_schema: bytes):
        schema_key = f"v1/schemas/{schema_id}.json"
        boto3.set_stream_logger("")
        try:
            response = self.object_store_client.put_object(
                Bucket=self.schema_bucket, Key=schema_key, Body=encoded_schema
            )
            self.known_schemas.add(schema_id)
            return response
        except self.object_store_client.exceptions.ClientError as e:
            handle_s3_error(e, "schema", self.schema_bucket, schema_id, schema_key)

        self.known_schemas.add(schema_id)

    def schema_exists(self, schema_id: int):
        if schema_id in self.known_schemas:
            return True
        schema_key = f"v1/schemas/{schema_id}.json"
        try:
            self.object_store_client.get_object(
                Bucket=self.schema_bucket, Key=schema_key
            )
            self.known_schemas.add(schema_id)
            return True
        except self.object_store_client.exceptions.ClientError:
            logging.warning("Schema not in schema bucket.")
            return False


def test_aws_credentials(s3_client):
    try:
        # Try to list buckets - this operation requires valid credentials
        response = s3_client.list_buckets()
        logging.info("AWS credentials are valid. Successfully connected to S3.")
        print(f"Available S3 bucket list: {response}")

        return True
    except Exception as e:
        logging.warning(f"Error validating AWS credentials: {str(e)}")
        error_code = e.response["Error"].get("Code")
        error_msg = e.response["Error"].get("Message")
        request_id = e.response.get("ResponseMetadata", {}).get("RequestId")
        status_code = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        logging.warning(
            f"Error Code: {error_code}\n"
            f"Message: {error_msg}\n"
            f"Status Code: {status_code}\n"
            f"Request ID: {request_id}"
        )

        return False


def handle_s3_error(
    e: ClientError, operation_type: str, bucket: str, item_id: int, item_key: str
):
    """
    Handle S3 ClientError exceptions with consistent error logging.

    Parameters
    ----------
    e : ClientError
        The boto3 ClientError exception
    operation_type : str
        Type of item being operated on (e.g., 'alert', 'schema')
    bucket : str
        Name of the S3 bucket
    item_id : int
        ID of the item (alert_id or schema_id)
    item_key : str
        S3 key for the item

    Raises
    ------
    ClientError
        Re-raises the original exception after logging
    """
    error_code = e.response["Error"].get("Code")
    error_msg = e.response["Error"].get("Message")
    request_id = e.response.get("ResponseMetadata", {}).get("RequestId")
    status_code = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if error_code == "409":
        logging.warning(
            f"{operation_type.capitalize()} bucket '{bucket}' is full. "
            f"Contact USDF for more space.\n"
            f"{operation_type.capitalize()} ID: {item_id}\n"
            f"Path: {item_key}\n"
            f"Error Code: {error_code}\n"
            f"Message: {error_msg}\n"
            f"Status Code: {status_code}\n"
            f"Request ID: {request_id}"
        )
    elif error_code == "404":
        logging.warning(
            f"Cannot reach {operation_type} bucket at '{bucket}{item_key}'.\n"
            f"Check {operation_type} bucket server status.\n"
            f"{operation_type.capitalize()} ID: {item_id}\n"
            f"Error Code: {error_code}\n"
            f"Message: {error_msg}\n"
            f"Status Code: {status_code}\n"
            f"Request ID: {request_id}"
        )
    else:
        logging.warning(
            f"Failed to store {operation_type} in bucket '{bucket}'.\n"
            f"{operation_type.capitalize()} ID: {item_id}\n"
            f"Path: {item_key}\n"
            f"Error Code: {error_code}\n"
            f"Message: {error_msg}\n"
            f"Status Code: {status_code}\n"
            f"Request ID: {request_id}"
        )
    raise e
