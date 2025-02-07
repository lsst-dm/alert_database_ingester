import datetime
import gzip
import unittest
from io import BytesIO
from unittest.mock import patch

import boto3
from botocore.exceptions import ClientError
from botocore.stub import Stubber

from alertingest.storage import USDFObjectStorageBackend


class TestUSDFObjectStorageBackend(unittest.TestCase):
    def setUp(self):

        self.s3 = boto3.client("s3")
        self.alert_payload = gzip.compress(b"mock_alert_payload")
        self.encoded_schema = b"mock_encoded_schema"
        self.backend = USDFObjectStorageBackend(
            alert_bucket_name="fake_alert_bucket",
            schema_bucket_name="fake_schema_bucket",
            s3_client=self.s3,
        )
        self.stubber = Stubber(self.s3)

    @staticmethod
    def fake_get_response():
        response = {
            "AcceptRanges": "bytes",
            "LastModified": datetime.datetime(
                2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc
            ),
            "ContentLength": 1234,
            "ETag": '"123abc456def7890"',
            "ContentType": "text/plain",
            "Metadata": {"customMetadata": "some-value"},
            "Body": BytesIO(b"Hello, world!"),
        }
        return response

    @staticmethod
    def fake_put_response():
        response = {
            "Expiration": "string",
            "ETag": "string",
            "ChecksumCRC32": "string",
            "ChecksumCRC32C": "string",
            "ChecksumSHA1": "string",
            "ChecksumSHA256": "string",
            "VersionId": "string",
            "SSECustomerAlgorithm": "string",
            "SSECustomerKeyMD5": "string",
            "SSEKMSKeyId": "string",
            "SSEKMSEncryptionContext": "string",
            "BucketKeyEnabled": True | False,
            "Size": 123,
            "RequestCharged": "requester",
        }
        return response

    def test_store_alert_success(self):
        put_response = self.fake_put_response()

        expected_params = {
            "Bucket": "fake_alert_bucket",
            "Key": "/v1/alerts/1.avro",
            "Body": self.alert_payload,
        }
        self.stubber.activate()
        self.stubber.add_response("put_object", put_response, expected_params)
        response = self.backend.store_alert(1, self.alert_payload, compression=False)
        self.assertEqual(response, self.fake_put_response())

        self.stubber.deactivate()

    def test_store_alert_fail(self):
        self.stubber.add_client_error(
            "put_object",
            service_error_code="Not Acceptable",
            service_message="Not Acceptable",
            http_status_code=406,
        )
        self.stubber.activate()
        with self.assertRaises(ClientError):
            self.backend.store_alert(1, self.alert_payload, compression=False)
        self.stubber.deactivate()

    def test_store_schema_success(self):
        put_response = self.fake_put_response()

        expected_params = {
            "Bucket": "fake_schema_bucket",
            "Key": "/v1/schemas/1.json",
            "Body": self.encoded_schema,
        }

        self.stubber.activate()
        self.stubber.add_response("put_object", put_response, expected_params)
        self.backend.store_schema(1, self.encoded_schema)
        self.assertIn(1, self.backend.known_schemas)
        self.stubber.deactivate()

    @patch("logging.warning")
    def test_store_schema_fail_bucket(self, mock_warning):
        self.stubber.activate()
        self.stubber.add_client_error(
            "put_object",
            service_message="NoSuchBucket",
            service_error_code="NoSuchBucket",
        )
        try:
            self.backend.store_schema(1, self.encoded_schema)
            self.fail("Expected ClientError not raised")
        except ClientError as e:
            self.assertEqual(e.response["Error"]["Code"], "NoSuchBucket")
            self.assertEqual(e.response["Error"]["Message"], "NoSuchBucket")
            self.assertEqual(e.response["ResponseMetadata"]["HTTPStatusCode"], 400)
            mock_warning.assert_called_once_with("Schema could not be stored.")
        self.stubber.deactivate()

    def test_schema_exists_retrieve_true(self):
        self.backend.known_schemas.add(2)
        self.assertTrue(self.backend.schema_exists(2))
        self.assertIn(2, self.backend.known_schemas)

    def test_schema_exists_add_to_known(self):
        get_response = self.fake_get_response()

        expected_params = {
            "Bucket": "fake_schema_bucket",
            "Key": "/v1/schemas/3.json",
        }
        # Test schema wasn't stored in known schemas but was retrieved
        # and added
        self.stubber.activate()
        self.stubber.add_response("get_object", get_response, expected_params)
        self.assertTrue(self.backend.schema_exists(3))
        self.stubber.deactivate()

    @patch("logging.warning")
    def test_schema_exists_failure(self, mock_warning):
        # Test non existent schema returns false
        self.stubber.activate()
        self.stubber.add_client_error(
            "get_object", service_message="NoSuchKey", service_error_code="NoSuchKey"
        )
        response = self.backend.schema_exists(1)
        self.assertFalse(response)
        mock_warning.assert_called_once_with("Schema not in schema bucket.")
        self.stubber.deactivate()


if __name__ == "__main__":
    unittest.main()
