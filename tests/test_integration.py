import asyncio
import io
import logging
import os
import ssl
import struct
import unittest
import urllib

import boto3
import fastavro
import kafka.errors
import lsst.alert.packet
from aiokafka import AIOKafkaProducer
from botocore.exceptions import ClientError
from kafka.admin import KafkaAdminClient, NewTopic
from lsst.alert.packet.simulate import (
    randomDouble,
    randomFloat,
    randomInt,
    randomLong,
    randomString,
)

from alertingest.ingester import IngestWorker, KafkaConnectionParams
from alertingest.schema_registry import SchemaRegistryClient
from alertingest.storage import USDFObjectStorageBackend

logger = logging.getLogger(__name__)
logger.level = logging.DEBUG


_required_env_vars = {
    "kafka_url": "ALERT_INGEST_TEST_KAFKA_URL",
    "registry_url": "ALERT_INGEST_TEST_REGISTRY_URL",
    "endpoint_url": "AWS_ENDPOINT_URL",
}


def _load_required_env_var(name):
    key = _required_env_vars[name]
    val = os.environ.get(key)
    if val is None:
        raise unittest.SkipTest(f"the ${key} environment variable must be set")
    return val


class IngesterIntegrationTest(unittest.TestCase):
    schema_id = -1
    schema = {}  # type: ignore

    @classmethod
    def setUpClass(cls):
        """
        Create:
            - a test bucket which will receive alerts
            - a test Kafka topic
            - a schema in the schema registry
        """
        cls._create_test_buckets()
        cls._set_kafka_creds()
        cls._create_test_topic()
        cls._setup_test_schema()
        cls._load_schema_registry_creds()

    def test_integration(self):
        """
        Run the ingester against a real Kafka topic, Google Cloud Storage
        bucket, and Schema Registry.
        """
        # This will be swapped to a MOCK since we shouldnt be testing again
        # a real setup
        kafka_group = "alert_ingest_integration_test_group"
        endpoint_url = _load_required_env_var("endpoint_url")
        kafka_params = KafkaConnectionParams.with_scram(
            self.kafka_hostport,
            self.topic_name,
            kafka_group,
            self.kafka_username,
            self.kafka_password,
        )
        backend = USDFObjectStorageBackend(
            alert_bucket_name=self.alert_bucket_name,
            schema_bucket_name=self.schema_bucket_name,
            endpoint_url=endpoint_url,
            s3_client=None,
        )
        registry = SchemaRegistryClient("https://" + self.registry_hostport)

        worker = IngestWorker(kafka_params, backend, registry)

        # Publish 5 messages into the Kafka topic.
        n_msg = 5
        messages = [self.generate_random_alert(i) for i in range(n_msg)]
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.publish_alerts(messages))

        # Run the worker, copying the messages into the backend. It's important
        # to use 'auto_offset_reset="earliest"' here to deal with races between
        # publishing and starting the consumer.
        run_worker = worker.run(limit=5, auto_offset_reset="earliest")
        loop.run_until_complete(asyncio.wait_for(run_worker, timeout=15))

        # The schema should be uploaded.
        assert backend.schema_exists(self.schema_id)

        # Each of the 5 alert should be uploaded.
        for message in messages:
            blob_url = f"/v1/alerts/{message['alertId']}.avro"
            s3_client = boto3.client("s3", endpoint_url=endpoint_url)
            response = s3_client.get_object(Bucket=self.alert_bucket_name, Key=blob_url)
            self.assertEqual(response["ResponseMetadata"]["HTTPStatusCode"], 200)

    async def publish_alerts(self, alerts):
        # Publish alerts to the Kafka broker.
        ssl_ctx = ssl.SSLContext()
        ssl_ctx.load_default_certs()
        producer = AIOKafkaProducer(
            bootstrap_servers=self.kafka_hostport,
            sasl_plain_username=self.kafka_username,
            sasl_plain_password=self.kafka_password,
            sasl_mechanism="SCRAM-SHA-512",
            security_protocol="SASL_PLAINTEXT",
        )
        await producer.start()
        try:
            for alert in alerts:
                logger.info("writing alert with ID %s", alert["alertId"])
                await producer.send_and_wait(self.topic_name, self._encode_alert(alert))
        finally:
            await producer.stop()

    def _encode_alert(self, alert: dict) -> bytes:
        """
        Encode an alert packet using self.schema, writing it in Confluent Wire
        Format.
        """
        outgoing_bytes = io.BytesIO()
        outgoing_bytes.write(struct.pack("!b", 0))
        outgoing_bytes.write(struct.pack("!I", self.schema_id))
        fastavro.schemaless_writer(outgoing_bytes, self.schema, alert)
        return outgoing_bytes.getvalue()

    @staticmethod
    def generate_random_alert(alert_id: int) -> dict:
        """
        Generate random alert packet.

        This is tightly coupled to version 4.0 of the alert packet schema. This
        is unfortunate, but it's relatively simple.
        """
        return {
            "alertId": alert_id,
            "diaSource": {
                "diaSourceId": randomLong(),
                "ccdVisitId": randomLong(),
                "diaObjectId": randomLong(),
                "ssObjectId": randomLong(),
                "midPointTai": randomDouble(),
                "filterName": randomString(),
                "programId": randomInt(),
                "ra": randomDouble(),
                "decl": randomDouble(),
                "x": randomFloat(),
                "y": randomFloat(),
                "apFlux": randomFloat(),
                "apFluxErr": randomFloat(),
                "snr": randomFloat(),
                "psFlux": randomFloat(),
                "psFluxErr": randomFloat(),
                "flags": 0,
            },
        }

    @classmethod
    def _create_test_buckets(cls):
        """
        Create test buckets for alerts and schemas.
        """
        endpoint_url = _load_required_env_var("endpoint_url")
        s3 = boto3.client("s3", endpoint_url=endpoint_url)
        s3_resource = boto3.resource("s3")

        cls._create_alert_test_bucket(s3, s3_resource)
        cls._create_schema_test_bucket(s3, s3_resource)

    @classmethod
    def _create_alert_test_bucket(cls, s3, s3_resource):
        """
        Create a bucket named 'alert_ingest_integration_test_bucket_alerts' and
        register a cleanup function when the test exits for any reason.
        """
        bucket_name = "alert-ingest-integration-test-bucket-alerts"

        logger.info("creating bucket %s", bucket_name)
        try:
            s3.create_bucket(Bucket=bucket_name)
            bucket = s3_resource.Bucket(bucket_name)
        except ClientError:
            logger.warning("bucket already exists!")
            bucket = s3_resource.Bucket(bucket_name)

        def delete_bucket():
            logger.info("deleting bucket %s", bucket_name)
            bucket.objects.all().delete()
            bucket.delete()

        cls.addClassCleanup(delete_bucket)
        cls.alert_bucket_name = bucket_name

    @classmethod
    def _create_schema_test_bucket(cls, s3, s3_resource):
        """
        Create a bucket named 'alert-ingest-integration-test-bucket-schemas'
        and register a cleanup function when the test exits for any reason.
        """
        bucket_name = "alert-ingest-integration-test-bucket-schemas"
        logger.info("creating bucket %s", bucket_name)
        try:
            s3.create_bucket(Bucket=bucket_name)
            bucket = s3_resource.Bucket(bucket_name)
        except ClientError:
            logger.warning("bucket already exists!")
            bucket = s3_resource.Bucket(bucket_name)

        def delete_bucket():
            logger.info("deleting bucket %s", bucket_name)
            bucket.objects.all().delete()
            bucket.delete()

        cls.addClassCleanup(delete_bucket)
        cls.schema_bucket_name = bucket_name

    @classmethod
    def _set_kafka_creds(cls):
        """
        Load Kafka username, password, host, and port from an environment
        variable.
        """
        kafka_url = _load_required_env_var("kafka_url")
        parsed_url = urllib.parse.urlparse(kafka_url)
        if (
            parsed_url.scheme != "kafka"
            or parsed_url.username is None
            or parsed_url.password is None
        ):
            raise ValueError(
                "ALERT_INGEST_TEST_KAFKA_URL's required format is "
                + "'kafka://USERNAME:PASSWORD@HOSTNAME[:PORT]'"
            )

        cls.kafka_username = parsed_url.username
        cls.kafka_password = parsed_url.password
        cls.kafka_hostport = parsed_url.hostname
        if parsed_url.port is not None:
            cls.kafka_hostport += ":" + str(parsed_url.port)

    @classmethod
    def _create_test_topic(cls):
        """
        Create a topic named 'alert_ingest_integration_test_topic'. Delete it
        when the test is done.

        Uses the credentials from cls._set_kafka_creds. Expects the broker to
        use SCRAM-SHA-256 plain authentication over SSL.
        """
        ssl_ctx = ssl.SSLContext()
        ssl_ctx.load_default_certs()

        client = KafkaAdminClient(
            bootstrap_servers=cls.kafka_hostport,
            client_id="alert-database-ingester-integration-test",
            sasl_plain_username=cls.kafka_username,
            sasl_plain_password=cls.kafka_password,
            sasl_mechanism="SCRAM-SHA-512",
            security_protocol="SASL_PLAINTEXT",
        )
        cls.addClassCleanup(client.close)

        topic_name = "alert_ingest_integration_test_topic"
        new_topic = NewTopic(name=topic_name, num_partitions=4, replication_factor=1)

        logger.info("creating topic %s", topic_name)
        try:
            client.create_topics([new_topic])
        except kafka.errors.TopicAlreadyExistsError:
            logger.warning("topic already exists!")

        def delete_topic():
            logger.info("deleting topic %s", topic_name)
            client.delete_topics([topic_name])

        cls.addClassCleanup(delete_topic)

        cls.topic_name = topic_name

    @classmethod
    def _load_schema_registry_creds(cls):
        """
        Parse the registry URL provided by environment variable to pull out
        credentials.
        """
        registry_url = _load_required_env_var("registry_url")
        parsed_url = urllib.parse.urlparse(registry_url)
        if (
            parsed_url.scheme != "https"
            or parsed_url.username is None
            or parsed_url.password is None
        ):
            raise ValueError(
                "schema registry URL's required format is "
                + "'https://USERNAME:PASSWORD@HOSTNAME[:PORT]'"
            )
        cls.registry_address = registry_url
        cls.registry_username = parsed_url.username
        cls.registry_password = parsed_url.password
        cls.registry_hostport = parsed_url.hostname
        if parsed_url.port is not None:
            cls.registry_hostport += ":" + parsed_url.port

    @classmethod
    def _setup_test_schema(cls):
        """
        Setup a alert schema.
        """
        cls.schema = _load_test_schema()
        cls.schema_id = 300


def _load_test_schema():
    return lsst.alert.packet.Schema.from_file().definition
