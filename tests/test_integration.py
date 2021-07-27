import asyncio
import fastavro
import io
import logging
import os
import ssl
import struct
import unittest
import urllib

import aiohttp
from aiokafka import AIOKafkaProducer
import google.api_core.exceptions
import google.cloud.storage as gcs
from kafka.admin import KafkaAdminClient, NewTopic
import kafka.errors
from kafkit.registry.aiohttp import RegistryApi

import lsst.alert.packet
from lsst.alert.packet.simulate import (
    randomLong,
    randomDouble,
    randomString,
    randomInt,
    randomFloat,
)

from alertingest.ingester import KafkaConnectionParams, IngestWorker
from alertingest.storage import GoogleObjectStorageBackend
from alertingest.schema_registry import SchemaRegistryClient


logger = logging.getLogger(__name__)
logger.level = logging.DEBUG


_required_env_vars = {
    "kafka_url": "ALERT_INGEST_TEST_KAFKA_URL",
    "registry_url": "ALERT_INGEST_TEST_REGISTRY_URL",
    "gcp_project": "ALERT_INGEST_TEST_GCP_PROJECT",
}


def _load_required_env_var(name):
    key = _required_env_vars[name]
    val = os.environ.get(key)
    if val is None:
        raise unittest.SkipTest(f"the ${key} environment variable must be set")
    return val


class IngesterIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        Create:
            - a test bucket which will receive alerts
            - a test Kafka topic
            - a schema in the schema registry
        """
        cls._create_test_bucket()
        cls._set_kafka_creds()
        cls._create_test_topic()
        cls._load_schema_registry_creds()
        cls._register_test_schema()

    def test_integration(self):
        """
        Run the ingester against a real Kafka topic, Google Cloud Storage bucket,
        and Schema Registry.
        """
        kafka_group = "alert_ingest_integration_test_group"
        kafka_params = KafkaConnectionParams(
            self.kafka_hostport,
            self.topic_name,
            kafka_group,
            self.kafka_username,
            self.kafka_password,
        )
        backend = GoogleObjectStorageBackend(self.gcp_project, self.bucket_name)
        registry = SchemaRegistryClient(self.registry_hostport)

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
        loop.run_until_complete(asyncio.wait_for(run_worker, timeout=5))

        # The schema should be uploaded.
        assert backend.schema_exists(self.schema_id)

        # Each of the 5 alert should be uploaded.
        for message in messages:
            blob_url = f"/alert_archive/v1/alerts/{message['alertId']}.avro.gz"
            assert backend.bucket.blob(blob_url).exists()

    async def publish_alerts(self, alerts):
        # Publish alerts to the Kafka broker.
        ssl_ctx = ssl.SSLContext()
        ssl_ctx.load_default_certs()
        producer = AIOKafkaProducer(
            bootstrap_servers=self.kafka_hostport,
            sasl_plain_username=self.kafka_username,
            sasl_plain_password=self.kafka_password,
            sasl_mechanism="SCRAM-SHA-256",
            security_protocol="SASL_SSL",
            ssl_context=ssl_ctx,
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
        Encode an alert packet using self.schema, writing it in Confluent Wire Format.
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
    def _create_test_bucket(cls):
        """
        Create a bucket named 'alert_ingest_integration_test_bucket' and register a
        cleanup function when the test exits for any reason.
        """
        gcp_project = _load_required_env_var("gcp_project")
        client = gcs.Client(project=gcp_project)

        bucket_name = "alert_ingest_integration_test_bucket"
        logger.info("creating bucket %s", bucket_name)
        try:
            bucket = client.create_bucket(bucket_name)
        except google.api_core.exceptions.Conflict:
            logger.warning("bucket already exists!")
            bucket = client.bucket(bucket_name)

        def delete_bucket():
            logger.info("deleting bucket %s", bucket_name)
            bucket.delete(force=True)

        cls.addClassCleanup(delete_bucket)
        cls.gcp_project = gcp_project
        cls.bucket_name = bucket_name

    @classmethod
    def _set_kafka_creds(cls):
        """
        Load Kafka username, password, host, and port from an environment variable.
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
            cls.kafka_hostport += ":" + parsed_url.port

    @classmethod
    def _create_test_topic(cls):
        """
        Create a topic named 'alert_ingest_integration_test_topic'. Delete it when
        the test is done.

        Uses the credentials from cls._set_kafka_creds. Expects the broker to
        use SCRAM-SHA-256 plain authentication over SSL.
        """
        ssl_ctx = ssl.SSLContext()
        ssl_ctx.load_default_certs()

        client = KafkaAdminClient(
            bootstrap_servers=[cls.kafka_hostport],
            client_id="alert_database_ingester-integration-test",
            sasl_plain_username=cls.kafka_username,
            sasl_plain_password=cls.kafka_password,
            sasl_mechanism="SCRAM-SHA-256",
            security_protocol="SASL_SSL",
            ssl_context=ssl_ctx,
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
        cls.registry_username = parsed_url.username
        cls.registry_password = parsed_url.password
        cls.registry_hostport = parsed_url.hostname
        if parsed_url.port is not None:
            cls.registry_hostport += ":" + parsed_url.port

    @classmethod
    def _register_test_schema(cls):
        """
        Register an alert schema in the Schema Registry. Delete it when the test is
        done.
        """
        cls.schema = _load_test_schema()
        schema_subject = "alert_ingest_integration_test_subject"
        auth = aiohttp.BasicAuth(
            login=cls.registry_username, password=cls.registry_password
        )

        async def register_schema():
            async with aiohttp.ClientSession(auth=auth) as session:
                reg = RegistryApi(
                    session=session, url="https://" + cls.registry_hostport
                )
                logger.info("registering schema subject %s", schema_subject)
                schema_id = await reg.register_schema(
                    cls.schema, subject=schema_subject
                )
                cls.schema_id = schema_id
                logger.info("schema registered with ID %s", schema_id)

        async def delete_schema():
            async with aiohttp.ClientSession(auth=auth) as session:
                reg = RegistryApi(
                    session=session, url="https://" + cls.registry_hostport
                )
                logger.info("deleting schema subject %s", schema_subject)
                await reg.delete(f"/subjects/{schema_subject}?permanent=true")
                logger.info("deletion complete")

        def delete_schema_callback():
            loop = asyncio.get_event_loop()
            loop.run_until_complete(delete_schema())

        loop = asyncio.get_event_loop()
        loop.run_until_complete(register_schema())

        cls.addClassCleanup(delete_schema_callback)


def _load_test_schema():
    return lsst.alert.packet.Schema.from_file().definition
