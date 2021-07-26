import asyncio
import argparse
import os
from aiokafka import AIOKafkaConsumer

from alertingest.ingester import KafkaConnectionParams, IngestWorker
from alertingest.storage import GoogleObjectStorageBackend
from alertingest.schema_registry import SchemaRegistryClient


def main():
    parser = argparse.ArgumentParser(
        "alertdb-ingester",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Run a worker to copy alerts from Kafka into an object store backend.",
    )
    parser.add_argument(
        "--gcp-project",
        type=str,
        default="alert-stream",
        help="when using the google-cloud backend, the name of the GCP project",
    )
    parser.add_argument(
        "--gcp-bucket",
        type=str,
        default="rubin-alert-archive",
        help="when using the google-cloud backend, the name of the Google Cloud Storage bucket",
    )
    parser.add_argument(
        "--kafka-host",
        type=str,
        default="alertbroker-scratch.lsst.codes",
        help="kafka host with alert data",
    )
    parser.add_argument(
        "--kafka-topic",
        type=str,
        default="alerts",
        help="name of the Kafka topic with alert data",
    )
    parser.add_argument(
        "--kafka-group",
        type=str,
        default="alertdb-ingester",
        help="Name of a Kafka Consumer group to run under",
    )
    parser.add_argument(
        "--kafka-username",
        type=str,
        default="admin",
        help="Username to use when connecting to Kafka",
    )
    parser.add_argument(
        "--schema-registry-host",
        type=str,
        default="alertschemas-scratch.lsst.codes",
        help="Address of a Confluent Schema Registry server hosting schemas",
    )
    args = parser.parse_args()

    kafka_params = KafkaConnectionParams(
        args.kafka_host,
        args.kafka_topic,
        args.kafka_group,
        args.kafka_username,
        os.environ["ALERTDB_KAFKA_PASSWORD"],
    )
    backend = GoogleObjectStorageBackend(args.gcp_project, args.gcp_bucket)
    registry = SchemaRegistryClient(args.schema_registry_host)

    worker = IngestWorker(kafka_params, backend, registry)
    asyncio.get_event_loop().run_until_complete(worker.run())
