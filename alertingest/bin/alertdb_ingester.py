import asyncio
import argparse

from aiokafka import AIOKafkaConsumer

from alertingest.ingester import IngestWorker
from alertingest.storage import FileBackend, GoogleObjectStorageBackend
from alertingest.schema_registry import SchemaRegistryClient


def main():
    parser = argparse.ArgumentParser(
        "alertdb-ingester",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Run a worker to copy alerts from Kafka into an object store backend.",
    )
    parser.add_argument(
        "--backend",
        type=str,
        choices=("local-files", "google-cloud"),
        default="local-files",
        help="backend to use to source alerts",
    )
    parser.add_argument(
        "--local-file-root",
        type=str,
        default=None,
        help="when using the local-files backend, the root directory where alerts should be found",
    )
    parser.add_argument(
        "--gcp-project",
        type=str,
        default=None,
        help="when using the google-cloud backend, the name of the GCP project",
    )
    parser.add_argument(
        "--gcp-bucket",
        type=str,
        default=None,
        help="when using the google-cloud backend, the name of the Google Cloud Storage bucket",
    )
    parser.add_argument(
        "--kafka-host", type=str, default="alert-broker.scratch.lsst.codes"
    )
    parser.add_argument("--kafka-topic", type=str, default="alerts")
    parser.add_argument(
        "--schema-registry-host", type=str, default="alert-schemas.scratch.lsst.codes"
    )
    args = parser.parse_args()

    # Configure the right backend
    if args.backend == "local-files":
        if args.local_file_root is None:
            parser.error("--backend=local-files requires --local-file-root be set")
        backend = FileBackend(args.local_file_root)
    elif args.backend == "google-cloud":
        if args.gcp_project is None:
            parser.error("--backend=google-cloud requires --gcp-project be set")
        if args.gcp_bucket is None:
            parser.error("--backend=google-cloud requires --gcp-bucket be set")
        backend = GoogleObjectStorageBackend(args.gcp_project, args.gcp_bucket)
    else:
        # Shouldn't be possible if argparse is using the choices parameter as expected...
        raise AssertionError("only valid --backend choices are local-files and google-cloud")
    kafka_consumer = AIOKafkaConsumer(args.kafka_topic, bootstrap_servers=[args.kafka_host])
    registry = SchemaRegistryClient(args.schema_registry_host)

    worker = IngestWorker(kafka_consumer, backend, registry)
    asyncio.run_forever(worker.run())
