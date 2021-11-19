import argparse
import asyncio
import os

from alertingest.ingester import IngestWorker, KafkaConnectionParams
from alertingest.schema_registry import SchemaRegistryClient
from alertingest.storage import GoogleObjectStorageBackend


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
        "--kafka-auth-mechanism",
        type=str,
        choices=("mtls", "scram"),
        default="scram",
        help="Kafka authentication mechanism to use",
    )
    parser.add_argument(
        "--kafka-username",
        type=str,
        default="admin",
        help="Username to use when connecting to Kafka. Only used if --kafka-auth-mechanism=ssl",
    )
    parser.add_argument(
        "--tls-client-key-location",
        type=str,
        default="",
        help=(
            "Path to a client PEM key used for mTLS authentication. "
            "Only used if --kafka-auth-mechanism=scram."
        ),
    )
    parser.add_argument(
        "--tls-client-crt-location",
        type=str,
        default="",
        help=(
            "Path to a client public cert used for mTLS authentication. "
            "Only used if --kafka-auth-mechanism=scram."
        ),
    )
    parser.add_argument(
        "--tls-server-ca-crt-location",
        type=str,
        default="",
        help=(
            "Path to a CA public cert used to verify the server's TLS cert. "
            "Only used if --kafka-auth-mechanism=scram."
        ),
    )
    parser.add_argument(
        "--schema-registry-host",
        type=str,
        default="alertschemas-scratch.lsst.codes",
        help="Address of a Confluent Schema Registry server hosting schemas",
    )
    args = parser.parse_args()

    if args.kafka_auth_mechanism == "scram":
        kafka_params = KafkaConnectionParams.with_scram(
            host=args.kafka_host,
            topic=args.kafka_topic,
            group=args.kafka_group,
            username=args.kafka_username,
            password=os.environ["ALERTDB_KAFKA_PASSWORD"],
        )
    elif args.kafka_auth_mechanism == "mtls":
        kafka_params = KafkaConnectionParams.with_scram(
            host=args.kafka_host,
            topic=args.kafka_topic,
            group=args.kafka_group,
            client_key_path=args.tls_client_key_location,
            client_crt_path=args.tls_client_crt_location,
            server_ca_crt_path=args.tls_server_ca_crt_location,
        )
    else:
        raise AssertionError("--kafka-auth-mechanism must be either scram or mtls")

    backend = GoogleObjectStorageBackend(args.gcp_project, args.gcp_bucket)
    registry = SchemaRegistryClient(args.schema_registry_host)

    worker = IngestWorker(kafka_params, backend, registry)
    asyncio.get_event_loop().run_until_complete(worker.run())
