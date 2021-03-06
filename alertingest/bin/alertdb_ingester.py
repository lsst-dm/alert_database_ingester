import argparse
import asyncio
import logging
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
        "--gcp-bucket-alerts",
        type=str,
        default="alert-packets",
        help="when using the google-cloud backend, the name of the GCS bucket for alert packets",
    )
    parser.add_argument(
        "--gcp-bucket-schemas",
        type=str,
        default="alert-schemas",
        help="when using the google-cloud backend, the name of the GCS bucket for alert schemas",
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
        "--schema-registry-address",
        type=str,
        default="https://alertschemas-scratch.lsst.codes:443",
        help="Address of a Confluent Schema Registry server hosting schemas",
    )
    parser.add_argument("--verbose", type="store_true", help="log a bunch")
    parser.add_argument("--debug", type="store_true", help="log even more")

    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    elif args.verbose:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(level=logging.WARNING)

    if args.kafka_auth_mechanism == "scram":
        kafka_params = KafkaConnectionParams.with_scram(
            host=args.kafka_host,
            topic=args.kafka_topic,
            group=args.kafka_group,
            username=args.kafka_username,
            password=os.environ["ALERTDB_KAFKA_PASSWORD"],
        )
    elif args.kafka_auth_mechanism == "mtls":
        kafka_params = KafkaConnectionParams.with_mtls(
            host=args.kafka_host,
            topic=args.kafka_topic,
            group=args.kafka_group,
            client_key_path=args.tls_client_key_location,
            client_crt_path=args.tls_client_crt_location,
            server_ca_crt_path=args.tls_server_ca_crt_location,
        )
    else:
        raise AssertionError("--kafka-auth-mechanism must be either scram or mtls")

    backend = GoogleObjectStorageBackend(
        args.gcp_project,
        args.gcp_bucket_alerts,
        args.gcp_bucket_schemas,
    )
    registry = SchemaRegistryClient(args.schema_registry_address)

    worker = IngestWorker(kafka_params, backend, registry)
    asyncio.get_event_loop().run_until_complete(worker.run())
