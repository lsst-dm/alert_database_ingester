import argparse
import asyncio
import logging
import os
import signal

from alertingest.ingester import IngestWorker, KafkaConnectionParams
from alertingest.schema_registry import SchemaRegistryClient
from alertingest.storage import USDFObjectStorageBackend


def str_list(value):
    return value.split(",")


def setup_logging(level):
    logging.basicConfig(
        level=level,
        format="%(asctime)s.%(msecs)03d %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main():
    parser = argparse.ArgumentParser(
        "alertdb-ingester",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Run a worker to copy alerts from Kafka into an object store backend.",
    )
    parser.add_argument(
        "--endpoint-url",
        type=str,
        default=None,
        help="when using a remote bucket, the url where the bucket is located",
    )
    parser.add_argument(
        "--bucket-alerts",
        type=str,
        default="alerts",
        help="when using the usdf backend, the name of the s3 bucket for alert packets",
    )
    parser.add_argument(
        "--bucket-schemas",
        type=str,
        default="schema",
        help="when using the usdf backend, the name of the s3 bucket for alert schemas",
    )
    parser.add_argument(
        "--kafka-host",
        type=str,
        default="usdf-alert-stream-dev.lsst.cloud:9094",
        help="kafka host with alert data",
    )

    parser.add_argument(
        "--kafka-topics",
        type=str_list,
        default=["lsst-alerts-v7.4", "lsst-alerts-v9.0"],
        help="names of the Kafka topics with alert data (comma-separated list in brackets)",
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
        default="kafka-admin",
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
        default="https://usdf-alert-schemas-dev.slac.stanford.edu",
        help="Address of a Confluent Schema Registry server hosting schemas",
    )
    parser.add_argument("--verbose", action="store_true", help="log a bunch")
    parser.add_argument("--debug", action="store_true", help="log even more")
    parser.add_argument(
        "--message-timeout",
        type=int,
        default=1800,
        help="Timeout in seconds for waiting for new messages (default: 1800)",
    )
    parser.add_argument(
        "--log-check-timeout",
        type=int,
        default=3600,
        help="Timeout in seconds for waiting for new messages (default: 3600)",
    )
    parser.add_argument(
        "--prefix-idle-timeout",
        type=int,
        default=300,
        help=(
            "Seconds of inactivity for an alert prefix before logging a "
            "per-prefix alert count summary (default: 7200)"
        ),
    )
    parser.add_argument(
        "--max-logged-prefixes",
        type=int,
        default=30,
        help="Maximum number of idle-prefix summaries to remember (default: 30)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=20,
        help="Maximum messages to fetch and process concurrently per loop iteration (default: 20)",
    )
    parser.add_argument(
        "--commit-timeout",
        type=int,
        default=600,
        help=(
            "Maximum seconds to hold uncommitted offsets before forcing a commit, "
            "regardless of commit interval (default: 600)"
        ),
    )
    parser.add_argument(
        "--commit-interval",
        type=int,
        default=100,
        help="Minimum number of messages between offset commits (default: 100)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=-1,
        help="Maximum number of messages to copy; -1 means no limit (default: -1)",
    )
    parser.add_argument(
        "--auto-offset-reset",
        type=str,
        choices=("latest", "earliest"),
        default="latest",
        help="Where to start reading when joining a new topic (default: latest)",
    )

    args = parser.parse_args()

    if args.debug:
        setup_logging(logging.DEBUG)
    elif args.verbose:
        setup_logging(logging.INFO)
    else:
        setup_logging(logging.WARNING)

    if args.kafka_auth_mechanism == "scram":
        kafka_params = KafkaConnectionParams.with_scram(
            host=args.kafka_host,
            topics=args.kafka_topics,
            group=args.kafka_group,
            username=args.kafka_username,
            password=os.environ["ALERTDB_KAFKA_PASSWORD"],
        )
    elif args.kafka_auth_mechanism == "mtls":
        kafka_params = KafkaConnectionParams.with_mtls(
            host=args.kafka_host,
            topics=args.kafka_topics,
            group=args.kafka_group,
            client_key_path=args.tls_client_key_location,
            client_crt_path=args.tls_client_crt_location,
            server_ca_crt_path=args.tls_server_ca_crt_location,
        )
    else:
        raise AssertionError("--kafka-auth-mechanism must be either scram or mtls")

    backend = USDFObjectStorageBackend(
        endpoint_url=args.endpoint_url,
        alert_bucket_name=args.bucket_alerts,
        schema_bucket_name=args.bucket_schemas,
    )
    registry = SchemaRegistryClient(args.schema_registry_address)

    worker = IngestWorker(
        kafka_params,
        backend,
        registry,
        message_timeout=args.message_timeout,
        log_check_timeout=args.log_check_timeout,
        prefix_idle_timeout=args.prefix_idle_timeout,
        max_logged_prefixes=args.max_logged_prefixes,
    )
    asyncio.run(
        _run_worker(
            worker,
            batch_size=args.batch_size,
            commit_timeout=args.commit_timeout,
            commit_interval=args.commit_interval,
            limit=args.limit,
            auto_offset_reset=args.auto_offset_reset,
        )
    )


async def _run_worker(
    worker,
    batch_size=20,
    commit_timeout=600,
    commit_interval=100,
    limit=-1,
    auto_offset_reset="latest",
):
    loop = asyncio.get_running_loop()
    task = asyncio.current_task()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, task.cancel)
    try:
        await worker.run(
            batch_size=batch_size,
            commit_timeout=commit_timeout,
            commit_interval=commit_interval,
            limit=limit,
            auto_offset_reset=auto_offset_reset,
        )
    except asyncio.CancelledError:
        pass
