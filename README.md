# Alert DB Ingester #

This is the Kafka ingester for the Rubin Observatory's alert database. The
database is described more thoroughly in [DMTN-183](https://dmtn-183.lsst.io/).

The ingester is expected to run continuously. It does these things:

 1. Reads alert data from Kafka off the live feed.
 2. Gzips the raw Kafka messages and copies them into a Google Cloud Storage
    bucket, indexed by alert ID.
 3. Ensures that the schemas used in all messages are copied into Google Cloud
    Storage from Confluent Schema Registry.

## Installation ##

Clone, and install with pip (probably in a virtualenv):
```
git clone git@github.com:lsst-dm/alert_database_ingester.git
cd alert_database_ingester
python -m virtualenv virtualenv
source virtualenv/bin/activate
pip install .
```

## Running the ingester ##

The ingester is installed by `pip` as a command named `alertdb-ingester`:
```
% usage: alertdb-ingester [-h] [--gcp-project GCP_PROJECT]
                        [--gcp-bucket GCP_BUCKET] [--kafka-host KAFKA_HOST]
                        [--kafka-topic KAFKA_TOPIC]
                        [--kafka-group KAFKA_GROUP]
                        [--kafka-username KAFKA_USERNAME]
                        [--schema-registry-host SCHEMA_REGISTRY_HOST]

Run a worker to copy alerts from Kafka into an object store backend.

optional arguments:
  -h, --help            show this help message and exit
  --gcp-project GCP_PROJECT
                        when using the google-cloud backend, the name of the
                        GCP project (default: alert-stream)
  --gcp-bucket GCP_BUCKET
                        when using the google-cloud backend, the name of the
                        Google Cloud Storage bucket (default: rubin-alert-
                        archive)
  --kafka-host KAFKA_HOST
                        kafka host with alert data (default: alertbroker-
                        scratch.lsst.codes)
  --kafka-topic KAFKA_TOPIC
                        name of the Kafka topic with alert data (default:
                        alerts)
  --kafka-group KAFKA_GROUP
                        Name of a Kafka Consumer group to run under (default:
                        alertdb-ingester)
  --kafka-username KAFKA_USERNAME
                        Username to use when connecting to Kafka (default:
                        admin)
  --schema-registry-host SCHEMA_REGISTRY_HOST
                        Address of a Confluent Schema Registry server hosting
                        schemas (default: alertschemas-scratch.lsst.codes)
```

The ingester needs a Kafka password. It gets this from you via an environment variable, `ALERTDB_KAFKA_PASSWORD`.

So, an example invocation:

```
export ALERTDB_KAFKA_PASSWORD=...
alertdb-ingester
```


## Development ##

### Dev setup

Install the dev dependencies and in editable mode with `pip install --editable
'.[dev]'`.

### Running tests

Run tests with `pytest`. You can just do `pytest .` in the repo root.

Integration tests require some credentials. If you don't have them set, the
integration tests will be skipped. You need these - you must fill in the actual
usernames and passwords, of course:

```
export ALERT_INGEST_TEST_KAFKA_URL=kafka://username:password@alertbroker-scratch.lsst.codes
export ALERT_INGEST_TEST_REGISTRY_URL=https://username:password@alertschemas-scratch.lsst.codes
export ALERT_INGEST_TEST_GCP_PROJECT=alert-stream
```

Then, `pytest .` will run the integration tests, which create temporary Kafka,
Google Cloud, and Schema Registry resources and run against them.
