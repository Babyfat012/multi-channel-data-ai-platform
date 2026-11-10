#!/usr/bin/env bash
# Run one v2 source Spark job (20_bronze / 21_silver) in the Airflow image, connected to Lakekeeper/MinIO.
#
# Requires the stack running: docker compose --profile catalog up -d postgres minio lakekeeper
# Walmart via JDBC: load first with scripts/setup/load_order_lifecycle_walmart.py, then add --walmart-jdbc walmart_lifecycle.orders.
# The fixture is read-only; Iceberg/PostgreSQL jars come from the ivy cache in the platform_airflow-data volume (no network downloads).
#
#   FIXTURE=out/source-v2-b-final-20261018-051930425055/default \
#     scripts/run/run_order_lifecycle_spark.sh /opt/spark_jobs/bronze/ingest_order_lifecycle.py --root /data/order_lifecycle --run-id X
#   RECEIPTS=out/source-v2-receipts \
#     scripts/run/run_order_lifecycle_spark.sh /opt/spark_jobs/silver/build_order_lifecycle.py --run-id Y --receipt /receipts/silver.json
set -euo pipefail
cd "$(dirname "$0")/../.."
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"
: "${FIXTURE:?set FIXTURE to a fixture variant directory (with manifest.json)}"
RECEIPTS="${RECEIPTS:-out/source-v2-receipts}"
mkdir -p "$RECEIPTS" out/spark-events
set -a; . ./.env; set +a
JARS=/ivy/.ivy2/jars
exec docker run --rm --network platform_default --user "$(id -u):0" \
  -e AWS_ACCESS_KEY_ID="$MINIO_ROOT_USER" -e AWS_SECRET_ACCESS_KEY="$MINIO_ROOT_PASSWORD" \
  -e AWS_REGION=local-01 -e SPARK_LOCAL_HOSTNAME=localhost \
  -e POSTGRES_USER="$POSTGRES_USER" -e POSTGRES_PASSWORD="$POSTGRES_PASSWORD" -e POSTGRES_DB="$POSTGRES_DB" \
  -e POSTGRES_HOST=postgres -e POSTGRES_PORT=5432 \
  -v platform_airflow-data:/ivy:ro -v "$PWD/spark_jobs":/opt/spark_jobs:ro \
  -v "$PWD/contracts":/opt/contracts:ro \
  -v "$(realpath "$FIXTURE")":/data/order_lifecycle:ro \
  -v "$PWD/out/spark-events":/spark-events -v "$(realpath "$RECEIPTS")":/receipts \
  platform-airflow:2.10.5-spark3.5.3 \
  bash -c 'exec spark-submit "$@"' spark-submit --master 'local[2]' --driver-memory 1g \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    --jars "$JARS/org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.11.0.jar,$JARS/org.apache.iceberg_iceberg-aws-bundle-1.11.0.jar,$JARS/org.postgresql_postgresql-42.7.4.jar" \
    "$@"
