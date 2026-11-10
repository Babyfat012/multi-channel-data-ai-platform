#!/usr/bin/env bash
# Airflow configuration for the DP1 CDC extension.
set -euo pipefail
cd "$(dirname "$0")/../.."
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"

airflow_cmd() {
  docker compose --profile airflow exec -T airflow airflow "$@"
}

airflow_cmd variables set platform_dp1_cdc_table 'raw_walmart_cdc_v2'
airflow_cmd variables set platform_dp1_cdc_topic 'platform_cdc.walmart.orders'
airflow_cmd variables set platform_cdc_spark_packages \
  'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3'
printf 'DP1 CDC Variables loaded.\n'
