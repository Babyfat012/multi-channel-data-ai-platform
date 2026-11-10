#!/usr/bin/env bash
# Load the DP1 Connection and Variables into the Airflow metadata DB, shared by the DAGs.
set -euo pipefail
cd "$(dirname "$0")/../.."
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"

airflow_cmd() {
  docker compose --profile airflow exec -T airflow airflow "$@"
}

# Create credential Connections in the metadata DB without passing passwords via argv.
docker compose --profile airflow exec -T airflow python /opt/platform/scripts/setup/bootstrap_airflow_config.py

# Airflow may pre-create spark_default pointing at YARN. Replace exactly this Connection in
# PLATFORM's own DB so rerunning setup still yields the same configuration.
airflow_cmd connections delete spark_default >/dev/null 2>&1 || true
airflow_cmd connections add spark_default \
  --conn-type spark --conn-host 'spark://spark-master' --conn-port 7077 \
  --conn-extra '{"deploy-mode":"client"}'

airflow_cmd variables set platform_spark_packages \
  'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0,org.postgresql:postgresql:42.7.4'
airflow_cmd variables set platform_dp1_amazon_window_days '10'
airflow_cmd variables set platform_dp1_amazon_window_start '2026-04-05'
airflow_cmd variables set platform_dp1_amazon_table 'raw_amazon_orders_dp1'
airflow_cmd variables set platform_dp1_walmart_table 'raw_walmart_orders_dp1'

printf 'DP1 Connection + Variables loaded. DAGs use the *_dp1 trial tables.\n'

# Iceberg maintenance: put the whole JSON config into a Variable because configs/ is not mounted into Airflow.
docker compose --profile airflow exec -T airflow python -c \
  'import sys; from airflow.models import Variable; Variable.set("platform_maintenance_config", sys.stdin.read())' \
  < configs/iceberg_maintenance.json

# DAG v2: in-container path; the planner must mount the fixture before triggering.
airflow_cmd variables set platform_order_lifecycle_receipt_dir '/opt/airflow/receipts/order_lifecycle'
airflow_cmd variables set platform_order_lifecycle_source_root '/data/order_lifecycle'
airflow_cmd variables set platform_order_lifecycle_cutoff '2026-06-01T00:00:00.000000Z'
airflow_cmd variables set platform_order_lifecycle_driver_memory '3g'
airflow_cmd variables set platform_order_lifecycle_executor_memory '4g'
airflow_cmd variables set platform_order_lifecycle_executor_cores '2'

# AWS (t3.xlarge): the worker only has 3g/1 core, so the executor is scaled down; sources are read via s3a://.
if [ "${PLATFORM_STORAGE:-minio}" = s3 ]; then
  # v1 DAGs on AWS: load the dp1/dp2/dp3 table names; s3a:// sources are set via AIRFLOW_VAR_* in compose.
  for setup in dp1_file_channels dp1_cdc dp2_silver dp2_file_channels dp3_features; do
    bash "scripts/setup/setup_${setup}.sh"
  done
  # dp1_cdc on AWS: S3 package (hadoop-aws) plus the Kafka connector, replacing setup_dp1_cdc.sh's MinIO package set.
  airflow_cmd variables set platform_cdc_spark_packages \
    "${PLATFORM_SPARK_PACKAGES_AWS:?AWS packages are required},org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
  airflow_cmd variables set platform_spark_packages_aws "${PLATFORM_SPARK_PACKAGES_AWS:?AWS packages are required}"
  # Maintenance on AWS: real tables replace the demo tables (which do not exist on EC2); rerunning setup keeps this.
  docker compose --profile airflow exec -T airflow python -c \
    'import sys; from airflow.models import Variable; Variable.set("platform_maintenance_config", sys.stdin.read())' \
    < configs/iceberg_maintenance_aws.json
  airflow_cmd variables set platform_maintenance_tables '["lakehouse.bronze.raw_shopee_orders_dp1"]'
  airflow_cmd variables set platform_order_lifecycle_source_root "s3a://${RAW_BUCKET:?}/order_lifecycle/${PLATFORM_DATASET_NAME:-fixture}"
  airflow_cmd variables set platform_order_lifecycle_cutoff "${PLATFORM_ORDER_LIFECYCLE_CUTOFF:-2026-06-01T00:00:00.000000Z}"
  airflow_cmd variables set platform_order_lifecycle_driver_memory '2g'
  airflow_cmd variables set platform_order_lifecycle_executor_memory '2g'
  airflow_cmd variables set platform_order_lifecycle_executor_cores '1'
  if [[ "${PLATFORM_RUNTIME_PROFILE:-standard}" == performance ]]; then
    airflow_cmd variables set platform_order_lifecycle_driver_memory '4g'
    airflow_cmd variables set platform_order_lifecycle_executor_memory '4g'
    airflow_cmd variables set platform_order_lifecycle_executor_cores '2'
  fi
fi

# Separate CDC v2: the planner must create and mount the oracle before triggering the paused DAG.
airflow_cmd variables set platform_order_lifecycle_cdc_control_dir '/opt/airflow/platform-control/order_lifecycle_cdc'
airflow_cmd variables set platform_order_lifecycle_cdc_oracle '/opt/airflow/platform-control/order_lifecycle_cdc/order_lifecycle_cdc_expected.json'
airflow_cmd variables set platform_order_lifecycle_cdc_spark_packages 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3'

# ML Platform: feature store. Redis lives in the kind cluster (scripts/setup/setup_kind_feature_store.sh).
airflow_cmd variables set platform_feast_root 's3://platform/feature-store'
airflow_cmd variables set platform_feast_redis 'platform-control-plane:30379'
