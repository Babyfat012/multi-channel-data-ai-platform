#!/usr/bin/env bash
# Load only the DP2 Silver stage Variables; does not reset the DP1 configuration.
set -euo pipefail
cd "$(dirname "$0")/../.."
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"

airflow_cmd() {
  docker compose --profile airflow exec -T airflow airflow "$@"
}

airflow_cmd variables set platform_dp2_amazon_bronze_table 'raw_amazon_orders_dp1'
airflow_cmd variables set platform_dp2_walmart_bronze_table 'raw_walmart_orders_dp1'
airflow_cmd variables set platform_dp2_amazon_silver_table 'stg_amazon_order_lines_dp2'
airflow_cmd variables set platform_dp2_walmart_silver_table 'stg_walmart_order_lines_dp2'
airflow_cmd variables set platform_dp2_gold_table 'fact_order_lines_dp2'
airflow_cmd variables set platform_dp2_amazon_dim_table 'dim_amazon_order_dp2'

printf 'DP2 Silver + Gold Variables loaded; source *_dp1, target *_dp2.\n'
