#!/usr/bin/env bash
# Load Variables for the three new Batch Channels; Amazon/Walmart config is left unchanged.
set -euo pipefail
cd "$(dirname "$0")/../.."
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"

airflow_cmd() {
  docker compose --profile airflow exec -T airflow airflow "$@"
}

airflow_cmd variables set platform_dp1_extra_source_root '/data/channels-small/source-exports'
airflow_cmd variables set platform_dp1_tiktok_shop_table 'raw_tiktok_shop_orders_dp1'
airflow_cmd variables set platform_dp1_shopify_table 'raw_shopify_orders_dp1'
airflow_cmd variables set platform_dp1_shopee_table 'raw_shopee_orders_dp1'

printf 'DP1 Variables for TikTok Shop, Shopify, Shopee loaded.\n'
