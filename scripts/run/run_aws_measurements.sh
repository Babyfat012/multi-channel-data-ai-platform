#!/usr/bin/env bash
# Measure on the real tables/sources on EC2; call after the v2 DAG, before the DataHub phase.
set -euo pipefail
cd "$(dirname "$0")/../.."
usage() {
  echo 'Usage: bash scripts/run/run_aws_measurements.sh --what skew|maintenance|warehouse'
  echo 'Reads the AWS .env. Output: out/aws-evidence/<date>/<what>-<run>/ (JSON, logs, events).'
  echo 'Skew uses the current S3 source prefix; a scale source with enough history is needed to conclude on skew.'
}
what=
while (($#)); do
  case "$1" in
    --help|-h) usage; exit 0 ;;
    --what) [[ $# -ge 2 ]] || { usage >&2; exit 2; }; what=$2; shift 2 ;;
    *) usage >&2; exit 2 ;;
  esac
done
case "$what" in skew|maintenance|warehouse) ;; *) usage >&2; exit 2 ;; esac
# Export only runtime config; Docker Compose reads secrets from .env itself.
if [[ -f .env ]]; then
  for key in PLATFORM_STORAGE AWS_REGION RAW_BUCKET PLATFORM_DATASET_NAME PLATFORM_SPARK_PACKAGES_AWS COMPOSE_FILE PLATFORM_ORDER_LIFECYCLE_CUTOFF; do
    if [[ ! -v "$key" ]]; then
      value=$( (set -a; source .env; printf '%s' "${!key:-}") )
      export "$key=$value"
    fi
  done
fi
[[ "${PLATFORM_STORAGE:-}" = s3 ]] || { echo 'Only runs with PLATFORM_STORAGE=s3' >&2; exit 2; }
: "${RAW_BUCKET:?}" "${AWS_REGION:?}" "${PLATFORM_SPARK_PACKAGES_AWS:?}"
compose=(docker compose --profile airflow --profile spark)
if [[ -n "$(docker ps -q --filter label=com.docker.compose.project=datahub)" ]]; then
  echo 'Stop the DataHub phase before measuring to free enough RAM.' >&2; exit 1
fi
day="$(date -u +%F)"
stamp="$(date -u +%Y%m%dT%H%M%S)-$$"
output="out/aws-evidence/$day/$what-$stamp"
work="/spark-events/aws-evidence/$day/$what-$stamp"
mkdir -p "$output"
"${compose[@]}" up -d --wait --wait-timeout 600 airflow spark-master spark-worker-1 spark-worker-2
"${compose[@]}" exec -T airflow mkdir -p "$work/events"
"${compose[@]}" images --format json > "$output/runtime-images.json"
# Always copy the evidence even if Spark fails; do not stop the stack before the next measurement.
collect() {
  local code=$?
  trap - EXIT
  "${compose[@]}" cp "airflow:$work/." "$output/" || { [[ "$code" != 0 ]] || code=1; }
  echo "Evidence $what: $output (exit $code)"
  exit "$code"
}
trap collect EXIT
submit() {
  "${compose[@]}" exec -T -e "PLATFORM_SPARK_EVENT_LOG_DIR=file://$work/events" airflow spark-submit \
    --master spark://spark-master:7077 --driver-memory 2g \
    --conf spark.driver.host=airflow --conf spark.driver.bindAddress=0.0.0.0 \
    --conf spark.cores.max=2 --conf spark.executor.cores=1 --executor-memory 2g \
    --conf spark.jars.ivy=/opt/airflow/.ivy2 --packages "$PLATFORM_SPARK_PACKAGES_AWS" "$@"
}
cutoff="${PLATFORM_ORDER_LIFECYCLE_CUTOFF:-2026-06-01T00:00:00.000000Z}"
case "$what" in
  skew)
    submit /opt/spark_jobs/measure/measure_order_lifecycle_skew.py \
      --source "s3a://$RAW_BUCKET/order_lifecycle/${PLATFORM_DATASET_NAME:-fixture}" \
      --workdir "$work/skew" --output "$work/skew.json" \
      --buckets 18 --partitions 16 2>&1 | tee "$output/run.log"
    ;;
  maintenance)
    # Use the same existing policy on the real Gold fact, not the demo table.
    config=$(python3 - <<'PY'
import json
from pathlib import Path
config = json.loads(Path('configs/iceberg_maintenance.json').read_text())
policy = next(iter(config['tables'].values()))
print(json.dumps({'tables': {'lakehouse.gold.lifecycle_fact_order_lines': policy}}))
PY
)
    submit /opt/spark_jobs/maintenance/maintain_iceberg_tables.py \
      --config-json "$config" --execute --receipt "$work/maintenance.json" 2>&1 | tee "$output/run.log"
    submit /opt/spark_jobs/maintenance/maintain_iceberg_tables.py \
      --validate-receipt "$work/maintenance.json" 2>&1 | tee "$output/validate.log"
    ;;
  warehouse)
    run_id="${PLATFORM_V2_RUN_ID:-$(cat out/receipts-v2-run/latest-run-id)}"
    submit /opt/spark_jobs/warehouse/export_order_lifecycle.py \
      --run-id "$run_id" --cutoff "$cutoff" --sql-dir /opt/sql/warehouse_lifecycle \
      --receipt "$work/warehouse-export.json" 2>&1 | tee "$output/run.log"
    # Pick SKU/dataset/timestamp from the freshly exported features, never guess fixture values.
    "${compose[@]}" exec -T airflow python - "$cutoff" "$work/warehouse-explain.json" <<'PY' 2>&1 | tee "$output/explain.log"
import importlib.util
import sys
from spark_jobs.lib.order_lifecycle_warehouse import connect
spec = importlib.util.spec_from_file_location(
    'warehouse_measure', '/opt/platform/scripts/evidence/measure_order_lifecycle_warehouse.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
conn = connect()
try:
    with conn.cursor() as cur:
        cur.execute('''SELECT dataset_version, seller_sku, event_timestamp, created
            FROM warehouse_lifecycle.feat_sku_return_rate_90d
            WHERE seller_sku IS NOT NULL AND event_timestamp IS NOT NULL AND created IS NOT NULL
            ORDER BY labeled_count_90d DESC, dataset_version, line_id LIMIT 1''')
        row = cur.fetchone()
        if row is None:
            raise ValueError('No features to measure; not writing empty evidence')
finally:
    conn.close()
module.main(['--cutoff', sys.argv[1], '--receipt', sys.argv[2], '--dataset', row[0],
             '--sku', row[1], '--event-timestamp', row[2].isoformat() + 'Z',
             '--as-of', row[3].isoformat() + 'Z', '--sql-dir', '/opt/sql/warehouse_lifecycle'])
PY
    ;;
esac
