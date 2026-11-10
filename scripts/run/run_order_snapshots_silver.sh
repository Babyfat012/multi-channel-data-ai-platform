#!/usr/bin/env bash
# Run the two Silver jobs on the Spark standalone cluster.
#
# Why a SEPARATE script instead of adding targets to `evidence/measure_order_snapshots_bronze.sh`: that script is
# the Bronze layer performance evidence (`docs/data-platform/reference/spark.md` sections 5 and 6) — each target there
# is tied to a number recorded in the docs. Silver is a different layer, run for a different purpose (to produce
# usable tables, not comparison numbers), so it stays separate to avoid mixing them up.
#
# MinIO MUST be started first, for the same reason as Bronze: `spark-master` does not `depends_on` MinIO,
# so the job reads and computes, then breaks EXACTLY AT WRITE TIME with `BadRequestException: Malformed request`.
#   docker compose up -d minio minio-init postgres
#   docker compose --profile spark up -d spark-master spark-worker-1 spark-worker-2
#
# DO NOT run a bare `docker compose --profile spark up -d` — it also starts `generator-batch`
# and regenerates 100 GB into MinIO.
#
# Targets:
#   smoke    — quick trial run on the two SMALL Bronze tables, writing Silver tables with the
#              `_smoke` suffix. Use it to check the job runs before touching the real tables.
#   dup      — DEDUP EVIDENCE: load Bronze from 10 CONSECUTIVE days (2026-04-05..04-14), then
#              run Silver on it. The days must be consecutive because generator duplicates only
#              fall within overlap_days=3 — 20 evenly spread days let dedup catch exactly 0 rows.
#              This window deliberately covers 04-07..04-12, where the two schema shapes overlap, so
#              one run proves both dedup and schema evolution.
#              Measured: 134,990 duplicates (1.5976%), 34,080 spanning both shapes.
#              See `docs/data-platform/reference/silver.md` section 10.6. Uses ~400 MB of disk, remember to DROP after measuring.
#   walmart  — real Walmart table  -> silver.stg_walmart_order_lines
#   amazon   — real Amazon table   -> silver.stg_amazon_order_lines  (HEAVY: 108 million rows)
#   all    — walmart then amazon
#
# Usage:  bash scripts/run/run_order_snapshots_silver.sh [smoke|dup|walmart|amazon|all]
set -euo pipefail
cd "$(dirname "$0")/../.."
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"

WHICH="${1:-smoke}"
# The driver runs on worker-1, not on spark-master: the master container only has 512 MB and gets
# OOM-killed by Docker (found while running brief 02 of the Bronze layer).
MASTER_CONTAINER="${MASTER_CONTAINER:-platform-spark-worker-1-1}"
ICEBERG_PKGS="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0"
LOG_DIR="out/silver-runs"
mkdir -p "$LOG_DIR"

submit() {
  # $1 = run name (used for the log file name), $2 = job file, remaining $@ = job flags
  local name="$1"; shift
  local job="$1"; shift
  local log="$LOG_DIR/${name}.log"
  printf '==> %s  (log: %s)\n' "$name" "$log"
  docker exec "$MASTER_CONTAINER" /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages "$ICEBERG_PKGS" \
    --driver-memory "${DRIVER_MEMORY:-600m}" \
    ${EXTRA_SUBMIT_ARGS:-} \
    "/opt/spark_jobs/$job" "$@" > "$log" 2>&1
  grep -E "^run_id|^(source|target) table|^Order Line|^duplicates dropped|^duplicate ratio|^DONE" \
    "$log" || tail -20 "$log"
}

case "$WHICH" in
  smoke)
    # Two small Bronze tables already exist from building Bronze: `*_smoke` (Walmart, 20 files) and
    # `raw_amazon_orders_sample` (20 sample days). Enough to check both Amazon schema branches,
    # because 20 evenly spread days cross the schema change point (export day 100 = 2026-04-10).
    submit "silver-walmart-smoke" silver/build_walmart_order_snapshots.py \
      --source-table raw_walmart_orders_smoke --table stg_walmart_order_lines_smoke
    submit "silver-amazon-smoke" silver/build_amazon_order_snapshots.py \
      --source-table raw_amazon_orders_sample --table stg_amazon_order_lines_smoke
    ;;
  dup)
    # Two steps: load Bronze for 10 consecutive days, then Silver on exactly that table.
    # Bronze uses ICEBERG_PKGS like Silver — job 01 does not need the JDBC driver.
    printf '==> bronze-dup (10 consecutive days 2026-04-05..04-14)\n'
    docker exec "$MASTER_CONTAINER" /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 --packages "$ICEBERG_PKGS" \
      --driver-memory "${DRIVER_MEMORY:-600m}" \
      /opt/spark_jobs/bronze/ingest_amazon_order_snapshots.py \
      --window-days 10 --window-start 2026-04-05 --aqe on \
      --table raw_amazon_orders_dup > "$LOG_DIR/bronze-dup.log" 2>&1
    grep -E "^days to load|^  ->|^DONE" "$LOG_DIR/bronze-dup.log" || tail -5 "$LOG_DIR/bronze-dup.log"
    submit "silver-amazon-dup" silver/build_amazon_order_snapshots.py \
      --source-table raw_amazon_orders_dup --table stg_amazon_order_lines_dup
    ;;
  walmart|all)
    submit "silver-walmart" silver/build_walmart_order_snapshots.py
    ;;&
  amazon|all)
    submit "silver-amazon" silver/build_amazon_order_snapshots.py
    ;;
  *)
    echo "Argument must be smoke | dup | walmart | amazon | all" >&2
    exit 2
    ;;
esac
