#!/usr/bin/env bash
# Run the Bronze 01 job three times to collect performance-tuning evidence for the measurement.
#
# Why three runs (decided 2026-10-10):
#   Run 1 — 20 days, AQE OFF  -> Spark UI screenshot shows skewed tasks + seconds.
#   Run 2 — 20 days, AQE ON   -> screenshot with even tasks + seconds. SAME 20 days, so seconds are comparable.
#   Run 3 — 180 days, AQE ON  -> the real, full Bronze table.
# The first two runs use a separate table `raw_amazon_orders_sample` so the real table is not overwritten.
# The 20 days are spread EVENLY over the whole range (see spark_jobs/lib/day_selection.py): taking 20
# consecutive days at the end makes every task the same size and the skew does not show.
#
# Event logs of all three runs are in ./out/spark-events -> reopen them with the History Server to take
# screenshots after the job finishes (the Spark UI on port 4040 dies with the job).
#
# Walmart (job 01b, reads PostgreSQL over JDBC) has its own trio, same "measure before deciding" pattern:
#   wal1  — 1 JDBC connection   -> Spark's DEFAULT behavior: one task pulls the whole table. Baseline.
#   wal16 — 16 JDBC connections -> SAME data, read in parallel. Seconds comparable because same scale.
#   wal   — the real Walmart table, final configuration.
# The first two runs write to `raw_walmart_orders_smoke` so the real table is not overwritten.
# PostgreSQL must be loaded first:
#   uv run python -m generator load-postgres --source /data/raw --sample-days 20 \
#     --dsn postgresql://<user>:<pass>@localhost:5432/sources
#
# Brief 02 (handle skew) has its own trio, NOT part of `all`. All three run on the SAME 20 sample
# days, AQE OFF like baseline run1 (72.3s), changing only configuration — the job bronze/ingest_amazon_order_snapshots.py
# stays unchanged. Each run writes to a scratch table `raw_amazon_orders_skew_*`, DROPped after measuring:
#   skew_0 — CONTROL: exactly the baseline run1 configuration (shuffle 200, AQE off), but in the SAME
#            environment as skew_a/b/c (driver on worker-1). Ran 3 times to measure the variation between
#            identical runs: 55.9 / 63.2 / 64.8s => noise 8.9s (14.5%).
#            Baseline for every comparison = mean of these three runs = 61.3s.
#   skew_a — spark.sql.shuffle.partitions 200 -> 24. Hypothesis: does NOT fix skew (the largest
#            day still lands on one task), only removes empty tasks.
#   skew_b — write.distribution-mode=none. Hypothesis: skipping the shuffle makes it faster, at the cost
#            of more small files.
#   skew_c — write.distribution-mode=range. Hypothesis: better load balancing than hash, but a
#            single export_date still cannot be split.
# The driver runs on worker-1 (limit 2.5 GB) with --driver-memory 600m, because spark-master only has 512 MB
# and gets OOM-killed by Docker. `table-default.*` is an Iceberg catalog property: it applies to NEWLY
# created tables, so only the scratch tables change; the job defaults and the two real tables do not.
#
# Usage: bash scripts/evidence/measure_order_snapshots_bronze.sh [run1|run2|run3|wal1|wal16|wal|all|skew_0|skew_a|skew_b|skew_c|drop_skew]
# AWS (on EC2, /opt/platform): PLATFORM_STORAGE=s3 bash scripts/evidence/measure_order_snapshots_bronze.sh skew_0
set -euo pipefail
cd "$(dirname "$0")/../.."
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"

WHICH="${1:-all}"
MASTER_CONTAINER="${MASTER_CONTAINER:-platform-spark-master-1}"
ICEBERG_PKGS="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0"
# PostgreSQL JDBC driver — only job 01b needs it, but it is added to both so there is no second
# list to remember. Spark ignores unused jars; the cost is a few seconds of download the first time, then cached.
JDBC_PKGS="org.postgresql:postgresql:42.7.4"
ALL_PKGS="$ICEBERG_PKGS,$JDBC_PKGS"
# AWS: /opt/platform is written by rsync (sudo), so it may not be writable; logs go to the ubuntu user's HOME.
if [[ "${PLATFORM_STORAGE:-minio}" == s3 ]]; then LOG_DIR="${LOG_DIR:-$HOME/bronze-runs}"; else LOG_DIR="${LOG_DIR:-out/bronze-runs}"; fi
mkdir -p "$LOG_DIR"

submit() {
  # $1 = run name (used for the log file name), $2 = job file, remaining $@ = job flags
  local name="$1"; shift
  local job="$1"; shift
  local log="$LOG_DIR/${name}.log"
  printf '==> %s  (log: %s)\n' "$name" "$log"
  if [[ "${PLATFORM_STORAGE:-minio}" == s3 ]]; then
    # AWS (runs in /opt/platform on EC2): driver inside the airflow container like the DAG; AWS packages and
    # the s3a:// source come from AIRFLOW_VAR_* variables already in the container, credentials via IAM role.
    docker compose exec -T airflow bash -c 'job="$1"; shift; exec spark-submit \
      --master spark://spark-master:7077 --conf spark.driver.host=airflow \
      --conf spark.driver.bindAddress=0.0.0.0 --conf spark.jars.ivy=/opt/airflow/.ivy2 \
      --packages "$AIRFLOW_VAR_PLATFORM_SPARK_PACKAGES" '"${EXTRA_SUBMIT_ARGS:-}"' \
      "/opt/spark_jobs/$job" --source "$AIRFLOW_VAR_PLATFORM_DP1_AMAZON_SOURCE" "$@"' \
      _ "$job" "$@" > "$log" 2>&1
  else
    docker exec "$MASTER_CONTAINER" /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --packages "$ALL_PKGS" \
      ${EXTRA_SUBMIT_ARGS:-} \
      "/opt/spark_jobs/$job" "$@" > "$log" 2>&1
  fi
  grep -E "^run_id|^(source|target) table|^AQE|^days to load|^partitioned by|^JDBC connections|^DONE|^Row count" \
    "$log" || tail -5 "$log"
}

case "$WHICH" in
  run1|all)
    submit "run1-20days-aqe-off" bronze/ingest_amazon_order_snapshots.py \
      --sample-days 20 --aqe off --table raw_amazon_orders_sample
    ;;&
  run2|all)
    submit "run2-20days-aqe-on" bronze/ingest_amazon_order_snapshots.py \
      --sample-days 20 --aqe on --table raw_amazon_orders_sample
    ;;&
  run3|all)
    submit "run3-180days-aqe-on" bronze/ingest_amazon_order_snapshots.py --aqe on --table raw_amazon_orders
    ;;&
  wal1|all)
    submit "wal1-jdbc-1conn" bronze/ingest_walmart_order_snapshots.py \
      --num-partitions 1 --table raw_walmart_orders_smoke
    ;;&
  wal16|all)
    submit "wal16-jdbc-16conn" bronze/ingest_walmart_order_snapshots.py \
      --num-partitions 16 --table raw_walmart_orders_smoke
    ;;&
  wal|all)
    submit "wal-full-16conn" bronze/ingest_walmart_order_snapshots.py \
      --num-partitions 16 --table raw_walmart_orders
    ;;&
  skew_0)
    # Brief 02, control: exactly the run1 configuration (default shuffle 200, AQE off), in the skew_a/b/c environment.
    MASTER_CONTAINER=platform-spark-worker-1-1 EXTRA_SUBMIT_ARGS="--driver-memory 600m" \
      submit "skew_0-control" bronze/ingest_amazon_order_snapshots.py \
        --sample-days 20 --aqe off --table raw_amazon_orders_skew_0
    ;;
  skew_a)
    # Brief 02, method a: reduce shuffle partitions 200 -> 24 (only changes an existing job flag).
    MASTER_CONTAINER=platform-spark-worker-1-1 EXTRA_SUBMIT_ARGS="--driver-memory 600m" \
      submit "skew_a-shuffle24" bronze/ingest_amazon_order_snapshots.py \
        --sample-days 20 --aqe off --shuffle-partitions 24 --table raw_amazon_orders_skew_a
    ;;
  skew_b)
    # Brief 02, method b: write.distribution-mode=none for the scratch table (no shuffle before writing).
    MASTER_CONTAINER=platform-spark-worker-1-1 \
      EXTRA_SUBMIT_ARGS="--driver-memory 600m --conf spark.sql.catalog.lakehouse.table-default.write.distribution-mode=none" \
      submit "skew_b-dist-none" bronze/ingest_amazon_order_snapshots.py \
        --sample-days 20 --aqe off --table raw_amazon_orders_skew_b
    ;;
  skew_c)
    # Brief 02, method c: write.distribution-mode=range for the scratch table.
    MASTER_CONTAINER=platform-spark-worker-1-1 \
      EXTRA_SUBMIT_ARGS="--driver-memory 600m --conf spark.sql.catalog.lakehouse.table-default.write.distribution-mode=range" \
      submit "skew_c-dist-range" bronze/ingest_amazon_order_snapshots.py \
        --sample-days 20 --aqe off --table raw_amazon_orders_skew_c
    ;;
  drop_skew)
    # Drop the scratch tables after measuring (Trino must be running: docker compose --profile query up -d trino).
    for t in skew_0 skew_a skew_b skew_c; do
      docker compose exec -T trino trino --execute "DROP TABLE IF EXISTS lakehouse.bronze.raw_amazon_orders_$t"
    done
    ;;
  run1|run2|run3|wal1|wal16|wal|all|skew_0|skew_a|skew_b|skew_c) ;;
  *) echo "Argument must be run1 | run2 | run3 | wal1 | wal16 | wal | all | skew_0 | skew_a | skew_b | skew_c | drop_skew" >&2; exit 2 ;;
esac

if [[ "${PLATFORM_STORAGE:-minio}" == s3 ]]; then
  printf '\n==> AWS: event logs in /mnt/platform-data/spark-events (owner 50000:root, mode 660 ⇒ History adds the root group: --group-add 0; --user 50000 fails because the uid has no name). History Server (loopback, tunnel 18080):\n'
  printf '    docker run -d --name spark-history --group-add 0 -p 127.0.0.1:18080:18080 -v /mnt/platform-data/spark-events:/spark-events:ro \\\n'
  printf '      -e SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=file:///spark-events" \\\n'
  printf '      apache/spark:3.5.3-java17 /opt/spark/bin/spark-class org.apache.spark.deploy.history.HistoryServer\n'
  exit 0
fi
printf '\n==> Event logs collected: %s\n' "$(ls -1 out/spark-events 2>/dev/null | wc -l) file"
printf '==> Review them with the History Server:\n'
printf '    docker run --rm -p 18080:18080 -v "$PWD/out/spark-events:/spark-events" \\\n'
printf '      -e SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=file:///spark-events" \\\n'
printf '      apache/spark:3.5.3-java17 /opt/spark/bin/spark-class \\\n'
printf '      org.apache.spark.deploy.history.HistoryServer\n'
printf '    then open http://localhost:18080\n'
