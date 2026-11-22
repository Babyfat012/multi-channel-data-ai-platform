"""Run the order-snapshot Silver jobs on the Spark standalone cluster.

Kept apart from scripts/evidence/measure_order_snapshots_bronze.sh: that one is Bronze performance evidence tied to
numbers in docs/data-platform/reference/spark.md; this one builds usable Silver tables.

Start MinIO FIRST (spark-master does not depend on it; without it the job breaks exactly at write time with
`BadRequestException: Malformed request`):
    docker compose up -d minio minio-init postgres
    docker compose --profile spark up -d spark-master spark-worker-1 spark-worker-2
Do NOT run a bare `docker compose --profile spark up -d`: it also starts generator-batch and regenerates 100 GB.

Targets:
  smoke    quick run on the two SMALL Bronze tables, writing `_smoke` Silver tables
  dup      DEDUP EVIDENCE: Bronze from 10 CONSECUTIVE days (2026-04-05..04-14), then Silver on it. Consecutive days
           because generator duplicates only fall within overlap_days=3; the window covers 04-07..04-12 where both
           schema shapes overlap, so one run proves dedup and schema evolution. Measured: 134,990 duplicates
           (1.5976%), 34,080 spanning both shapes (docs/data-platform/reference/silver.md §10.6). ~400 MB: DROP after.
  walmart  real Walmart table -> silver.stg_walmart_order_lines
  amazon   real Amazon table  -> silver.stg_amazon_order_lines (HEAVY: 108 million rows)
  all      walmart then amazon

    python3 -m scripts.data.run_order_snapshots_silver [smoke|dup|walmart|amazon|all]
"""

import argparse
import os
import re
import shlex
import subprocess
from pathlib import Path

from scripts.common import ROOT

ICEBERG_PKGS = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0"
LOG_DIR = Path("out/silver-runs")
SUMMARY = re.compile(
    r"^(run_id|(source|target) table|Order Line|duplicates dropped|duplicate ratio|DONE)"
)


def submit(name: str, job: str, *flags: str, summary: re.Pattern = SUMMARY) -> None:
    """spark-submit a job from worker-1 (the master container has 512 MB and gets OOM-killed); print the summary."""
    log = LOG_DIR / f"{name}.log"
    print(f"==> {name}  (log: {log})")
    container = os.environ.get("MASTER_CONTAINER", "platform-spark-worker-1-1")
    cmd = [
        "docker",
        "exec",
        container,
        "/opt/spark/bin/spark-submit",
        "--master",
        "spark://spark-master:7077",
        "--packages",
        ICEBERG_PKGS,
        "--driver-memory",
        os.environ.get("DRIVER_MEMORY", "600m"),
        *shlex.split(os.environ.get("EXTRA_SUBMIT_ARGS", "")),
        f"/opt/spark_jobs/{job}",
        *flags,
    ]
    with log.open("w") as out:
        code = subprocess.run(cmd, stdout=out, stderr=subprocess.STDOUT).returncode
    output = log.read_text().splitlines()
    if code != 0:
        print("\n".join(output[-20:]))
        raise SystemExit(f"{name} failed ({code}); see {log}")
    print("\n".join([line for line in output if summary.match(line)] or output[-20:]))


def main() -> None:
    """Run the chosen target."""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "target", nargs="?", default="smoke", choices=["smoke", "dup", "walmart", "amazon", "all"]
    )
    target = parser.parse_args().target
    os.chdir(ROOT)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    if target == "smoke":
        # Small Bronze tables from the Bronze build: Walmart `*_smoke` (20 files) and `raw_amazon_orders_sample`
        # (20 days spread across the schema change at export day 100 = 2026-04-10), enough for both Amazon branches.
        submit(
            "silver-walmart-smoke",
            "silver/build_walmart_order_snapshots.py",
            "--source-table",
            "raw_walmart_orders_smoke",
            "--table",
            "stg_walmart_order_lines_smoke",
        )
        submit(
            "silver-amazon-smoke",
            "silver/build_amazon_order_snapshots.py",
            "--source-table",
            "raw_amazon_orders_sample",
            "--table",
            "stg_amazon_order_lines_smoke",
        )
    elif target == "dup":
        # Bronze for 10 consecutive days (no JDBC driver needed), then Silver on exactly that table.
        submit(
            "bronze-dup",
            "bronze/ingest_amazon_order_snapshots.py",
            "--window-days",
            "10",
            "--window-start",
            "2026-04-05",
            "--aqe",
            "on",
            "--table",
            "raw_amazon_orders_dup",
            summary=re.compile(r"^(days to load|  ->|DONE)"),
        )
        submit(
            "silver-amazon-dup",
            "silver/build_amazon_order_snapshots.py",
            "--source-table",
            "raw_amazon_orders_dup",
            "--table",
            "stg_amazon_order_lines_dup",
        )
    if target in ("walmart", "all"):
        submit("silver-walmart", "silver/build_walmart_order_snapshots.py")
    if target in ("amazon", "all"):
        submit("silver-amazon", "silver/build_amazon_order_snapshots.py")


if __name__ == "__main__":
    main()
