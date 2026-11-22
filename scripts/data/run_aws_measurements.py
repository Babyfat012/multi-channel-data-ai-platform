"""Measure on the real tables/sources on EC2 (PLATFORM_STORAGE=s3); run after the v2 DAG, before the DataHub phase.

Output: out/aws-evidence/<date>/<what>-<run>/ (JSON, logs, Spark events), copied even when Spark fails.
Skew uses the current S3 source prefix; a scale source with enough history is needed to conclude on skew.

    python3 -m scripts.data.run_aws_measurements --what skew|maintenance|warehouse
"""

import argparse
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

from scripts.common import ROOT, env, env_defaults, run, succeeds, tee

COMPOSE = ["docker", "compose", "--profile", "airflow", "--profile", "spark"]
# Runtime config only; Docker Compose reads the secrets from .env itself.
RUNTIME_KEYS = [
    "PLATFORM_STORAGE",
    "AWS_REGION",
    "RAW_BUCKET",
    "PLATFORM_DATASET_NAME",
    "PLATFORM_SPARK_PACKAGES_AWS",
    "COMPOSE_FILE",
    "PLATFORM_ORDER_LIFECYCLE_CUTOFF",
]
# Runs inside the Airflow container: pick SKU/dataset/timestamp from the freshly exported features, never guess.
WAREHOUSE_EXPLAIN = """
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
"""


def submit(work: str, *job: str) -> list[str]:
    """spark-submit from the Airflow container onto the cluster, sized for the EC2 workers."""
    return [
        *COMPOSE,
        "exec",
        "-T",
        "-e",
        f"PLATFORM_SPARK_EVENT_LOG_DIR=file://{work}/events",
        "airflow",
        "spark-submit",
        "--master",
        "spark://spark-master:7077",
        "--driver-memory",
        "2g",
        "--conf",
        "spark.driver.host=airflow",
        "--conf",
        "spark.driver.bindAddress=0.0.0.0",
        "--conf",
        "spark.cores.max=2",
        "--conf",
        "spark.executor.cores=1",
        "--executor-memory",
        "2g",
        "--conf",
        "spark.jars.ivy=/opt/airflow/.ivy2",
        "--packages",
        os.environ["PLATFORM_SPARK_PACKAGES_AWS"],
        *job,
    ]


def measure(what: str, work: str, output: Path) -> None:
    """Run the Spark job(s) for one measurement; logs are mirrored into the output folder."""
    cutoff = os.environ.get("PLATFORM_ORDER_LIFECYCLE_CUTOFF") or "2026-06-01T00:00:00.000000Z"
    if what == "skew":
        source = f"s3a://{os.environ['RAW_BUCKET']}/order_lifecycle/{os.environ.get('PLATFORM_DATASET_NAME') or 'fixture'}"
        tee(
            submit(
                work,
                "/opt/spark_jobs/measure/measure_order_lifecycle_skew.py",
                "--source",
                source,
                "--workdir",
                f"{work}/skew",
                "--output",
                f"{work}/skew.json",
                "--buckets",
                "18",
                "--partitions",
                "16",
            ),
            output / "run.log",
        )
    elif what == "maintenance":
        # The existing policy, applied to the real Gold fact instead of the demo table.
        policy = next(
            iter(
                json.loads((ROOT / "configs/iceberg_maintenance.json").read_text())[
                    "tables"
                ].values()
            )
        )
        config = json.dumps({"tables": {"lakehouse.gold.lifecycle_fact_order_lines": policy}})
        tee(
            submit(
                work,
                "/opt/spark_jobs/maintenance/maintain_iceberg_tables.py",
                "--config-json",
                config,
                "--execute",
                "--receipt",
                f"{work}/maintenance.json",
            ),
            output / "run.log",
        )
        tee(
            submit(
                work,
                "/opt/spark_jobs/maintenance/maintain_iceberg_tables.py",
                "--validate-receipt",
                f"{work}/maintenance.json",
            ),
            output / "validate.log",
        )
    else:
        run_id = (
            os.environ.get("PLATFORM_V2_RUN_ID")
            or (ROOT / "out/receipts-v2-run/latest-run-id").read_text().strip()
        )
        tee(
            submit(
                work,
                "/opt/spark_jobs/warehouse/export_order_lifecycle.py",
                "--run-id",
                run_id,
                "--cutoff",
                cutoff,
                "--sql-dir",
                "/opt/sql/warehouse_lifecycle",
                "--receipt",
                f"{work}/warehouse-export.json",
            ),
            output / "run.log",
        )
        tee(
            [
                *COMPOSE,
                "exec",
                "-T",
                "airflow",
                "python",
                "-",
                cutoff,
                f"{work}/warehouse-explain.json",
            ],
            output / "explain.log",
            stdin=WAREHOUSE_EXPLAIN,
        )


def main() -> None:
    """Check config, start the cluster, measure, and always copy the evidence back."""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--what", required=True, choices=["skew", "maintenance", "warehouse"])
    what = parser.parse_args().what
    os.chdir(ROOT)
    env_defaults(RUNTIME_KEYS)
    if os.environ.get("PLATFORM_STORAGE") != "s3":
        sys.exit("Only runs with PLATFORM_STORAGE=s3")
    for key in ("RAW_BUCKET", "AWS_REGION", "PLATFORM_SPARK_PACKAGES_AWS"):
        env(key)
    if run(
        ["docker", "ps", "-q", "--filter", "label=com.docker.compose.project=datahub"], capture=True
    ):
        sys.exit("Stop the DataHub phase before measuring to free enough RAM.")
    now = datetime.now(UTC)
    name = f"{what}-{now:%Y%m%dT%H%M%S}-{os.getpid()}"
    output = Path(f"out/aws-evidence/{now:%Y-%m-%d}/{name}")
    work = f"/spark-events/aws-evidence/{now:%Y-%m-%d}/{name}"
    output.mkdir(parents=True, exist_ok=True)
    run(
        [
            *COMPOSE,
            "up",
            "-d",
            "--wait",
            "--wait-timeout",
            "600",
            "airflow",
            "spark-master",
            "spark-worker-1",
            "spark-worker-2",
        ]
    )
    run([*COMPOSE, "exec", "-T", "airflow", "mkdir", "-p", f"{work}/events"])
    (output / "runtime-images.json").write_text(
        run([*COMPOSE, "images", "--format", "json"], capture=True) + "\n"
    )
    code = 0
    try:
        measure(what, work, output)
    except SystemExit as exc:
        code = exc.code if isinstance(exc.code, int) else 1
        if not isinstance(exc.code, int):
            print(exc.code, file=sys.stderr)
    finally:
        # Copy the evidence even if Spark failed; do not stop the stack before the next measurement.
        if not succeeds([*COMPOSE, "cp", f"airflow:{work}/.", f"{output}/"]) and code == 0:
            code = 1
        print(f"Evidence {what}: {output} (exit {code})")
    sys.exit(code)


if __name__ == "__main__":
    main()
