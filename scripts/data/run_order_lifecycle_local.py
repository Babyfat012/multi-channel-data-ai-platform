"""Run scale source → local Iceberg → warehouse; the receipt keeps failed stages too, never secrets."""

from __future__ import annotations

import argparse
import json
import os
import resource
import sys
import tempfile
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlparse

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
PROFILES = {"small": (50, 45), "medium": (500, 60)}
SILVER = ("line_lifecycle", "native_order_lines", "observed_evidence", "coverage_validity")
STAGES = (
    "source",
    "spark_start",
    "bronze",
    "silver",
    "gold_build",
    "gold_validate",
    "gold_write",
    "warehouse",
    "explain",
)


def parse_args(argv=None):
    """The profile supplies defaults; explicit flags win, and an existing source is not regenerated."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=PROFILES, default="medium")
    parser.add_argument("--orders-per-day", type=int)
    parser.add_argument("--days", type=int)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--sku-count", type=int, default=100)
    parser.add_argument("--sku-zipf-s", type=float, default=1.2)
    parser.add_argument("--source", type=Path)
    parser.add_argument("--cutoff")
    parser.add_argument(
        "--run-id", default=datetime.now(timezone.utc).strftime("e2e-%Y%m%dT%H%M%S%fZ")
    )
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--sql-dir", type=Path, default=ROOT / "sql/warehouse_lifecycle")
    args = parser.parse_args(argv)
    orders, days = PROFILES[args.profile]
    args.orders_per_day = orders if args.orders_per_day is None else args.orders_per_day
    args.days = days if args.days is None else args.days
    from generator.generate_order_lifecycle import ScaleConfig

    try:
        ScaleConfig(
            orders_per_day=args.orders_per_day,
            days=args.days,
            seed=args.seed,
            sku_count=args.sku_count,
            sku_zipf_s=args.sku_zipf_s,
        ).validate()
        if args.cutoff:
            cutoff = datetime.fromisoformat(args.cutoff.replace("Z", "+00:00"))
            if cutoff.tzinfo is None:
                raise ValueError("cutoff must have a timezone")
    except ValueError as error:
        parser.error(str(error))
    return args


def empty_receipt(args):
    """Stable schema with public parameters only; connection environment variables are not captured."""
    return {
        "schema_version": 1,
        "status": "running",
        "run_id": args.run_id,
        "parameters": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
        "dataset_version": None,
        "cutoff": args.cutoff,
        "tables": {},
        "checks": {},
        "stages": {name: {"status": "pending", "seconds": None, "rows": {}} for name in STAGES},
        "warehouse": None,
        "explain": None,
        "peak_rss": {},
    }


def save_receipt(path, receipt):
    """Linux RSS is in KiB; the JVM driver is a child process; never add two peaks from different times."""
    receipt["peak_rss"] = {
        "python_driver_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "terminated_children_kib": resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss,
        "scope": "Linux resource.ru_maxrss; a still-running JVM may not be in children yet, not the total Spark RSS",
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(receipt, ensure_ascii=False, indent=2, default=str) + "\n")
    temporary.replace(path)


@contextmanager
def stage(receipt, path, name):
    """Measure through the action/count so lazy plan build time is not mistaken for execution."""
    item = receipt["stages"][name]
    item["status"] = "running"
    began = time.perf_counter()
    save_receipt(path, receipt)
    try:
        yield item
        item["status"] = "passed"
    except Exception as error:
        item.update(status="failed", error_type=type(error).__name__)
        raise
    finally:
        item["seconds"] = round(time.perf_counter() - began, 6)
        save_receipt(path, receipt)


def write_tables(spark, frames, zone, receipt, item):
    """Materialize into a fresh per-run catalog, then return only a DataFrame read from the table."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS local.{zone}")
    result = {}
    for name, frame in frames.items():
        physical = (
            "lifecycle_raw_records"
            if zone == "bronze" and name == "raw_source_records"
            else "lifecycle_quarantine_records"
            if zone == "silver" and name == "quarantine_records"
            else f"lifecycle_stg_{name}"
            if zone == "silver"
            else f"lifecycle_{name.removesuffix('_v2')}"
        )
        target = f"local.{zone}.{physical}"
        frame.writeTo(target).using("iceberg").create()
        result[name] = spark.table(target)
        rows = result[name].count()
        item["rows"][name] = rows
        receipt["tables"][target] = {"rows": rows}
    return result


def warehouse_connection():
    """Same DSN URI as the warehouse harness; falls back to the export module's POSTGRES_*."""
    dsn = os.getenv("WAREHOUSE_TEST_DSN")
    if not dsn:
        return None, None
    parsed = urlparse(dsn)
    if parsed.scheme not in ("postgres", "postgresql") or not parsed.hostname:
        raise ValueError("WAREHOUSE_TEST_DSN must be a PostgreSQL URI")
    host = parsed.hostname
    if ":" in host:
        host = f"[{host}]"
    return dsn, {
        "url": f"jdbc:postgresql://{host}:{parsed.port or 5432}/{parsed.path.lstrip('/')}",
        "user": unquote(parsed.username or ""),
        "password": unquote(parsed.password or ""),
    }


def run(args, receipt):
    """One Spark session, Hadoop warehouse in /tmp; PostgreSQL must be a separate throwaway DB."""
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    from generator.generate_order_lifecycle import ScaleConfig, generate_scaled
    from scripts.evidence.measure_order_lifecycle_warehouse import benchmark
    from spark_jobs.lib.order_lifecycle_gold import build_gold, validate_gold
    from spark_jobs.lib.order_lifecycle_lakehouse import bronze_from_root, build_silver
    from spark_jobs.lib.order_lifecycle_warehouse import TABLES, connect, cutoff_utc, export_frames

    spark = None
    with tempfile.TemporaryDirectory(prefix="platform-e2e-v2-", dir="/tmp") as work:
        work = Path(work)
        receipt["local_warehouse"] = str(work / "warehouse")
        try:
            with stage(receipt, args.receipt, "source") as item:
                source = args.source.resolve() if args.source else work / "source"
                if args.source:
                    manifest = json.loads((source / "manifest.json").read_text())
                    item["mode"] = "existing"
                else:
                    manifest = generate_scaled(
                        source,
                        ScaleConfig(
                            orders_per_day=args.orders_per_day,
                            days=args.days,
                            seed=args.seed,
                            sku_count=args.sku_count,
                            sku_zipf_s=args.sku_zipf_s,
                        ),
                    )
                    item["mode"] = "generated"
                receipt["dataset_version"] = manifest["dataset_version"]
                receipt["source_parameters"] = manifest.get("scenario_distribution", {}).get(
                    "parameters"
                )
                receipt["cutoff"] = args.cutoff or manifest["horizons"]["source_observation_end"]
                cutoff = receipt["cutoff"]
                cutoff_utc(cutoff)
                item["rows"] = {
                    "source_records": sum(f["rows"] for f in manifest["files"]),
                    **manifest["counts"],
                }
            with stage(receipt, args.receipt, "spark_start"):
                spark = (
                    SparkSession.builder.master("local[*]")
                    .appName("e2e-order-lifecycle-local")
                    .config("spark.sql.session.timeZone", "UTC")
                    .config("spark.sql.shuffle.partitions", "8")
                    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
                    .config("spark.sql.catalog.local.type", "hadoop")
                    .config("spark.sql.catalog.local.warehouse", (work / "warehouse").as_uri())
                    .config("spark.sql.warehouse.dir", str(work / "spark-warehouse"))
                    .getOrCreate()
                )
            with stage(receipt, args.receipt, "bronze") as item:
                bronze = write_tables(
                    spark,
                    {"raw_source_records": bronze_from_root(spark, str(source), args.run_id)},
                    "bronze",
                    receipt,
                    item,
                )["raw_source_records"]
            with stage(receipt, args.receipt, "silver") as item:
                built = build_silver(
                    bronze=bronze, as_of=cutoff, contracts_dir=str(ROOT / "contracts")
                )
                silver = write_tables(
                    spark, {name: built[name] for name in SILVER}, "silver", receipt, item
                )
                # Do not keep Silver caches once Gold has its own physical table.
                spark.catalog.clearCache()
            with stage(receipt, args.receipt, "gold_build") as item:
                gold = build_gold(silver=silver, cutoff=cutoff, run_id=args.run_id)
                for name, frame in gold.items():
                    item["rows"][name] = frame.count()
            with stage(receipt, args.receipt, "gold_validate"):
                result = validate_gold(silver, gold, cutoff)
                receipt["checks"] = result["checks"]
                receipt["gold_validation"] = result
                if not receipt["checks"] or any(receipt["checks"].values()):
                    raise ValueError("Gold validation failed")
            with stage(receipt, args.receipt, "gold_write") as item:
                gold = write_tables(spark, gold, "gold", receipt, item)
                spark.catalog.clearCache()
            dsn, jdbc = warehouse_connection()
            with stage(receipt, args.receipt, "warehouse") as item:
                conn = connect(dsn)
                try:
                    with conn.cursor() as cur:
                        cur.execute("SELECT to_regnamespace('warehouse_lifecycle')")
                        if cur.fetchone()[0] is not None:
                            raise ValueError(
                                "Only run against a throwaway DB without the warehouse_lifecycle schema"
                            )
                finally:
                    conn.close()
                receipt["warehouse"] = export_frames(
                    spark,
                    {n: gold["label_return_v2" if n == "label_return" else n] for n in TABLES},
                    args.run_id,
                    cutoff,
                    args.receipt.with_name(args.receipt.stem + "-warehouse.json"),
                    args.sql_dir,
                    dsn,
                    jdbc,
                )
                item["rows"] = {
                    n: v["postgres"]["rows"] for n, v in receipt["warehouse"]["tables"].items()
                }
            with stage(receipt, args.receipt, "explain") as item:
                sku = (
                    gold["fact_order_lines"]
                    .groupBy("seller_sku")
                    .count()
                    .orderBy(F.desc("count"), "seller_sku")
                    .first()["seller_sku"]
                )
                feature = (
                    gold["feat_sku_return_rate_90d"]
                    .filter(F.col("seller_sku") == sku)
                    .orderBy(F.desc("event_timestamp"), "line_id")
                    .first()
                )
                params = {
                    "cutoff": cutoff_utc(cutoff),
                    "sku": sku,
                    "dataset": receipt["dataset_version"],
                    "event_timestamp": feature["event_timestamp"],
                    "as_of": feature["created"],
                }
                conn = connect(dsn)
                try:
                    receipt["explain"] = benchmark(conn, args.sql_dir, params)
                finally:
                    conn.close()
                item["rows"] = {
                    phase: {q: data["median"]["rows"] for q, data in queries.items()}
                    for phase, queries in receipt["explain"]["phases"].items()
                }
        finally:
            if spark is not None:
                spark.stop()


def main(argv=None):
    """A failure receipt records only the exception type, to avoid leaking JDBC/DSN credentials."""
    args = parse_args(argv)
    receipt = empty_receipt(args)
    began = time.perf_counter()
    try:
        run(args, receipt)
        receipt["status"] = "passed"
        return 0
    except Exception as error:
        receipt.update(status="failed", error_type=type(error).__name__)
        print(f"E2E failed ({type(error).__name__}); see receipt {args.receipt}", file=sys.stderr)
        return 1
    finally:
        receipt["total_seconds"] = round(time.perf_counter() - began, 6)
        save_receipt(args.receipt, receipt)


if __name__ == "__main__":
    raise SystemExit(main())
