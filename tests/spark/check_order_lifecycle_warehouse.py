"""Harness with local Spark + real PostgreSQL 16; runs only against an empty temporary DB."""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path
from urllib.parse import unquote, urlparse

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
os.environ["PLATFORM_CONTRACTS_DIR"] = str(ROOT / "contracts")


def serving(cur):
    """Capture every serving value, not only counts or money."""
    from spark_jobs.lib.order_lifecycle_warehouse import TABLES

    result = {}
    for name in TABLES:
        cur.execute(
            f"SELECT row_to_json(t)::text FROM warehouse_lifecycle.{name} t ORDER BY dataset_version, line_id"
        )
        result[name] = cur.fetchall()
    return result


def corrupt_money(cur, name, stage):
    """Inject a one-minor-unit drift into each staging row, without changing the expected Gold."""
    if name == "fact_order_lines":
        cur.execute(
            f"UPDATE warehouse_lifecycle.{stage} SET headline_product_net_minor=headline_product_net_minor+1"
        )


def main():
    """Check the first load, blocking money drift, DDL rollback mid-swap, a second swap and indexes."""
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    from generator.generate_order_lifecycle_fixture import generate_fixture
    from spark_jobs.lib.order_lifecycle_gold import build_gold
    from spark_jobs.lib.order_lifecycle_lakehouse import bronze_from_root, build_silver
    from spark_jobs.lib.order_lifecycle_warehouse import TABLES, connect, export_frames

    dsn = os.environ["WAREHOUSE_TEST_DSN"]
    parsed = urlparse(dsn)
    if parsed.scheme not in ("postgresql", "postgres"):
        raise ValueError("WAREHOUSE_TEST_DSN must be a postgresql://user:password@host:port/db URI")
    jdbc = {
        "url": f"jdbc:postgresql://{parsed.hostname}:{parsed.port or 5432}/{parsed.path.lstrip('/')}",
        "user": unquote(parsed.username or ""),
        "password": unquote(parsed.password or ""),
    }
    conn = connect(dsn)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SELECT to_regnamespace('warehouse_lifecycle')")
        if cur.fetchone()[0] is not None:
            raise ValueError(
                "The harness only accepts a temporary DB without the warehouse_lifecycle schema"
            )
    with tempfile.TemporaryDirectory(prefix="warehouse-v2-") as tmp:
        work = Path(tmp)
        generate_fixture(work / "source")
        spark = (
            SparkSession.builder.master("local[1]")
            .appName("warehouse-v2-checks")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.warehouse.dir", str(work / "spark-warehouse"))
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
        )
        try:
            bronze = bronze_from_root(spark, str(work / "source"), "warehouse-test")
            silver = build_silver(bronze=bronze)
            gold = build_gold(silver=silver, cutoff="2026-06-01T00:00:00Z", run_id="load-one")
            # Write Gold to Parquet and read it back, like the export job reads the written Iceberg table: four exports
            # on an unmaterialized Gold lineage run local[1] 3g out of memory.
            frames = {}
            for name in TABLES:
                gold["label_return_v2" if name == "label_return" else name].write.parquet(
                    str(work / "gold" / name)
                )
                frames[name] = spark.read.parquet(str(work / "gold" / name))
            sql_dir = ROOT / "sql/warehouse_lifecycle"
            first = export_frames(
                spark,
                frames,
                "load-one",
                "2026-06-01T00:00:00Z",
                work / "first.json",
                sql_dir,
                dsn,
                jdbc,
            )
            assert first["status"] == "passed"
            with conn.cursor() as cur:
                original = serving(cur)
            second = {
                name: frame.withColumn("generation_id", F.lit("load-bad"))
                for name, frame in frames.items()
            }
            try:
                export_frames(
                    spark,
                    second,
                    "load-bad",
                    "2026-06-01T00:00:00Z",
                    work / "bad.json",
                    sql_dir,
                    dsn,
                    jdbc,
                    stage_transform=corrupt_money,
                )
                raise AssertionError("Load with wrong money was not blocked")
            except ValueError:
                pass
            receipt = json.loads((work / "bad.json").read_text())
            assert receipt["checks"]["fact.money_by_channel_currency"] is False
            with conn.cursor() as cur:
                assert serving(cur) == original
            broken_sql = work / "broken-sql"
            broken_sql.mkdir()
            (broken_sql / "001_tables.sql").write_text((sql_dir / "001_tables.sql").read_text())
            (broken_sql / "002_indexes.sql").write_text(
                (sql_dir / "002_indexes.sql")
                .read_text()
                .replace("(cutoff, dataset_version", "(missing_column, dataset_version")
            )
            third = {
                name: frame.withColumn("generation_id", F.lit("load-ddl-fail"))
                for name, frame in frames.items()
            }
            failed = False
            try:
                export_frames(
                    spark,
                    third,
                    "load-ddl-fail",
                    "2026-06-01T00:00:00Z",
                    work / "ddl-fail.json",
                    broken_sql,
                    dsn,
                    jdbc,
                )
            except Exception:
                failed = True
            assert failed
            assert all(json.loads((work / "ddl-fail.json").read_text())["checks"].values())
            with conn.cursor() as cur:
                assert serving(cur) == original
            fourth = {
                name: frame.withColumn("generation_id", F.lit("load-two"))
                for name, frame in frames.items()
            }
            export_frames(
                spark,
                fourth,
                "load-two",
                "2026-06-01T00:00:00Z",
                work / "second.json",
                sql_dir,
                dsn,
                jdbc,
            )
            with conn.cursor() as cur:
                for name in TABLES:
                    cur.execute(f"SELECT DISTINCT generation_id FROM warehouse_lifecycle.{name}")
                    assert cur.fetchall() == [("load-two",)]
                cur.execute(
                    "SELECT indexname FROM pg_indexes WHERE schemaname='warehouse_lifecycle'"
                )
                indexes = {r[0] for r in cur.fetchall()}
                assert {
                    "lifecycle_fact_sales",
                    "lifecycle_fact_sku",
                    "lifecycle_label_cutoff",
                    "lifecycle_feature_pit",
                } <= indexes
            print(
                "PASS: first load; money mismatch blocked; DDL failure rolled back; second swap retained indexes"
            )
        finally:
            spark.stop()
            conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
