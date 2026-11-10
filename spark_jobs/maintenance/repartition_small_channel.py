"""Compact small files of a small Channel's table: change day → month partitioning (partition evolution), then rewrite.

The small-Channel Bronze table partitioned by day has 1 file/day (~2.5 MB), so `rewrite_data_files` usually cannot
compact anything (gap #16). The job measures first, changes the spec, rewrites everything with the new spec, measures again and blocks if
the data changed (fingerprint). The old spec stays in earlier snapshots: roll back with `rollback_to_snapshot`.

Run: spark-submit .../repartition_small_channel.py --table lakehouse.bronze.raw_shopee_orders_dp1 \
  --column export_date --receipt /opt/airflow/logs/iceberg-maintenance/repartition.json
The module only imports pyspark when building the session; the SQL is testable without Spark.
"""

from __future__ import annotations

import argparse
import statistics
import sys
import time

from spark_jobs.maintenance.maintain_iceberg_tables import (
    column_name,
    fingerprint,
    identifier,
    literal,
    write_receipt,
)

RUNS = 3


def evolve_sql(table, column):
    """Change the partition field identity(day) to months(day): old data keeps the old spec until rewritten."""
    return (
        f"ALTER TABLE {identifier(table)} REPLACE PARTITION FIELD {column_name(column)} "
        f"WITH months({column_name(column)})"
    )


def rewrite_sql(table):
    """Rewrite EVERY file with the current spec (rewrite-all), not only files below the size threshold."""
    identifier(table)
    catalog, short = table.split(".", 1)
    return (
        f"CALL `{catalog}`.system.rewrite_data_files(table => {literal(short)}, "
        "options => map('rewrite-all', 'true'))"
    )


def probe_queries(table, column):
    """Two measurement queries: full table scan and a filter on the last 30 days (by the partition column)."""
    relation = identifier(table)
    col = column_name(column)
    return {
        "count_all": f"SELECT count(*) FROM {relation}",
        "last_30_days": (
            f"SELECT count(*) FROM {relation} "
            f"WHERE {col} > (SELECT date_sub(max({col}), 30) FROM {relation})"
        ),
    }


def check_same(before, after):
    """Block if the row count or fingerprint differs: rewriting files must not change data."""
    if (before["rows"], before["fingerprint"]) != (after["rows"], after["fingerprint"]):
        raise ValueError(f"Data changed after rewrite: {before} → {after}")


def measure(spark, table, column):
    """File count, average size, number of specs in use and median time per measurement query (3 runs)."""
    files = spark.sql(
        f"SELECT count(*) AS n, coalesce(avg(file_size_in_bytes), 0) AS avg, "
        f"count(DISTINCT spec_id) AS specs FROM {identifier(table)}.files"
    ).first()
    timings = {}
    for name, query in probe_queries(table, column).items():
        seconds = []
        for _ in range(RUNS):
            started = time.perf_counter()
            spark.sql(query).collect()
            seconds.append(time.perf_counter() - started)
        timings[name] = {"median_s": round(statistics.median(seconds), 3), "runs_s": seconds}
    return {
        "data_files": files["n"],
        "avg_file_mb": round(files["avg"] / 2**20, 2),
        "specs_in_use": files["specs"],
        "timings": timings,
        **fingerprint(spark, table),
    }


def main(argv=None):
    """Measure → change spec → rewrite → measure, writing the receipt even on failure."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--table", required=True)
    parser.add_argument("--column", required=True)
    parser.add_argument("--receipt", required=True)
    args = parser.parse_args(argv)
    identifier(args.table)

    from spark_jobs.lib.iceberg_session import build_spark

    spark = build_spark("platform-repartition-small-channel", shuffle_partitions=8)
    receipt = {"table": args.table, "column": args.column, "status": "running"}
    try:
        receipt["before"] = measure(spark, args.table, args.column)
        receipt["snapshot_before"] = spark.sql(
            f"SELECT snapshot_id FROM {identifier(args.table)}.snapshots "
            "ORDER BY committed_at DESC LIMIT 1"
        ).first()[0]
        receipt["sql"] = [evolve_sql(args.table, args.column), rewrite_sql(args.table)]
        for statement in receipt["sql"]:
            spark.sql(statement).collect()
        receipt["after"] = measure(spark, args.table, args.column)
        check_same(receipt["before"], receipt["after"])
        receipt["status"] = "success"
        return 0
    except Exception as error:
        receipt.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        write_receipt(args.receipt, receipt)
        print(
            f"repartition {receipt['status']}: {receipt.get('before', {}).get('data_files')} → "
            f"{receipt.get('after', {}).get('data_files')} file; receipt {args.receipt}"
        )
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
