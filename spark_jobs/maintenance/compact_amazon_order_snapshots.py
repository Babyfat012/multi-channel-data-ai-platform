"""Compact the seven Amazon Bronze partitions with more than one data file.

Measure the same payload-reading query before and after on exactly those dates.
"""

from __future__ import annotations

import argparse
import json
import statistics
import time

from pyspark.sql import functions as F

from spark_jobs.lib.iceberg_session import build_spark

TABLE = "lakehouse.bronze.raw_amazon_orders"
MEASURED_DATES = [
    "2026-02-23",
    "2026-05-07",
    "2026-05-16",
    "2026-05-18",
    "2026-06-11",
    "2026-06-23",
    "2026-06-28",
]


def file_stats(spark):
    """Summarize data-file count and size for the selected Bronze table."""
    files = spark.table(f"{TABLE}.files")
    summary = (
        files.agg(
            F.count("*").alias("files"),
            F.sum("record_count").alias("rows"),
            F.sum("file_size_in_bytes").alias("bytes"),
        )
        .first()
        .asDict()
    )
    duplicate = (
        files.groupBy("partition.export_date")
        .agg(F.count("*").alias("files"), F.sum("file_size_in_bytes").alias("bytes"))
        .filter(F.col("files") > 1)
        .orderBy("export_date")
        .collect()
    )
    return summary, [row.asDict() for row in duplicate]


def query_stats(spark, dates):
    """Measure query latency and rows for a fixed set of partition dates."""
    date_values = ",".join(f"DATE '{date}'" for date in dates)
    query = (
        "SELECT count(*) AS rows, sum(length(payload)) AS payload_chars "
        f"FROM {TABLE} WHERE export_date IN ({date_values})"
    )
    samples = []
    result = None
    for attempt in range(4):
        started = time.perf_counter()
        result = spark.sql(query).first().asDict()
        elapsed = time.perf_counter() - started
        if attempt:
            samples.append(round(elapsed, 3))
    return {
        "sql": query,
        "result": result,
        "samples_seconds": samples,
        "median_seconds": round(statistics.median(samples), 3),
    }


def main() -> None:
    """Compact selected Amazon Bronze partitions and report before/after metrics."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--after-only",
        action="store_true",
        help="Recheck the already compacted snapshot after an interrupted run",
    )
    args = parser.parse_args()
    spark = build_spark("platform-compact-amazon-bronze", shuffle_partitions=4)
    spark.sparkContext.setLogLevel("ERROR")
    try:
        before_files, duplicate = file_stats(spark)
        if args.after_only:
            print(
                "COMPACTION_AFTER "
                + json.dumps(
                    {
                        "files": before_files,
                        "duplicate_partitions": duplicate,
                        "query": query_stats(spark, MEASURED_DATES),
                    },
                    default=str,
                    ensure_ascii=False,
                ),
                flush=True,
            )
            return
        if not duplicate:
            raise RuntimeError("No partitions with many files left to compact")
        dates = [str(row["export_date"]) for row in duplicate]
        before_query = query_stats(spark, dates)
        print(
            "COMPACTION_BEFORE "
            + json.dumps(
                {"files": before_files, "duplicate_partitions": duplicate, "query": before_query},
                default=str,
                ensure_ascii=False,
            ),
            flush=True,
        )

        started = time.perf_counter()
        rewrite = (
            spark.sql(
                "CALL lakehouse.system.rewrite_data_files("
                "table => 'bronze.raw_amazon_orders', "
                "options => map('target-file-size-bytes', '134217728', "
                "'min-input-files', '2', 'max-concurrent-file-group-rewrites', '1'))"
            )
            .first()
            .asDict()
        )
        elapsed = round(time.perf_counter() - started, 3)
        print(
            "COMPACTION_REWRITE "
            + json.dumps({"result": rewrite, "seconds": elapsed}, ensure_ascii=False),
            flush=True,
        )

        spark.catalog.refreshTable(TABLE)
        after_files, after_duplicate = file_stats(spark)
        after_query = query_stats(spark, dates)
        print(
            "COMPACTION_AFTER "
            + json.dumps(
                {
                    "files": after_files,
                    "duplicate_partitions": after_duplicate,
                    "query": after_query,
                },
                default=str,
                ensure_ascii=False,
            ),
            flush=True,
        )
        if (
            before_files["rows"] != after_files["rows"]
            or before_query["result"] != after_query["result"]
        ):
            raise RuntimeError("Compaction changed the query data")
        if after_files["files"] >= before_files["files"]:
            raise RuntimeError("Compaction did not reduce the file count")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
