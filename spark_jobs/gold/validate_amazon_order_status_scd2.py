"""Validate SCD2 validity ranges and the one-version relationship for each Amazon fact."""

from __future__ import annotations

import argparse
import re
import sys

from pyspark.sql import Window
from pyspark.sql import functions as F

from spark_jobs.lib.iceberg_session import CATALOG, build_spark

IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def main(argv: list[str] | None = None) -> int:
    """Validate dimension intervals and ensure every Gold row resolves once."""
    parser = argparse.ArgumentParser(description="Validate Amazon Order SCD2")
    parser.add_argument("--table", default="dim_amazon_order_dp2")
    parser.add_argument("--fact-table", default="fact_order_lines_dp2")
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args(argv)
    for value in (args.table, args.fact_table):
        if not IDENTIFIER.fullmatch(value):
            parser.error(f"Invalid table name: {value!r}")

    spark = build_spark("validate-dim-amazon-order", shuffle_partitions=64)
    try:
        table = f"{CATALOG}.gold.{args.table}"
        dim = spark.table(table)
        timeline = Window.partitionBy("order_id").orderBy("valid_from_ts")
        checked = dim.withColumn("_expected_to", F.lead("valid_from_ts").over(timeline))
        invalid = (
            checked.filter(
                F.col("order_version_key").isNull()
                | F.col("order_id").isNull()
                | F.col("valid_from_ts").isNull()
                | F.col("order_status").isNull()
                | F.col("channel_status").isNull()
                | F.col("first_observed_export_date").isNull()
                | (F.col("valid_to_ts") < F.col("valid_from_ts"))
                | ~F.col("valid_to_ts").eqNullSafe(F.col("_expected_to"))
                | (F.col("is_current") != F.col("valid_to_ts").isNull())
                | (F.col("run_id") != args.run_id)
            )
            .limit(1)
            .count()
        )
        duplicate = (
            dim.groupBy("order_version_key").count().filter(F.col("count") > 1).limit(1).count()
        )
        multiple_current = (
            dim.filter("is_current")
            .groupBy("order_id")
            .count()
            .filter(F.col("count") != 1)
            .limit(1)
            .count()
        )
        fact = spark.table(f"{CATALOG}.gold.{args.fact_table}")
        amazon = fact.filter(F.col("channel") == "amazon")
        missing_key = amazon.filter(F.col("amazon_order_version_key").isNull())
        missing_version = (
            amazon.select("amazon_order_version_key")
            .distinct()
            .join(
                dim.select("order_version_key"),
                F.col("amazon_order_version_key") == F.col("order_version_key"),
                "left_anti",
            )
            .limit(1)
            .count()
        )
        other_key = fact.filter(
            (F.col("channel") != "amazon") & F.col("amazon_order_version_key").isNotNull()
        )
        bad_fact = missing_key.limit(1).count() + other_key.limit(1).count()
        rows = dim.groupBy("is_current").count().collect()
        print(
            f"Amazon SCD2 versions={rows}; invalid={invalid}; duplicate={duplicate}; "
            f"multiple_current={multiple_current}; bad_fact={bad_fact}; "
            f"missing_version={missing_version}"
        )
        if invalid or duplicate or multiple_current or bad_fact or missing_version:
            raise ValueError("Validate Amazon Order SCD2 failed")
        print("Validate Amazon Order SCD2: PASS")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
