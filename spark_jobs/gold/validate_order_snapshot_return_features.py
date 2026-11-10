"""Validate the offline feature table and the prediction-time boundary."""

from __future__ import annotations

import argparse
import re
import sys

from pyspark.sql import functions as F

from spark_jobs.lib.iceberg_session import CATALOG, build_spark

IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def validate_feature_frames(fact, features, run_id):
    """Compare logical rows and identities; this is not full PIT/availability proof."""
    fact_rows = fact.count()
    actual = features.count()
    forbidden = {
        "label",
        "delivered_at",
        "label_available_at",
        "order_status",
        "channel_status",
        "last_updated_time",
        "source_export_date",
    }
    if forbidden.intersection(features.columns):
        raise ValueError("Feature table has fields only known after the Order is placed")
    bad = (
        features.filter(
            F.col("order_line_key").isNull()
            | F.col("event_timestamp").isNull()
            | F.col("created").isNull()
            | (F.col("event_timestamp") > F.col("created"))
            | F.col("quantity").isNull()
            | (F.col("quantity") <= 0)
            | F.col("unit_net_amount").isNull()
            | (F.col("unit_net_amount") < 0)
            | F.col("run_id").isNull()
            | (F.col("run_id") != run_id)
        )
        .limit(1)
        .count()
    )
    keys = ["order_line_key"]
    duplicates = features.groupBy(*keys).count().filter(F.col("count") > 1).limit(1).count()
    source_invalid = fact.filter(F.col("order_line_key").isNull()).limit(1).count()
    source_duplicates = fact.groupBy(*keys).count().filter(F.col("count") > 1).limit(1).count()
    missing = fact.select(*keys).join(features.select(*keys), keys, "left_anti").count()
    extra = features.select(*keys).join(fact.select(*keys), keys, "left_anti").count()
    wrong_time = (
        features.join(
            fact.select("order_line_key", F.col("created_at").alias("source_created_at")), keys
        )
        .filter(~F.col("event_timestamp").eqNullSafe(F.col("source_created_at")))
        .limit(1)
        .count()
    )
    result = dict(
        fact_rows=fact_rows,
        feature_rows=actual,
        invalid=bad,
        duplicate_keys=duplicates,
        source_invalid=source_invalid,
        source_duplicates=source_duplicates,
        missing=missing,
        extra=extra,
        wrong_event_time=wrong_time,
    )
    print(result)
    if fact_rows != actual or any(
        result[key] for key in result if key not in {"fact_rows", "feature_rows"}
    ):
        raise ValueError("Validate offline features failed")
    return result


def main(argv: list[str] | None = None) -> int:
    """Check feature coverage, required values, and key uniqueness."""
    parser = argparse.ArgumentParser(description="Validate feature offline DP3")
    parser.add_argument("--source-table", default="fact_order_lines_dp2")
    parser.add_argument("--table", default="feat_order_line_return_dp3")
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args(argv)
    for value in (args.source_table, args.table):
        if not IDENTIFIER.fullmatch(value):
            parser.error(f"Invalid table name: {value!r}")

    spark = build_spark("validate-return-features", shuffle_partitions=32)
    try:
        source = f"{CATALOG}.gold.{args.source_table}"
        target = f"{CATALOG}.gold.{args.table}"
        features = spark.table(target)
        validate_feature_frames(spark.table(source), features, args.run_id)
        print("Validate feature offline: PASS")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
