"""Join Return labels for all five Channels with the Gold fact into a single label table.

The bridge is only for simulated data. Four Channels join via the generator's key ledger
(`ground-truth/order_line_keys/<channel>/`); Walmart also needs customer_order_id, regenerated
from the seed by the key-preparation step on the host. Labels live in a separate table, not in the fact or
the features used at prediction time.
"""

from __future__ import annotations

import argparse
import re
import sys

from pyspark.sql import functions as F

from spark_jobs.lib.iceberg_session import CATALOG, build_spark
from spark_jobs.lib.label_channels import KEY_CHANNELS, missing_channels

IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
FACT_COLUMNS = (
    "channel",
    "order_line_key",
    "channel_order_id",
    "channel_secondary_order_id",
    "channel_line_id",
    "created_at",
)


def main(argv: list[str] | None = None) -> int:
    """Join source labels to Gold rows of all five Channels and validate per Channel."""
    parser = argparse.ArgumentParser(description="Return label bridge for the five Channels")
    parser.add_argument("--fact-table", default="fact_order_lines_dp2")
    parser.add_argument("--table", default="fact_return_labels_dp3")
    parser.add_argument("--ground-truth", default="/data/ground-truth")
    parser.add_argument("--keys-path", default="/data/ground-truth-bridge/walmart-keys.parquet")
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args(argv)
    for value in (args.fact_table, args.table):
        if not IDENTIFIER.fullmatch(value):
            parser.error(f"Invalid table name: {value!r}")

    # 256 instead of 32: Amazon has ~150 million lines, 32 slices is ~700 MB/slice with a 1536m executor.
    spark = build_spark("return-label-bridge", shuffle_partitions=256)
    try:
        fact = spark.table(f"{CATALOG}.gold.{args.fact_table}").select(*FACT_COLUMNS)
        walmart_keys = spark.read.parquet(args.keys_path).select(
            "order_line_id",
            "channel_order_id",
            "channel_line_id",
            "channel_secondary_order_id",
        )
        walmart = fact.filter(F.col("channel") == "walmart").join(
            walmart_keys,
            ["channel_order_id", "channel_secondary_order_id", "channel_line_id"],
            "inner",
        )
        channel_keys = None
        for channel in KEY_CHANNELS:
            frame = spark.read.parquet(
                f"{args.ground_truth}/order_line_keys/{channel}/*.parquet"
            ).select(
                F.lit(channel).alias("channel"),
                "order_line_id",
                "channel_order_id",
                "channel_line_id",
            )
            channel_keys = frame if channel_keys is None else channel_keys.unionByName(frame)
        others = fact.filter(F.col("channel").isin(*KEY_CHANNELS)).join(
            channel_keys, ["channel", "channel_order_id", "channel_line_id"], "inner"
        )
        matched = walmart.unionByName(others)
        labels = spark.read.parquet(f"{args.ground_truth}/labels/*.parquet").select(
            "order_line_id", "label", "delivered_at", "label_available_at"
        )
        result = matched.join(labels, "order_line_id", "inner").select(
            "channel",
            "order_line_key",
            "order_line_id",
            "label",
            "delivered_at",
            "label_available_at",
            "created_at",
            F.lit(args.run_id).alias("run_id"),
        )
        target = f"{CATALOG}.gold.{args.table}"
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.gold")
        result.writeTo(target).partitionedBy(F.days("created_at")).createOrReplace()
        saved = spark.table(target)
        duplicate = (
            saved.groupBy("order_line_key").count().filter(F.col("count") > 1).limit(1).count()
        )
        invalid = (
            saved.filter(
                F.col("order_line_key").isNull()
                | F.col("label").isNull()
                | ~F.col("label").isin(0, 1)
                | F.col("delivered_at").isNull()
                | F.col("label_available_at").isNull()
                | (F.col("created_at") > F.col("delivered_at"))
                | (F.col("delivered_at") > F.col("label_available_at"))
                | (F.col("run_id") != args.run_id)
            )
            .limit(1)
            .count()
        )
        rows = spark.table(f"{target}.files").agg(F.sum("record_count")).first()[0]
        counts = {r["channel"]: r["count"] for r in saved.groupBy("channel").count().collect()}
        totals = {r["channel"]: r["count"] for r in fact.groupBy("channel").count().collect()}
        for channel in sorted(totals):
            labeled = counts.get(channel, 0)
            print(
                f"Labels {channel}: {labeled}/{totals[channel]} ({labeled / totals[channel]:.1%})"
            )
        missing = missing_channels(counts)
        print(f"Label bridge: rows={rows}, duplicate={duplicate}, invalid={invalid}")
        if not rows or duplicate or invalid or missing:
            raise ValueError(f"Invalid label bridge; Channels without labels: {missing}")
        print(f"Label bridge for five Channels written: {target}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
