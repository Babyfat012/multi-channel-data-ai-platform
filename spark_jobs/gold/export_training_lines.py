"""Gold A → Parquet training set: labeled Order Lines (known before the cutoff) + attributes at Order time.

This is the "entity dataframe" for Feast: KFP adds SKU features with a PIT join at `event_timestamp`
(= Order time). Labels come from `fact_return_labels_dp3` (five Channels); only labels with
`label_available_at <= cutoff` are kept. No columns known after the Order is placed, to avoid leakage. Samples
`--sample-per-mille` by `order_line_key` hash: reruns give the same set, spread evenly over time.
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import functions as F

from spark_jobs.lib.iceberg_session import CATALOG, build_spark
from spark_jobs.lib.order_snapshot_ml import configure_output, training_rows


def main(argv=None):
    """Join labels with the fact, overwrite Parquet and print the label distribution."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--output", required=True, help="vd s3a://platform/feature-store/training_lines"
    )
    parser.add_argument("--cutoff", required=True)
    parser.add_argument("--fact-table", default="fact_order_lines_dp2")
    parser.add_argument("--labels-table", default="fact_return_labels_dp3")
    # 10‰ = 1%: five Channels ~ hundreds of millions of labeled lines, the training pod on EKS has only ~8 GB RAM.
    parser.add_argument("--sample-per-mille", type=int, default=10)
    args = parser.parse_args(argv)
    if not 0 < args.sample_per_mille <= 1000:
        parser.error("--sample-per-mille must be in (0, 1000]")
    spark = build_spark("export-training-lines", shuffle_partitions=64)
    try:
        configure_output(spark, args.output)
        labels = spark.table(f"{CATALOG}.gold.{args.labels_table}")
        runs = labels.select("run_id").distinct().collect()
        if len(runs) != 1:
            raise ValueError(f"expected exactly 1 run_id in the label table, got {runs}")
        fact = spark.table(f"{CATALOG}.gold.{args.fact_table}")
        rows = training_rows(fact, labels, args.cutoff, args.sample_per_mille)
        rows.coalesce(1).write.mode("overwrite").parquet(args.output)
        written = spark.read.parquet(args.output)
        counts = {
            (r["channel"], r["label"]): r["count"]
            for r in written.groupBy("channel", "label").count().collect()
        }
        bad = written.filter(F.col("ordered_product_net_minor").isNull()).limit(1).count()
        print(f"wrote {written.count()} rows to {args.output}; label counts {counts}")
        if not counts or bad:
            raise ValueError(
                f"Training set is empty or has NULL money (unknown currency): bad={bad}"
            )
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
