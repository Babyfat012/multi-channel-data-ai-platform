"""Gold A (order snapshot) → training set and daily SKU features for ML, point-in-time correct.

A label is only "known" from `label_available_at`: the feature snapshot at 00:00 on day S only counts
labels with `label_available_at <= S`, and the training set only takes labels available before the cutoff.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

DATASET_VERSION = "order_snapshots"
# Same table as generator/core/storefronts.py CURRENCY_DECIMALS (Spark does not ship the generator).
CURRENCY_DECIMALS = {"USD": 2, "GBP": 2, "EUR": 2, "VND": 0}
WINDOW_DAYS = 90


def configure_output(spark, output: str) -> None:
    """Local MinIO needs endpoint + keys; AWS (PLATFORM_STORAGE=s3) uses the default IAM role."""
    if not output.startswith("s3a://") or os.environ.get("PLATFORM_STORAGE", "minio") == "s3":
        return
    hadoop = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop.set("fs.s3a.endpoint", os.environ["MINIO_INTERNAL_ENDPOINT"])
    hadoop.set("fs.s3a.path.style.access", "true")
    hadoop.set("fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    hadoop.set("fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])


def minor_units(amount, currency):
    """Decimal money → integer in the currency's minor units; unknown currencies give NULL, no guessing."""
    scale = F.create_map(*[F.lit(v) for kv in CURRENCY_DECIMALS.items() for v in kv])[currency]
    return F.round(amount * F.pow(F.lit(10), scale)).cast("bigint")


def first_snapshot_day(available_at):
    """First 00:00 day on which the label is known: round up to the day (exactly 00:00 is that day)."""
    day = F.to_date(available_at)
    return F.when(F.to_timestamp(day) == available_at, day).otherwise(F.date_add(day, 1))


def training_rows(fact: DataFrame, labels: DataFrame, cutoff, per_mille: int) -> DataFrame:
    """Lines with a label known before the cutoff + order-time attributes; keep `per_mille`/1000 by key hash."""
    sampled = fact.filter(F.pmod(F.xxhash64("order_line_key"), F.lit(1000)) < per_mille)
    known = labels.filter(F.col("label_available_at") <= F.lit(cutoff).cast("timestamp"))
    # The Gold fact also has a run_id column (the run that built the fact): rename the label run before joining to avoid ambiguity.
    label_run = known.select("order_line_key", "label", F.col("run_id").alias("label_run_id"))
    return sampled.join(label_run, "order_line_key").select(
        F.lit(DATASET_VERSION).alias("dataset_version"),
        F.col("order_line_key").alias("line_id"),
        "seller_sku",
        "created_at",
        F.col("created_at").alias("event_timestamp"),
        "channel",
        "country",
        "currency",
        F.col("quantity").alias("ordered_basis_qty"),
        minor_units(F.col("line_net_amount"), F.col("currency")).alias("ordered_product_net_minor"),
        "label",
        F.col("label_run_id").alias("generation_id"),
    )


def sku_daily_features(fact: DataFrame, labels: DataFrame, cutoff) -> DataFrame:
    """00:00 snapshot each day, from the SKU's first order day to the cutoff: labels known within 90 days."""
    cutoff_day = F.to_date(F.lit(cutoff).cast("timestamp"))
    days = (
        fact.filter(F.col("created_at").isNotNull())
        .groupBy("seller_sku")
        .agg(F.to_date(F.min("created_at")).alias("first_day"))
        .select("seller_sku", F.explode(F.sequence("first_day", cutoff_day)).alias("day"))
    )
    known = (
        labels.join(fact.select("order_line_key", "seller_sku"), "order_line_key")
        .groupBy("seller_sku", first_snapshot_day(F.col("label_available_at")).alias("day"))
        .agg(F.count("*").alias("n"), F.sum("label").alias("p"))
    )
    # rangeBetween in days: labels known from day S-89 to S ⇔ label_available_at in (S-90d, S].
    window = (
        Window.partitionBy("seller_sku")
        .orderBy(F.datediff("day", F.lit("1970-01-01")))
        .rangeBetween(-(WINDOW_DAYS - 1), 0)
    )
    # Days with labels but outside the grid (before the first order day/after the cutoff) are dropped by the left join.
    daily = days.join(known, ["seller_sku", "day"], "left").na.fill(0, ["n", "p"])
    labeled = F.sum("n").over(window)
    positive = F.sum("p").over(window)
    return daily.select(
        F.lit(DATASET_VERSION).alias("dataset_version"),
        "seller_sku",
        F.to_timestamp("day").alias("event_timestamp"),
        labeled.cast("bigint").alias("labeled_count_90d"),
        positive.cast("bigint").alias("positive_count_90d"),
        F.when(labeled > 0, positive / labeled).alias("return_or_refund_rate_90d"),
    )
