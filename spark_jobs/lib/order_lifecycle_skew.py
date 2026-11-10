"""SKU sweep split into UTC buckets; kept separate, does not touch the Gold job in use."""

from __future__ import annotations

from datetime import datetime, timezone


def bucket_width_days(buckets: int) -> int:
    """Split the 180-day horizon into roughly `buckets` fixed ranges, at least one day each."""
    if type(buckets) is not int or not 1 <= buckets <= 180:
        raise ValueError("buckets must be an integer in 1..180")
    return (180 + buckets - 1) // buckets


def time_bucket(timestamp: datetime, buckets: int) -> int:
    """Range id counted from the UTC epoch; independent of the host time zone."""
    if timestamp.tzinfo is None:
        raise ValueError("timestamp must have a timezone")
    days = (timestamp.astimezone(timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)).days
    return days // bucket_width_days(buckets)


def bucket_prefix(totals: dict[int, tuple[int, int]]) -> dict[int, tuple[int, int]]:
    """Python oracle: dc/dp totals of earlier buckets, including empty/query-only buckets."""
    n = p = 0
    result = {}
    for bucket in sorted(totals):
        result[bucket] = (n, p)
        dc, dp = totals[bucket]
        n, p = n + dc, p + dp
    return result


def sku_features_salted(fact, segments, buckets: int):
    """Equivalent to sku_features: bucket prefix + local sweep, then subtract the line's window.

    Splits every SKU by time (no need to collect the list of hot SKUs). Each bucket
    has both changes and queries; the prefix only gets one total row per bucket. Deltas at the same
    timestamp always come before queries, preserving the half-open window [a,b) and the microsecond edge.
    buckets is the target number of ranges over 180 days, not the number of hash partitions.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    from spark_jobs.lib.order_lifecycle_gold import KEY, _feature_windows

    width = bucket_width_days(buckets)
    fact.sparkSession.conf.set("spark.sql.session.timeZone", "UTC")
    windows = _feature_windows(segments, fact.select(*KEY, "seller_sku"))
    group = ["dataset_version", "seller_sku"]
    changes = (
        windows.select(
            *group,
            F.explode(
                F.array(
                    F.struct(
                        F.col("a").alias("time"), F.lit(1).alias("dc"), F.col("label").alias("dp")
                    ),
                    F.struct(
                        F.col("b").alias("time"),
                        F.lit(-1).alias("dc"),
                        (-F.col("label")).alias("dp"),
                    ),
                )
            ).alias("change"),
        )
        .select(*group, "change.*")
        .withColumn("kind", F.lit(0))
        .withColumn("line_id", F.lit(None).cast("string"))
    )
    queries = fact.filter(F.col("created_at").isNotNull()).select(
        *group,
        F.col("created_at").alias("time"),
        F.lit(0).alias("dc"),
        F.lit(0).alias("dp"),
        F.lit(1).alias("kind"),
        "line_id",
    )
    events = changes.unionByName(queries).withColumn(
        "_bucket",
        F.floor(F.datediff(F.to_date("time"), F.lit("1970-01-01")) / F.lit(width)).cast("long"),
    )
    totals = events.groupBy(*group, "_bucket").agg(
        F.sum("dc").alias("_dc"), F.sum("dp").alias("_dp")
    )
    prefix_window = (
        Window.partitionBy(*group).orderBy("_bucket").rowsBetween(Window.unboundedPreceding, -1)
    )
    prefix = totals.select(
        *group,
        "_bucket",
        F.coalesce(F.sum("_dc").over(prefix_window), F.lit(0)).alias("_n0"),
        F.coalesce(F.sum("_dp").over(prefix_window), F.lit(0)).alias("_p0"),
    )
    local = (
        Window.partitionBy(*group, "_bucket")
        .orderBy("time", "kind")
        .rowsBetween(Window.unboundedPreceding, 0)
    )
    # Compute the window before the join so Spark is forced to split the sweep by SKU + bucket.
    swept = (
        events.withColumn("_n", F.sum("dc").over(local))
        .withColumn("_p", F.sum("dp").over(local))
        .filter(F.col("kind") == 1)
        .alias("events")
    )
    joined = swept.join(
        prefix.alias("prefix"),
        F.col("events.dataset_version").eqNullSafe(F.col("prefix.dataset_version"))
        & F.col("events.seller_sku").eqNullSafe(F.col("prefix.seller_sku"))
        & F.col("events._bucket").eqNullSafe(F.col("prefix._bucket")),
    )
    swept = joined.select(
        "events.dataset_version",
        "events.line_id",
        (F.col("_n") + F.col("_n0")).alias("n"),
        (F.col("_p") + F.col("_p0")).alias("p"),
    )
    own = (
        windows.join(fact.select(*KEY, "created_at"), KEY)
        .filter((F.col("a") <= F.col("created_at")) & (F.col("created_at") < F.col("b")))
        .groupBy(*KEY)
        .agg(F.count("*").alias("own_n"), F.sum("label").alias("own_p"))
    )
    counts = (
        swept.join(own, KEY, "left")
        .fillna(0, subset=["own_n", "own_p"])
        .select(
            *KEY,
            (F.col("n") - F.col("own_n")).cast("bigint").alias("labeled_count_90d"),
            (F.col("p") - F.col("own_p")).cast("bigint").alias("positive_count_90d"),
        )
    )
    return (
        fact.select(*KEY, "seller_sku", "created_at")
        .join(counts, KEY, "left")
        .fillna(0, subset=["labeled_count_90d", "positive_count_90d"])
        .withColumn(
            "return_or_refund_rate_90d",
            F.when(
                F.col("labeled_count_90d") > 0,
                F.col("positive_count_90d") / F.col("labeled_count_90d"),
            ),
        )
    )
