"""Check `spark_jobs/lib/order_snapshot_ml.py` with hand-built data, local[1], no catalog.

Must hold: (1) SKU snapshots match a pure-Python count: labels only count when
`label_available_at <= snapshot` and within 90 days; (2) the training set drops labels known after the cutoff, converts
money to the currency's minor units, and samples by a stable hash.

Run on EC2 (/opt/platform) or locally with the image:
docker run --rm --network none --read-only --tmpfs /tmp:rw,exec,size=1g \
  --mount type=bind,src=$PWD,dst=/workspace,readonly --entrypoint python \
  platform-airflow:2.10.5-spark3.5.3 /workspace/tests/spark/check_order_snapshot_ml.py
"""

import os
import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark_jobs.lib.order_snapshot_ml import sku_daily_features, training_rows

CUTOFF = "2026-04-15T00:00:00Z"


def ts(month, day, hour=0):
    """UTC timestamp in 2026."""
    return datetime(2026, month, day, hour, tzinfo=timezone.utc)


# (order_line_key, sku, created_at, channel, country, currency, quantity, line_net_amount)
FACT = [
    ("k1", "A", ts(1, 1, 9), "amazon", "US", "USD", 2, Decimal("10.50")),
    ("k2", "A", ts(1, 2, 15), "shopee", "VN", "VND", 1, Decimal("250000.00")),
    ("k3", "A", ts(1, 3, 8), "walmart", "US", "USD", 1, Decimal("3.00")),
    ("k4", "B", ts(1, 1, 10), "shopify", "GB", "GBP", 1, Decimal("7.25")),
    ("k5", "A", ts(3, 1), "tiktok_shop", "GB", "GBP", 1, Decimal("1.00")),
]
# (order_line_key, label, label_available_at, run_id)
LABELS = [
    ("k1", 1, ts(2, 1, 12), "r1"),  # known from the 2/2 snapshot
    ("k2", 0, ts(2, 3), "r1"),  # exactly 00:00 → in the 3/2 snapshot right away
    ("k3", 1, ts(4, 20), "r1"),  # after the cutoff → not in training, not in snapshots
    ("k4", 0, ts(2, 1, 1), "r1"),
    ("k5", 1, ts(4, 1, 6), "r1"),
]


def expected_daily(sku, day):
    """Pure-Python count: the SKU's labels with label_available_at in (day-90d, day]."""
    snap = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    keys = {f[0] for f in FACT if f[1] == sku}
    hits = [
        label
        for key, label, available, _ in LABELS
        if key in keys and snap - timedelta(days=90) < available <= snap
    ]
    return len(hits), sum(hits)


def main():
    """Run both checks; failures raise AssertionError."""
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("order-snapshot-ml-check")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        fact = spark.createDataFrame(
            FACT,
            "order_line_key string, seller_sku string, created_at timestamp, channel string, "
            "country string, currency string, quantity int, line_net_amount decimal(14,2)",
        ).withColumn(
            "run_id", F.lit("fact-run")
        )  # the real Gold has its own run_id: catches an ambiguous join
        labels = spark.createDataFrame(
            LABELS, "order_line_key string, label int, label_available_at timestamp, run_id string"
        )

        daily = {
            (r["seller_sku"], r["event_timestamp"].date()): r
            for r in sku_daily_features(fact, labels, CUTOFF).collect()
        }
        cutoff_day = date(2026, 4, 15)
        for sku, first in (("A", date(2026, 1, 1)), ("B", date(2026, 1, 1))):
            day = first
            while day <= cutoff_day:
                n, p = expected_daily(sku, day)
                row = daily.pop((sku, day))
                assert (row["labeled_count_90d"], row["positive_count_90d"]) == (n, p), (sku, day)
                rate = row["return_or_refund_rate_90d"]
                assert (rate is None) if n == 0 else abs(rate - p / n) < 1e-12, (sku, day)
                day += timedelta(days=1)
        assert not daily, f"extra snapshots outside the grid: {sorted(daily)[:3]}"
        a = {d: expected_daily("A", d) for d in (date(2026, 2, 1), date(2026, 2, 2))}
        assert a == {date(2026, 2, 1): (0, 0), date(2026, 2, 2): (1, 1)}, a

        rows = {r["line_id"]: r for r in training_rows(fact, labels, CUTOFF, 1000).collect()}
        assert set(rows) == {"k1", "k2", "k4", "k5"}, sorted(
            rows
        )  # k3 has a label after the cutoff
        assert rows["k1"]["ordered_product_net_minor"] == 1050
        assert rows["k2"]["ordered_product_net_minor"] == 250000  # VND has no minor unit
        assert rows["k1"]["event_timestamp"] == rows["k1"]["created_at"]
        assert rows["k1"]["generation_id"] == "r1" and rows["k5"]["country"] == "GB"
        half = [r["line_id"] for r in training_rows(fact, labels, CUTOFF, 500).collect()]
        assert half == [r["line_id"] for r in training_rows(fact, labels, CUTOFF, 500).collect()]
        assert set(half) <= set(rows)
        print(f"OK: {len(rows)} train rows; daily snapshots match the Python count")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
