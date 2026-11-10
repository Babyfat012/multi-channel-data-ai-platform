"""Check `sku_daily_features` with hand-built data, local[1], no catalog.

Two things must hold: (1) it matches a pure-Python count over the same window; (2) a line placed exactly at 00:00
gets the same value as that day's snapshot, i.e. training and serving share one definition.

docker run --rm --network none --read-only --tmpfs /tmp:rw,exec,size=1g \
  --mount type=bind,src=$PWD,dst=/workspace,readonly --entrypoint python \
  platform-airflow:2.10.5-spark3.5.3 /workspace/tests/spark/check_sku_daily_features.py
"""

import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import SparkSession

from spark_jobs.lib.order_lifecycle_gold import (
    SEGMENT_DDL,
    sku_daily_features,
    sku_features,
)

D = "ds1"


def ts(day, hour=0):
    """UTC timestamp in January 2026."""
    return datetime(2026, 1, day, hour, tzinfo=timezone.utc)


# (line_id, sku, created_at)
FACT = [
    ("l1", "A", ts(1, 9)),
    ("l2", "A", ts(2, 15)),
    ("l3", "A", ts(3, 8)),
    ("l4", "B", ts(1, 10)),
    ("l5", "A", ts(20)),  # placed exactly at 00:00 → must equal the day-20 snapshot
]
# (line_id, start_exclusive, end_inclusive, label, cohort_date)
SEGMENTS = [
    ("l1", ts(4), None, 1, date(2026, 1, 3)),
    ("l2", ts(5), ts(12, 6), 0, date(2026, 1, 4)),  # segment closes midway
    ("l3", ts(6), None, 0, date(2026, 1, 5)),
    ("l4", ts(6), None, 1, date(2026, 1, 5)),
]
CUTOFF = ts(25)


def oracle(sku, at):
    """Count by hand: segments open in [a, b) with a, b as in `return_label.feature_window`."""
    micro = timedelta(microseconds=1)
    skus = {line: s for line, s, _ in FACT}
    n = p = 0
    for line, start, end, label, cohort in SEGMENTS:
        if skus[line] != sku:
            continue
        a = max(
            start + micro,
            datetime.combine(cohort + timedelta(1), datetime.min.time(), timezone.utc),
        )
        b = datetime.combine(cohort + timedelta(91), datetime.min.time(), timezone.utc)
        if end is not None:
            b = min(b, end + micro)
        if a <= at < b:
            n, p = n + 1, p + label
    return n, p


def main():
    """Run both checks; a failed assert exits non-zero."""
    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    fact = spark.createDataFrame(
        [(D, line, sku, at) for line, sku, at in FACT],
        "dataset_version string, line_id string, seller_sku string, created_at timestamp",
    )
    segments = spark.createDataFrame(
        [(D, line, s, e, lab, c, None, None, None) for line, s, e, lab, c in SEGMENTS],
        SEGMENT_DDL,
    )
    daily = sku_daily_features(fact, segments, CUTOFF).collect()
    assert daily, "no rows"
    for row in daily:
        at = row["event_timestamp"].replace(tzinfo=timezone.utc)
        got = (row["labeled_count_90d"], row["positive_count_90d"])
        assert got == oracle(row["seller_sku"], at), (row, oracle(row["seller_sku"], at))
    days_a = sorted(r["event_timestamp"] for r in daily if r["seller_sku"] == "A")
    assert len(days_a) == 25 and days_a[0].day == 1 and days_a[-1].day == 25, days_a
    snap = {(r["seller_sku"], r["event_timestamp"].day): r for r in daily}[("A", 20)]
    line = [r for r in sku_features(fact, segments).collect() if r["line_id"] == "l5"][0]
    for col in ("labeled_count_90d", "positive_count_90d", "return_or_refund_rate_90d"):
        assert snap[col] == line[col], (col, snap[col], line[col])
    print(f"OK: {len(daily)} rows match the oracle; day-20 snapshot = feature of line l5 ({line})")
    spark.stop()


if __name__ == "__main__":
    main()
