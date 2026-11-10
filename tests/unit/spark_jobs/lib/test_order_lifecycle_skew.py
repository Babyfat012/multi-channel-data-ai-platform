"""Check bucket/prefix without a JVM or pyspark; data and expected answers built by hand."""

import importlib.util
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from spark_jobs.lib.order_lifecycle_skew import bucket_prefix, bucket_width_days, time_bucket

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def test_bucket_utc_and_boundaries():
    """10-day buckets, microsecond edges and timestamps in other time zones still land in the same range."""
    edge = EPOCH + timedelta(days=10)
    assert bucket_width_days(18) == 10
    assert time_bucket(edge - timedelta(microseconds=1), 18) == 0
    assert time_bucket(edge, 18) == 1
    assert time_bucket(edge.astimezone(timezone(timedelta(hours=7))), 18) == 1
    assert time_bucket(EPOCH - timedelta(microseconds=1), 18) == -1
    assert bucket_width_days(7) == 26


@pytest.mark.parametrize("value", [0, -1, 181, True, 1.5])
def test_invalid_bucket_count(value):
    """Never silently change the bucket count or accept bool as int."""
    with pytest.raises(ValueError):
        bucket_width_days(value)


def test_naive_timestamp_rejected():
    """The oracle must not depend on the host time zone."""
    with pytest.raises(ValueError):
        time_bucket(datetime(2026, 1, 1), 18)


def test_sparse_prefix_with_query_only_bucket():
    """Query-only buckets must get the prefix; a negative delta closes the window across an empty bucket."""
    assert bucket_prefix({8: (-2, -1), 1: (2, 1), 5: (0, 0), 10: (0, 0)}) == {
        1: (0, 0),
        5: (2, 1),
        8: (2, 1),
        10: (0, 0),
    }
    assert bucket_prefix({}) == {}


def test_local_sweep_plus_prefix_equals_direct_half_open_oracle():
    """Label changes, self-subtraction, equal timestamps, end-exclusive and many buckets with no change."""
    # (line, begin day, end day, label), with two consecutive segments for the same line.
    windows = [("a", 2, 30, 1), ("a", 30, 61, 0), ("b", 10, 40, 1), ("c", 20, 21, 0)]
    queries = [
        ("a", 10),
        ("x", 20),
        ("x", 21),
        ("x", 30),
        ("b", 40),
        ("x", 55),
        ("x", 61),
        ("x", 90),
    ]
    events = []
    for _line, begin, end, label in windows:
        events += [(begin, 0, 1, label, None), (end, 0, -1, -label, None)]
    events += [(day, 1, 0, 0, (line, day)) for line, day in queries]
    grouped = defaultdict(list)
    for event in events:
        grouped[time_bucket(EPOCH + timedelta(days=event[0]), 18)].append(event)
    totals = {
        bucket: (sum(e[2] for e in rows), sum(e[3] for e in rows))
        for bucket, rows in grouped.items()
    }
    prefix = bucket_prefix(totals)
    actual = {}
    for bucket, rows in grouped.items():
        n, p = prefix[bucket]
        for day, kind, dc, dp, query in sorted(rows, key=lambda e: e[:2]):
            n, p = n + dc, p + dp
            if kind:
                own = [label for line, a, b, label in windows if line == query[0] and a <= day < b]
                actual[query] = (n - len(own), p - sum(own))
    expected = {
        (line, day): (len(labels), sum(labels))
        for line, day in queries
        for labels in [[label for other, a, b, label in windows if other != line and a <= day < b]]
    }
    assert actual == expected
    assert actual[("x", 30)] == (2, 1)
    assert actual[("x", 61)] == (0, 0)


def test_event_log_metrics_and_sweep_attribution(tmp_path):
    """Hand-built event log: correct shuffle bytes, failed tasks dropped and no invented ratio when median=0."""
    import json

    spec = importlib.util.spec_from_file_location(
        "measure_v2_skew",
        Path(__file__).parents[4] / "spark_jobs/measure/measure_order_lifecycle_skew.py",
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    events = [
        {
            "Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
            "executionId": 1,
            "physicalPlanDescription": "Window",
            "sparkPlanInfo": {
                "nodeName": "Window",
                "children": [
                    {
                        "nodeName": "Sort",
                        "children": [
                            {
                                "nodeName": "Exchange",
                                "simpleString": "Exchange hashpartitioning(dataset_version#1, seller_sku#2, _bucket#3, 16)",
                                "metrics": [{"accumulatorId": 42}],
                                "children": [],
                            }
                        ],
                    }
                ],
            },
        },
        {
            "Event": "SparkListenerJobStart",
            "Stage IDs": [0, 1],
            "Properties": {"spark.jobGroup.id": "salted"},
        },
        {
            "Event": "SparkListenerStageCompleted",
            "Stage Info": {
                "Stage ID": 0,
                "Stage Attempt ID": 0,
                "Stage Name": "producer",
                "Number of Tasks": 1,
                "Parent IDs": [],
                "Accumulables": [{"ID": 42}],
            },
        },
        {
            "Event": "SparkListenerStageCompleted",
            "Stage Info": {
                "Stage ID": 1,
                "Stage Attempt ID": 0,
                "Stage Name": "consumer",
                "Number of Tasks": 1,
                "Parent IDs": [0],
                "Accumulables": [],
            },
        },
        {
            "Event": "SparkListenerTaskEnd",
            "Stage ID": 1,
            "Stage Attempt ID": 0,
            "Task End Reason": {"Reason": "Success"},
            "Task Info": {"Index": 0, "Attempt": 0, "Launch Time": 10, "Finish Time": 20},
            "Task Metrics": {
                "Shuffle Read Metrics": {
                    "Total Records Read": 100,
                    "Remote Bytes Read": 8,
                    "Local Bytes Read": 12,
                }
            },
        },
    ]
    (tmp_path / "app").write_text("\n".join(json.dumps(e) for e in events))
    stage = module.read_event_logs(tmp_path)["stages"][1]
    assert stage["sweep_exchange_inputs"] == ["bucket_sweep"]
    assert stage["records"]["max"] == 100
    assert stage["shuffle_read_bytes"]["max"] == 20
    assert stage["duration_ms"]["max"] == 10
    assert module.distribution([0, 0, 9])["max_median"] is None


def test_exchange_detection_excludes_aggregate_and_reads_adaptive_plan(tmp_path):
    """A same-key aggregate is not a sweep; an updated AQE plan still locates the Window."""
    import json

    spec = importlib.util.spec_from_file_location(
        "measure_v2_skew",
        Path(__file__).parents[4] / "spark_jobs/measure/measure_order_lifecycle_skew.py",
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    exchange = {
        "nodeName": "Exchange",
        "simpleString": "Exchange hashpartitioning(dataset_version#1, seller_sku#2, _bucket#3L, 16)",
        "metrics": [{"accumulatorId": 9}],
        "children": [],
    }
    aggregate = {"nodeName": "HashAggregate", "children": [exchange]}
    assert list(module._window_exchanges(aggregate)) == []
    assert list(module._window_exchanges({"nodeName": "Window", "children": [aggregate]})) == []
    event = {
        "Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate",
        "executionId": 2,
        "physicalPlanDescription": "updated Window",
        "sparkPlanInfo": {
            "nodeName": "Window",
            "children": [{"nodeName": "Sort", "children": [exchange]}],
        },
    }
    (tmp_path / "app").write_text(json.dumps(event) + "\n")
    assert module.read_event_logs(tmp_path)["sql_plans"][0]["plan"] == "updated Window"
    assert list(module._window_exchanges(event["sparkPlanInfo"])) == [exchange]
