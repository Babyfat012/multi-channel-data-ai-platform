"""Measure PIT history on real generated sources, using only pure Python.

Run with pytest -q -s on this file to reproduce the measured availability ranges.
Scale targets are real generated Orders; fixture retains an explicit later probe.
"""

import json
from collections import defaultdict
from datetime import timedelta

import pytest

from generator.generate_order_lifecycle import ScaleConfig, generate_scaled
from generator.generate_order_lifecycle_fixture import generate_fixture
from spark_jobs.lib.return_label import (
    FEED_KINDS,
    feature_window,
    label_segments,
    sku_return_feature,
    source_line,
    utc,
)

MICRO = timedelta(microseconds=1)


def _source_lines(root, manifest):
    """Validate full-feed receipts once, then adapt order-scoped evidence like Gold."""
    rows = [
        json.loads(raw)
        for item in manifest["files"]
        if item["feed"] in (*FEED_KINDS, "source.coverage_envelopes", "source.uncertainties")
        for raw in (root / item["path"]).read_text().splitlines()
    ]
    identities = []
    for row in rows:
        if row.get("feed") == "seller_oms.line_commitments" and row.get("entity_revision") == 1:
            payload = row["payload"]
            identities.append(
                dict(
                    dataset_version=row["dataset_version"],
                    seller_sku=payload["internal_sku_id"],
                    **{
                        key: payload[key]
                        for key in (
                            "channel",
                            "account_id",
                            "native_order_id",
                            "native_line_id",
                            "line_id",
                        )
                    },
                )
            )
    # The adapter verifies digest, count, sequence, interval and partition against
    # the full source feed. Never assume every declared 'complete' receipt is valid.
    checked = source_line(identities[0], rows)
    statuses = {
        r["source_event_id"]: r["receipt_status"]
        for r in checked["events"]
        if r["kind"] == "coverage"
    }
    by_order, keyless = defaultdict(list), []
    for row in rows:
        order_id = (
            row.get("payload", {}).get("native_order_id")
            or row.get("scope", {}).get("native_order_id")
            or row.get("affected_scope", {}).get("native_order_id")
        )
        if order_id:
            by_order[order_id].append(row)
        else:
            keyless.append(row)
    return [
        source_line(identity, by_order[identity["native_order_id"]] + keyless, statuses)
        for identity in identities
    ]


def _windows(lines):
    """Materialize actual label segments and their nonempty PIT windows once."""
    segments = [(line, segment) for line in lines for segment in label_segments(line)]
    windows = [
        (line, segment, window)
        for line, segment in segments
        if (window := feature_window(segment)) is not None
    ]
    return segments, windows


def _feature(target, windows):
    """Count same-SKU historical segments at target creation, excluding its identity."""
    values = [
        segment["label"]
        for line, segment, (begin, end) in windows
        if line["dataset_version"] == target["dataset_version"]
        and line["seller_sku"] == target["seller_sku"]
        and line["line_id"] != target["line_id"]
        and begin <= target["created_at"] < end
    ]
    return len(values), sum(values)


def test_scale_60_days_has_generated_targets_with_pit_history(tmp_path):
    """A real 60-day source has history known before the Order is placed, with no added probes."""
    root = tmp_path / "scale"
    manifest = generate_scaled(root, ScaleConfig(days=60, orders_per_day=4, sku_count=3))
    lines = _source_lines(root, manifest)
    segments, windows = _windows(lines)
    counts = [_feature(line, windows) for line in lines]
    positive = [line for line, count in zip(lines, counts) if count[0] > 0]
    assert positive
    assert len({line["line_id"] for line in positive}) == len(positive)
    # Compare the sweep against the as-of policy on real targets, including before/after a label exists.
    for target in (lines[0], positive[0], positive[-1]):
        brute = sku_return_feature(target, lines)
        assert _feature(target, windows) == (
            brute["labeled_count_90d"],
            brute["positive_count_90d"],
        )
    coverage_counts = [sum(e["kind"] == "coverage" for e in line["events"]) for line in lines]
    # At most 7 weeks intersect the 20-day split + 30-day qualification window, times 5 feeds.
    assert max(coverage_counts) <= 7 * 5
    print(
        "scale60:",
        len(lines),
        "lines;",
        len(segments),
        "segments;",
        len(windows),
        "windows; generated targets labeled_count_90d > 0 =",
        len(positive),
        "; max per-line coverage =",
        max(coverage_counts),
    )


@pytest.mark.parametrize("source", ["minimal-scale", "fixture"])
def test_later_probe_has_pit_history_only_after_coverage(tmp_path, source):
    """Short tail retains cohorts; real policy agrees at exclusive coverage/90-day edges."""
    root = tmp_path / source
    if source == "minimal-scale":
        config = ScaleConfig(
            days=1,
            orders_per_day=1,
            sku_count=2,
            lines_per_order=(1.0, 0.0, 0.0, 0.0),
            followup_days=51,
            observation_days=60,
        )
        manifest = generate_scaled(root, config)
        sku, expected, opened = "sku-00001", (1, 0), utc("2026-02-06T00:00:00Z")
    else:
        manifest = generate_fixture(root)
        sku, expected, opened = "sku-A", (15, 15), utc("2026-04-02T00:00:00Z")
    lines = _source_lines(root, manifest)
    segments, windows = _windows(lines)
    assert {s["start_exclusive"] for _, s in segments} == {opened}
    assert all(
        s["start_exclusive"] == s["decision_observed_at"] > s["maturity_end"] for _, s in segments
    )
    assert max(line["created_at"] for line in lines) < opened
    assert all(_feature(line, windows) == (0, 0) for line in lines)
    target = dict(
        dataset_version=manifest["dataset_version"], line_id="later-probe", seller_sku=sku
    )
    # Boundary, +1µs, the Spark harness's later time and final cohort expiry.
    expiry = max(end for line, _, (_, end) in windows if line["seller_sku"] == sku)
    for timestamp, counts in (
        (opened, (0, 0)),
        (opened + MICRO, expected),
        (opened + timedelta(days=1), expected),
        (expiry, (0, 0)),
    ):
        target["created_at"] = timestamp
        assert _feature(target, windows) == counts
        brute = sku_return_feature(target, lines)
        assert (brute["labeled_count_90d"], brute["positive_count_90d"]) == counts
        if counts[0]:
            assert brute["return_or_refund_rate_90d"] == counts[1] / counts[0]
    print(
        source,
        ":",
        len(lines),
        "lines;",
        len(segments),
        "segments; start",
        opened,
        "; later probe count/positive",
        expected,
    )
