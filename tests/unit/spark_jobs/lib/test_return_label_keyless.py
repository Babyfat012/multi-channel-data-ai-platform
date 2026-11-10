"""Check labels through the pure Python API when the coverage-verification-only field is dropped."""

import json

import pytest

from generator.generate_order_lifecycle_fixture import generate_fixture
from spark_jobs.lib.return_label import label_line, source_line


@pytest.fixture(scope="module")
def source_records(tmp_path_factory):
    """Read only the source from the fixture; truth/probe are never used as expected values."""
    root = tmp_path_factory.mktemp("keyless") / "source"
    manifest = generate_fixture(root)
    return [
        json.loads(line)
        for item in manifest["files"]
        if item["role"] == "source"
        for line in (root / item["path"]).read_text().splitlines()
    ]


@pytest.mark.parametrize("status", ["complete", "partial", None])
@pytest.mark.parametrize("cutoff", ["2026-04-05T00:00:00Z", "2026-06-01T00:00:00Z"])
def test_sequence_ranges_do_not_change_labels_with_silver_status(source_records, status, cutoff):
    """Compare the whole LabelResult; an empty map must also use the Silver status branch."""
    coverage = [row for row in source_records if row.get("coverage_id")]
    assert coverage and any(row.get("source_sequence_ranges") for row in coverage)
    statuses = (
        {f"{row['coverage_id']}:{row['revision']}": status for row in coverage} if status else {}
    )
    trimmed = [
        {k: v for k, v in row.items() if k != "source_sequence_ranges"}
        if row.get("coverage_id")
        else row
        for row in source_records
    ]
    identities = {
        row["payload"]["line_id"]: {
            "dataset_version": row["dataset_version"],
            **{
                key: row["payload"][key]
                for key in ("line_id", "channel", "account_id", "native_order_id", "native_line_id")
            },
        }
        for row in source_records
        if row.get("feed") == "seller_oms.line_commitments" and row.get("payload")
    }
    assert identities
    outcomes = set()
    for identity in identities.values():
        full = source_line(identity, source_records, coverage_status=statuses)
        slim = source_line(identity, trimmed, coverage_status=statuses)
        # Both the JSON passed to workers and the dict go through the same return_label API.
        from_json = source_line(
            identity, [json.dumps(row) for row in trimmed], coverage_status=statuses
        )
        result = label_line(full, cutoff)
        assert label_line(slim, cutoff) == result
        assert label_line(from_json, cutoff) == result
        outcomes.add((result.state, result.label))
    if status == "complete" and cutoff == "2026-06-01T00:00:00Z":
        assert {("labeled", 0), ("labeled", 1)} <= outcomes
    if status != "complete":
        assert all(state != "labeled" for state, _ in outcomes)


def test_weekly_tail_does_not_grow_line_input_or_borrow_authority(source_records):
    """Adding 100 weeks outside the window does not change the input; an unknown producer still has no authority."""
    from copy import deepcopy
    from datetime import timedelta

    from spark_jobs.lib.return_label import utc

    raw = deepcopy(source_records)
    commitment = next(
        r for r in raw if r.get("feed") == "seller_oms.line_commitments" and r.get("payload")
    )
    identity = {
        "dataset_version": commitment["dataset_version"],
        **{
            k: commitment["payload"][k]
            for k in ("line_id", "channel", "account_id", "native_order_id", "native_line_id")
        },
    }
    statuses = {
        f"{r['coverage_id']}:{r['revision']}": "complete" for r in raw if r.get("coverage_id")
    }
    before = source_line(identity, raw, statuses)
    envelope = next(r for r in raw if r.get("coverage_id"))
    for week in range(100):
        begin = utc("2027-01-01T00:00:00Z") + timedelta(weeks=week)
        raw.append(
            {
                **envelope,
                "coverage_id": f"future-{week}",
                "event_interval": {
                    "start_inclusive": begin.isoformat(),
                    "end_exclusive": (begin + timedelta(weeks=1)).isoformat(),
                },
            }
        )
        statuses[f"future-{week}:1"] = "complete"
    assert source_line(identity, raw, statuses) == before
    # Drop the authority of exactly that producer, keeping another producer's envelope with the same feed.
    foreign = [{**r, "producer": "foreign"} if r.get("coverage_id") else r for r in raw]
    line = source_line(identity, foreign, statuses)
    assert any(r.get("authority_valid") is False for r in line["events"] if r["kind"] == "delivery")
    assert label_line(line, "2026-06-01T00:00:00Z").state == "unknown"


def test_coverage_revision_moving_outside_window_does_not_revive_old_claim(source_records):
    """A revision narrowing the interval must invalidate the old claim even if the new interval does not intersect the line."""
    from copy import deepcopy

    records = deepcopy(source_records)
    commitment = next(
        r for r in records if r.get("feed") == "seller_oms.line_commitments" and r.get("payload")
    )
    identity = {
        "dataset_version": commitment["dataset_version"],
        **{
            k: commitment["payload"][k]
            for k in ("line_id", "channel", "account_id", "native_order_id", "native_line_id")
        },
    }
    old = next(
        r for r in records if r.get("coverage_id") and r["feed"] == "carrier.delivery_events"
    )
    records.append(
        {
            **old,
            "revision": 2,
            "source_observed_at": "2026-05-01T00:00:00Z",
            "event_interval": {
                "start_inclusive": "2027-01-01T00:00:00Z",
                "end_exclusive": "2027-01-08T00:00:00Z",
            },
        }
    )
    statuses = {
        f"{r['coverage_id']}:{r['revision']}": "complete" for r in records if r.get("coverage_id")
    }
    data = source_line(identity, records, statuses)
    assert label_line(data, "2026-04-30T00:00:00Z").state == "labeled"
    assert label_line(data, "2026-06-01T00:00:00Z").state == "unknown"
