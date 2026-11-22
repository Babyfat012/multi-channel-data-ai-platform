"""Check ml/drift.summarize: reads p-value (small = drift) and distance (large = drift) in the right direction."""

import pandas as pd
import pytest

from ml.drift import comparisons, decide, empty_columns, summarize, unseen


def snapshot(*metrics):
    """Minimal shape of Snapshot.dict() that summarize reads."""
    return {"metrics": [{"metric_name": n, "value": v} for n, v in metrics]}


@pytest.mark.parametrize(
    ("method", "threshold", "score", "drift"),
    [
        ("chi-square p_value", 0.05, 0.049, True),
        ("chi-square p_value", 0.05, 0.05, False),
        ("Jensen-Shannon distance", 0.2, 0.2, True),
        ("Jensen-Shannon distance", 0.2, 0.199, False),
    ],
)
def test_direction_and_boundary(method, threshold, score, drift):
    """Threshold boundary for each method type."""
    out = summarize(
        snapshot((f"ValueDrift(column=x,method={method},threshold={threshold})", score))
    )
    assert out["columns"]["x"]["drift"] is drift and out["alert"] is drift


def test_alert_when_any_column_drifts_and_ignores_other_metrics():
    """One drifted column is enough to alert; metrics other than ValueDrift are ignored."""
    out = summarize(
        snapshot(
            ("DriftedColumnsCount(drift_share=0.5)", {"count": 1.0, "share": 0.5}),
            ("ValueDrift(column=seller_sku,method=Jensen-Shannon distance,threshold=0.2)", 0.436),
            ("ValueDrift(column=label,method=Jensen-Shannon distance,threshold=0.1)", 0.001),
        )
    )
    assert out["drifted"] == ["seller_sku"] and out["alert"] is True


def test_comparisons_split_by_reference_window_and_channel():
    """The first N days are the reference; channel_shift keeps only the later part of one Channel; an absent Channel ⇒ clear error."""
    frame = pd.DataFrame(
        {
            "event_timestamp": pd.to_datetime(
                ["2026-01-01", "2026-01-05", "2026-02-10", "2026-02-11", "2026-02-12"], utc=True
            ),
            "channel": ["amazon", "shopee", "amazon", "shopee", "shopee"],
            "seller_sku": ["A", "B", "A", "B", "B"],
            "label": [0, 1, 0, 1, 0],
        }
    )
    pairs = comparisons(frame, reference_days=30, shift_channel="shopee")
    assert [len(x) for x in pairs["stable"]] == [2, 3]
    ref, cur = pairs["channel_shift"]
    assert len(ref) == 2 and set(cur["channel"]) == {"shopee"} and len(cur) == 2
    assert "channel_shift" not in comparisons(frame, 30)
    with pytest.raises(ValueError, match="walmart"):
        comparisons(frame, 30, shift_channel="walmart")


def lines(*days):
    """Lines with only an Order timestamp per day given."""
    return pd.DataFrame({"event_timestamp": pd.to_datetime(list(days), utc=True)})


def test_unseen_keeps_only_lines_after_everything_the_model_saw():
    """Cutoff = the latest of train AND test: the test window was already used to judge the model."""
    frame = lines("2026-01-05", "2026-01-20", "2026-01-21", "2026-02-01")
    current = unseen(frame, lines("2026-01-01", "2026-01-10"), lines("2026-01-11", "2026-01-20"))
    assert current["event_timestamp"].dt.day.tolist() == [21, 1]
    assert unseen(frame, lines("2026-02-01")).empty


@pytest.mark.parametrize(
    ("alert", "rows", "pending", "decision", "reason"),
    [
        (True, 999, None, "skip", "999 new labeled lines < 1000"),  # rows checked before drift
        (False, 1000, None, "skip", "no drift"),
        (True, 1000, "7", "skip", "challenger v7"),  # no daily retrain while waiting for approval
        (True, 1000, None, "train", "drift"),
    ],
)
def test_decide(alert, rows, pending, decision, reason):
    """Train only on drift, with enough new lines, and no challenger waiting for approval."""
    got = decide(alert, rows, 1000, pending)
    assert got[0] == decision and reason in got[1]


def test_empty_columns_on_either_side():
    """A column with no value on one side cannot be scored and is reported, not silently dropped."""
    ref = pd.DataFrame({"a": [1.0, None], "b": [None, None], "c": [1.0, 2.0]})
    cur = pd.DataFrame({"a": [None, None], "b": [None, None], "c": [None, 3.0]})
    assert empty_columns(ref, cur, ["a", "b", "c"]) == ["a", "b"]
