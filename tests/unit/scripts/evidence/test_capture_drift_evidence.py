"""The parts of capture_drift_evidence that need no Chrome/Evidently: simulated data and the summary table."""

from ml.drift import comparisons
from scripts.evidence.capture_drift_evidence import (
    data_page,
    summary_page,
    synthetic_lines,
    window_rows,
)


def test_synthetic_lines_shift_only_shopee_after_reference_window():
    """After day 30 only shopee shifts to the second half of the SKU catalog; other Channels keep their distribution."""
    pairs = comparisons(synthetic_lines(per_day=200), 30, "shopee")
    reference, shifted = pairs["channel_shift"]
    ids = lambda f: f["seller_sku"].str[4:].astype(int)  # noqa: E731
    assert ids(shifted).min() >= 50 and ids(reference).min() == 0
    assert set(shifted["channel"]) == {"shopee"} and len(pairs["stable"][1]) > len(shifted)


def test_summary_page_lists_each_column_with_alert():
    """One row per (comparison, column); a column over threshold shows DRIFT, a comparison with an alert shows YES."""
    summary = {
        "channel_shift": {
            "reference_rows": 10,
            "current_rows": 5,
            "alert": True,
            "columns": {
                "seller_sku": {"score": 0.65, "threshold": 0.2, "drift": True},
                "label": {"score": 0.01, "threshold": 0.1, "drift": False},
            },
        }
    }
    page = summary_page(summary)
    assert page.count("<tr><td>channel_shift") == 2
    assert "DRIFT" in page and "<td>stable</td>" in page and "<td>YES</td>" in page


def test_window_rows_describe_both_windows_of_each_pair():
    """Step 1: rows, days and Channels of the reference/current windows match `comparisons`."""
    pairs = comparisons(synthetic_lines(per_day=100), 30, "shopee")
    rows = {row["pair"]: row for row in window_rows(pairs)}
    for name, (reference, current) in pairs.items():
        assert rows[name]["reference"]["rows"] == len(reference)
        assert rows[name]["current"]["rows"] == len(current)
    assert rows["channel_shift"]["current"]["channels"] == {
        "shopee": rows["channel_shift"]["current"]["rows"]
    }
    assert rows["stable"]["reference"]["to"] < rows["stable"]["current"]["from"]
    page = data_page(list(rows.values()), "<source & test>")
    assert (
        page.count("<td>reference</td>") == 2
        and page.count("<td>current</td>") == 2
        and "&lt;source &amp; test&gt;" in page
    )
