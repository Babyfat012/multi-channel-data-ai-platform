"""Check ml/train_lib.py: time-based split, baseline, evaluation; real LightGBM training when available."""

import numpy as np
import pandas as pd
import pytest

from ml.train_lib import add_order_features, baseline_scores, evaluate, temporal_split


def frame(days=20, per_day=10):
    """Hand-built data: label 1 for SKU-A, so the model has a clear signal."""
    rows = []
    for d in range(days):
        for i in range(per_day):
            sku = "A" if i % 3 == 0 else "B"
            rows.append(
                dict(
                    event_timestamp=pd.Timestamp("2026-01-01", tz="UTC")
                    + pd.Timedelta(days=d, hours=i),
                    seller_sku=sku,
                    channel="amazon" if i % 2 else "walmart",
                    country="US",
                    ordered_basis_qty=1 + i % 2,
                    ordered_product_net_minor=1000 * (1 + i % 2),
                    labeled_count_90d=None if d == 0 else 10,
                    return_or_refund_rate_90d=0.9 if sku == "A" else 0.1,
                    approved_returns_24h=None,
                    label=int(sku == "A"),
                )
            )
    return pd.DataFrame(rows)


@pytest.mark.parametrize("test_days", [1, 5, 19])
def test_temporal_split_boundary_has_no_overlap(test_days):
    """Boundary: every test row is after every train row; test covers exactly `test_days` calendar days."""
    train, test = temporal_split(frame(), test_days)
    assert train["event_timestamp"].max() < test["event_timestamp"].min()
    assert test["event_timestamp"].dt.normalize().nunique() == test_days
    assert len(train) + len(test) == 200


def test_split_rejects_whole_range_as_test():
    """Test covers all data ⇒ empty train ⇒ error, not silently training on 0 rows."""
    with pytest.raises(ValueError):
        temporal_split(frame(days=3), test_days=3)


def test_baseline_fills_missing_with_train_prior():
    """A SKU without history gets the overall train rate, not 0."""
    train = pd.DataFrame({"label": [1, 0, 0, 0]})
    test = pd.DataFrame({"return_or_refund_rate_90d": [None, 0.5]})
    assert baseline_scores(train, test).tolist() == [0.25, 0.5]


def test_unit_price_guards_zero_quantity():
    """Quantity 0 ⇒ unit price NaN, no division by 0."""
    out = add_order_features(
        pd.DataFrame(
            {
                "ordered_basis_qty": [0, 2],
                "ordered_product_net_minor": [500, 500],
                "channel": ["a", "b"],
                "country": ["x", "y"],
                "labeled_count_90d": [1, 1],
                "return_or_refund_rate_90d": [0.1, 0.1],
                "approved_returns_24h": [None, 1],
            }
        )
    )
    assert np.isnan(out["unit_price_minor"][0]) and out["unit_price_minor"][1] == 250


def test_evaluate_perfect_vs_random():
    """A perfectly ranking model ⇒ PR-AUC 1 and beats the inverted baseline."""
    test = pd.DataFrame({"label": [1, 0, 1, 0]})
    m = evaluate(test, np.array([0.9, 0.1, 0.8, 0.2]), np.array([0.1, 0.9, 0.2, 0.8]))
    assert m["pr_auc_model"] == 1.0 and m["beats_baseline"] and m["prevalence"] == 0.5
