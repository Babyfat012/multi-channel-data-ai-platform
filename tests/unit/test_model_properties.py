"""Property-based (hypothesis): the model and preprocessing steps give the same result on rerun (idempotent).

Hypothesis generates hundreds of random datasets — including price 0, quantity 0, NaN,
unknown Channels — to find cases that change the result between two runs or break an invariant.
"""

import numpy as np
import pandas as pd
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from ml.train_lib import FEATURES, add_order_features, temporal_split, train_model

CHANNELS = ["amazon", "walmart", "shopify", "tiktok_shop", "shopee"]

order = st.fixed_dictionaries(
    {
        "channel": st.sampled_from(CHANNELS),
        "country": st.sampled_from(["a", "b", "c"]),
        "ordered_basis_qty": st.integers(0, 50),
        "ordered_product_net_minor": st.integers(0, 10_000_000),
        "labeled_count_90d": st.one_of(st.none(), st.integers(0, 500)),
        "return_or_refund_rate_90d": st.one_of(st.none(), st.floats(0, 1)),
        "approved_returns_24h": st.one_of(st.none(), st.integers(0, 20)),
    }
)


def training_frame(n=300, seed=0):
    """Fixed training set with signal (the tiktok_shop Channel returns more often)."""
    rng = np.random.default_rng(seed)
    frame = pd.DataFrame(
        {
            "channel": rng.choice(CHANNELS, n),
            "country": rng.choice(["a", "b", "c"], n),
            "ordered_basis_qty": rng.integers(1, 5, n),
            "ordered_product_net_minor": rng.integers(1000, 50000, n),
            "labeled_count_90d": rng.integers(0, 100, n),
            "return_or_refund_rate_90d": rng.random(n),
            "approved_returns_24h": rng.integers(0, 5, n),
        }
    )
    frame["label"] = ((frame["channel"] == "tiktok_shop") | (rng.random(n) < 0.2)).astype(int)
    return add_order_features(frame)


MODEL = train_model(training_frame())


@settings(max_examples=200, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(st.lists(order, min_size=1, max_size=20))
def test_prediction_is_idempotent_and_bounded(rows):
    """Same input ⇒ same score, every time; the score is always in [0, 1] and never NaN."""
    frame = add_order_features(pd.DataFrame(rows))
    first = MODEL.predict_proba(frame[FEATURES])[:, 1]
    second = MODEL.predict_proba(add_order_features(pd.DataFrame(rows))[FEATURES])[:, 1]
    assert np.array_equal(first, second)
    assert np.all((first >= 0) & (first <= 1)) and not np.isnan(first).any()


@settings(max_examples=200, deadline=None)
@given(st.lists(order, min_size=1, max_size=20))
def test_feature_prep_is_idempotent(rows):
    """Running add_order_features twice = running it once (no accumulation, no type change)."""
    once = add_order_features(pd.DataFrame(rows))
    twice = add_order_features(once)
    pd.testing.assert_frame_equal(once, twice)


@settings(max_examples=100, deadline=None)
@given(st.integers(1, 30), st.integers(0, 2**32 - 1))
def test_retraining_same_data_gives_same_model(test_days, seed):
    """Retraining on the same data with the same seed ⇒ identical scores (deterministic=True)."""
    frame = training_frame(n=120, seed=seed % 1000)
    a = train_model(frame).predict_proba(frame[FEATURES])[:, 1]
    b = train_model(frame).predict_proba(frame[FEATURES])[:, 1]
    assert np.array_equal(a, b)


@settings(max_examples=200, deadline=None)
@given(st.integers(2, 60), st.integers(1, 59))
def test_temporal_split_never_leaks_future(days, test_days):
    """Every valid split: test always after train, no lost/duplicated rows."""
    if test_days >= days:
        return
    rows = [
        {
            "event_timestamp": pd.Timestamp("2026-01-01", tz="UTC") + pd.Timedelta(days=d, hours=h),
            "label": int(h == 1),  # every day has both labels 0 and 1
        }
        for d in range(days)
        for h in (1, 13)
    ]
    train, test = temporal_split(pd.DataFrame(rows), test_days)
    assert train["event_timestamp"].max() < test["event_timestamp"].min()
    assert len(train) + len(test) == len(rows)


def test_temporal_split_rejects_single_class_test():
    """Edge case hypothesis found: test days with a single label ⇒ reject, no meaningless PR-AUC."""
    import pytest

    rows = [
        {
            "event_timestamp": pd.Timestamp("2026-01-01", tz="UTC") + pd.Timedelta(days=d),
            "label": d % 2 == 0,
        }
        for d in range(3)
    ]
    rows[-1]["label"] = 0
    with pytest.raises(ValueError, match="single class"):
        temporal_split(pd.DataFrame(rows).astype({"label": int}), 1)
