"""Pure training logic, no Kubeflow dependency: fetch features, split by time, train, evaluate.

Each KFP step in `ml/training_pipeline.py` calls just one function here, so it is testable with pytest.
Score is a RANKING signal (Q38), not a probability; baseline = the SKU's 90-day return rate.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

FEAST_FEATURES = [
    "sku_return_daily:labeled_count_90d",
    # Not a model input: the predictor needs it to tell a whole daily row from a broken one (daily_status).
    "sku_return_daily:positive_count_90d",
    "sku_return_daily:return_or_refund_rate_90d",
    "sku_return_24h:approved_returns_24h",
]
CATEGORICAL = ["channel", "country"]
NUMERIC = [
    "ordered_basis_qty",
    "ordered_product_net_minor",
    "unit_price_minor",
    "labeled_count_90d",
    "return_or_refund_rate_90d",
    "approved_returns_24h",
]
FEATURES = CATEGORICAL + NUMERIC


def add_order_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Add unit price; cast categoricals for LightGBM. Does not touch the label."""
    out = frame.copy()
    qty = out["ordered_basis_qty"].where(out["ordered_basis_qty"] > 0)
    out["unit_price_minor"] = out["ordered_product_net_minor"] / qty
    for col in CATEGORICAL:
        out[col] = out[col].astype("category")
    for col in NUMERIC:
        out[col] = pd.to_numeric(out[col], errors="coerce").astype("float64")
    return out


def temporal_split(frame: pd.DataFrame, test_days: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Test = the last `test_days` order days; train = everything before. No random split."""
    ts = pd.to_datetime(frame["event_timestamp"], utc=True)
    boundary = ts.max().normalize() - pd.Timedelta(days=test_days - 1)
    train, test = frame[ts < boundary], frame[ts >= boundary]
    if train.empty or test.empty or test["label"].nunique() < 2:
        raise ValueError(f"empty split or test has a single class (boundary {boundary})")
    return train, test


def baseline_scores(train: pd.DataFrame, test: pd.DataFrame) -> np.ndarray:
    """Baseline Q38: the SKU's historical return rate; SKUs without history get the overall train rate."""
    prior = float(train["label"].mean())
    return test["return_or_refund_rate_90d"].fillna(prior).to_numpy()


def train_model(train: pd.DataFrame, seed: int = 7):
    """Small LightGBM with a fixed seed so reruns produce the same model."""
    import lightgbm as lgb

    model = lgb.LGBMClassifier(
        n_estimators=200,
        learning_rate=0.05,
        num_leaves=15,
        min_child_samples=20,
        subsample=0.8,
        subsample_freq=1,
        colsample_bytree=0.8,
        random_state=seed,
        deterministic=True,
        force_row_wise=True,
        verbose=-1,
    )
    model.fit(train[FEATURES], train["label"])
    return model


def evaluate(
    test: pd.DataFrame,
    model_scores: np.ndarray,
    base_scores: np.ndarray,
    champion_scores: np.ndarray | None = None,
) -> dict:
    """PR-AUC (average precision) of the model, baseline and (if any) the current production model
    (champion), all on the SAME test set, so the same prevalence. A PR-AUC from another run's test set
    is not comparable: a test window with more returns scores higher with no better model."""
    from sklearn.metrics import average_precision_score

    y = test["label"].to_numpy()
    prevalence = float(y.mean())
    model_ap = float(average_precision_score(y, model_scores))
    base_ap = float(average_precision_score(y, base_scores))
    result = {
        "test_rows": int(len(y)),
        "prevalence": prevalence,
        "pr_auc_model": model_ap,
        "pr_auc_baseline": base_ap,
        "lift_model": model_ap / prevalence,
        "beats_baseline": model_ap > base_ap,
    }
    if champion_scores is not None:
        champion_ap = float(average_precision_score(y, champion_scores))
        result |= {"pr_auc_champion": champion_ap, "beats_champion": model_ap > champion_ap}
    return result


def champion_scores(booster, test: pd.DataFrame) -> np.ndarray | None:
    """Scores of the production model on this test set, using the features IT was trained with.
    None when the test set lacks one of them (feature set changed): not comparable, not a crash."""
    needed = booster.feature_name()
    if not set(needed) <= set(test.columns):
        return None
    return booster.predict(test[needed])
