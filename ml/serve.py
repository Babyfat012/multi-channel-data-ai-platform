"""KServe return-risk predictor: the model is baked into the image and reads SKU features from Feast online.

Callers (agent/Web API) send only the order attributes; the predictor joins SKU features from Redis and scores.
A SKU with fewer than MIN_HISTORY labeled lines in 90 days ⇒ `abstain` (Q38), score still included for reference.
Missing labeled_count_90d ⇒ no scoring, score=null and reason=feature_data_unavailable.
"""

import os
import time

import lightgbm as lgb
import pandas as pd
from kserve import Model, ModelServer
from kserve.errors import InvalidInput
from prometheus_client import Counter, Histogram

from feature_repo.publication import FeatureDataUnavailable, load_published_store
from ml.train_lib import FEAST_FEATURES, FEATURES, add_order_features

# Business metrics, exposed at /metrics next to KServe's default latency metrics (scraped by Prometheus).
PREDICTIONS = Counter(
    "return_risk_predictions_total", "Number of predictions/abstentions", ["abstain"]
)
ERRORS = Counter("return_risk_errors_total", "Number of failed requests", ["reason"])
FEAST_SECONDS = Histogram("return_risk_feast_lookup_seconds", "Online feature read time")

ORDER_FIELDS = [
    "seller_sku",
    "channel",
    "country",
    "ordered_basis_qty",
    "ordered_product_net_minor",
]


class ReturnRisk(Model):
    """Model v1 protocol: `{"instances": [...]}` → `{"predictions": [...]}`."""

    def __init__(self, name: str):
        """Initialize config, version and load state of the risk prediction model."""
        super().__init__(name)
        self.min_history = int(os.environ.get("MIN_HISTORY", "20"))
        self.version = os.environ.get("MODEL_VERSION", "unknown")
        self.booster = None
        self.store = None

    def load(self):
        """Load the booster from the image and open the Feast connection (S3 registry + Redis)."""
        from feast import FeatureStore

        self.booster = lgb.Booster(model_file=os.environ.get("MODEL_PATH", "/opt/model/model.lgb"))
        repo = os.environ.get("FEAST_REPO", "/opt/feature_repo")
        self.store = (
            load_published_store(repo)
            if os.environ.get("FEAST_PUBLICATION_ROOT")
            else FeatureStore(repo)
        )
        self.ready = True
        return self.ready

    def score(self, instances: list[dict]) -> list[dict]:
        """Join online features by SKU, score, set the abstain flag."""
        orders = pd.DataFrame(instances)
        missing = [c for c in ORDER_FIELDS if c not in orders]
        if missing:
            ERRORS.labels("missing_field").inc()
            raise InvalidInput(f"missing fields {missing}")  # KServe returns 400, not 500
        started = time.perf_counter()
        release = None
        try:
            result = self.store.get_online_features(
                features=FEAST_FEATURES,
                entity_rows=[{"seller_sku": s} for s in orders["seller_sku"]],
            )
            release = getattr(result, "release", None)
            online = result.to_df()
        except FeatureDataUnavailable:
            online = pd.DataFrame(
                {
                    "seller_sku": orders["seller_sku"],
                    **{feature.split(":")[1]: None for feature in FEAST_FEATURES},
                }
            )
        FEAST_SECONDS.observe(time.perf_counter() - started)
        frame = add_order_features(
            pd.concat([orders.reset_index(drop=True), online.drop(columns="seller_sku")], axis=1)
        )
        history = frame["labeled_count_90d"]
        available = history.notna()
        scores = pd.Series(float("nan"), index=frame.index)
        if available.any():
            scores.loc[available] = self.booster.predict(frame.loc[available, FEATURES])
        predictions = []
        for sku, score, count in zip(orders["seller_sku"], scores, history, strict=True):
            unavailable = pd.isna(count)
            if unavailable:
                reason = "feature_data_unavailable"
                ERRORS.labels(reason).inc()
            elif count < self.min_history:
                reason = "insufficient_history"
            else:
                reason = None
            PREDICTIONS.labels(str(reason is not None).lower()).inc()
            predictions.append(
                {
                    "seller_sku": sku,
                    "score": None if unavailable else float(score),
                    "abstain": reason is not None,
                    "reason": reason,
                    "model_version": self.version,
                    "batch_release": release.release_id if release else None,
                    "batch_cutoff": release.cutoff if release else None,
                }
            )
        return predictions

    def predict(self, payload: dict, headers: dict | None = None) -> dict:
        """KServe entry point."""
        return {"predictions": self.score(payload["instances"])}


if __name__ == "__main__":
    model = ReturnRisk(os.environ.get("MODEL_NAME", "return-risk"))
    model.load()
    ModelServer().start([model])
