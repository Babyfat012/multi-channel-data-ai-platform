"""KServe return-risk predictor: KServe downloads the model via STORAGE_URI into /mnt/models; features come from Feast.

Callers (MCP server, or clients through the ingress `/predict`) send only the order attributes; the predictor
validates them, joins SKU features from Redis and scores. This is the trust boundary for prediction input.
Daily features are read with `feature_store.publication.daily_status`, the same rule as the Feature API:
- missing or incomplete (count, positive, rate) ⇒ no scoring, score=null, reason=feature_data_unavailable;
- 0 labeled lines ⇒ abstain, reason=no_history; fewer than MIN_HISTORY ⇒ abstain, reason=too_few_samples
  (Q38). Both still include the score for reference.
"""

import os
import re
import time

import lightgbm as lgb
import pandas as pd
from kserve import Model, ModelServer
from kserve.errors import InvalidInput
from prometheus_client import Counter, Histogram

from feature_store.publication import FeatureDataUnavailable, daily_status, load_published_store
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
MAX_INSTANCES = 100  # requests now reach KServe directly through the ingress: cap the batch size
CHANNELS = ("amazon", "walmart", "shopify", "tiktok_shop", "shopee")
# Same limits the Web API enforced before it handed /predict to KServe. Plain checks, no pydantic: the
# image has it, but the unit-test environment and CI do not.
TEXT_RULES = {
    # The real form is `sku-00001`; wider so other Channels still pass, but odd characters are blocked.
    "seller_sku": re.compile(r"[A-Za-z0-9_.-]{1,64}"),
    # Ship-to country (ISO-2): present in the source files of all five Channels; required, never imputed.
    "country": re.compile(r"[A-Z]{2}"),
}
INT_RULES = {"ordered_basis_qty": (1, 1000), "ordered_product_net_minor": (0, 1_000_000_000)}


def order_error(row) -> str | None:
    """First problem in one order line, or None when it is valid."""
    if not isinstance(row, dict):
        return "instance: must be an object"
    missing = [field for field in ORDER_FIELDS if field not in row]
    if missing:
        return f"missing fields {missing}"
    for field, pattern in TEXT_RULES.items():
        if not isinstance(row[field], str) or not pattern.fullmatch(row[field]):
            return f"invalid {field}: must match {pattern.pattern}"
    if row["channel"] not in CHANNELS:
        return f"invalid channel: must be one of {', '.join(CHANNELS)}"
    for field, (low, high) in INT_RULES.items():
        value = row[field]
        if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
            return f"invalid {field}: must be an integer in [{low}, {high}]"
    return None


def validated_orders(instances) -> pd.DataFrame:
    """Validate every instance; any bad one rejects the whole request with KServe's 400 and the first error."""
    if not isinstance(instances, list) or not 1 <= len(instances) <= MAX_INSTANCES:
        ERRORS.labels("invalid_input").inc()
        raise InvalidInput(f"instances must be a list of 1-{MAX_INSTANCES} order lines")
    for row in instances:
        error = order_error(row)
        if error:
            ERRORS.labels("invalid_input").inc()
            raise InvalidInput(error)
    # Only the known order fields go on; anything extra a client sends is dropped.
    return pd.DataFrame([{field: row[field] for field in ORDER_FIELDS} for row in instances])


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
        """Load the booster downloaded by the storage-initializer and open Feast (S3 registry + Redis)."""
        from feast import FeatureStore

        self.booster = lgb.Booster(model_file=os.environ.get("MODEL_PATH", "/mnt/models/model.lgb"))
        repo = os.environ.get("FEAST_REPO", "/opt/feature_store")
        self.store = (
            load_published_store(repo)
            if os.environ.get("FEAST_PUBLICATION_ROOT")
            else FeatureStore(repo)
        )
        self.ready = True
        return self.ready

    def score(self, instances: list[dict]) -> list[dict]:
        """Validate, join online features by SKU, score, set the abstain flag."""
        orders = validated_orders(instances)
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
        plain = lambda v: None if pd.isna(v) else v  # noqa: E731 - NaN/NA → None for daily_status
        expected = release.expected_skus if release else frozenset()
        statuses = [
            daily_status(
                plain(row.labeled_count_90d),
                plain(row.positive_count_90d),
                plain(row.return_or_refund_rate_90d),
                expected=row.seller_sku in expected,
            )
            for row in frame.itertuples()
        ]
        # Same reading as the Feature API: score only rows whose daily features are whole.
        available = pd.Series([s in ("ready", "no_history") for s in statuses], index=frame.index)
        scores = pd.Series(float("nan"), index=frame.index)
        if available.any():
            scores.loc[available] = self.booster.predict(frame.loc[available, FEATURES])
        predictions = []
        history = frame["labeled_count_90d"]
        for sku, score, count, ok, status in zip(
            orders["seller_sku"], scores, history, available, statuses, strict=True
        ):
            unavailable = not ok
            if unavailable:
                reason = "feature_data_unavailable"
                ERRORS.labels(reason).inc()
            elif status == "no_history":
                reason = "no_history"
            elif count < self.min_history:
                reason = "too_few_samples"
            else:
                reason = None
            PREDICTIONS.labels(str(reason is not None).lower()).inc()
            predictions.append(
                {
                    "seller_sku": sku,
                    # 4 digits: long decimals get masked as <PHONE_NUMBER> by the agentgateway guardrail.
                    "score": None if unavailable else round(float(score), 4),
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
        return {"predictions": self.score(payload.get("instances"))}


if __name__ == "__main__":
    model = ReturnRisk(os.environ.get("MODEL_NAME", "return-risk"))
    model.load()
    ModelServer().start([model])
