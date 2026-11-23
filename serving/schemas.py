"""Request/response models shared by the Feature API, the Policy API and the MCP server."""

from typing import Annotated, Literal

from pydantic import AfterValidator, BaseModel, Field, StringConstraints

MAX_SKUS = 100

# The real form is `sku-00001`; allow a wider pattern so other Channels still pass, but block odd characters.
SellerSku = Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9_.-]{1,64}$")]
# Long decimals (0.16980269175486737) get masked as <PHONE_NUMBER> by the agentgateway phoneNumber guardrail
# in the Agent's answer ⇒ round to 4 digits, still enough to rank SKUs.
Rounded = Annotated[float, AfterValidator(lambda v: round(v, 4))]


class FeatureRequest(BaseModel):
    """List of valid SKUs to look up features for, with a cap on SKUs per request."""

    seller_skus: list[SellerSku] = Field(min_length=1, max_length=MAX_SKUS)


class SkuFeatures(BaseModel):
    """One SKU; a feature is None when Redis does not have it yet or its TTL has expired."""

    seller_sku: str
    labeled_count_90d: int | None
    positive_count_90d: int | None
    return_or_refund_rate_90d: Rounded | None
    approved_returns_24h: int | None
    batch_status: Literal["ready", "no_history", "not_available", "unavailable"]
    stream_status: Literal["ready", "not_available", "unavailable"]
    batch_release: str | None = None
    batch_cutoff: str | None = None


class FeatureResponse(BaseModel):
    """Feature lookup result for the requested SKU list."""

    features: list[SkuFeatures]


class ChunkRequest(BaseModel):
    """Question and number of policy chunks to retrieve by similarity."""

    query: str = Field(min_length=3, max_length=500)
    top_k: int = Field(default=3, ge=1, le=10)


class Prediction(BaseModel):
    """score is a Return risk ranking signal in [0, 1], not a calibrated probability."""

    seller_sku: str
    score: Rounded | None
    abstain: bool
    # no_history: 0 labeled lines; too_few_samples: fewer than the predictor's MIN_HISTORY (score is a hint only).
    reason: Literal["feature_data_unavailable", "no_history", "too_few_samples"] | None
    model_version: str | None
    batch_release: str | None = None
    batch_cutoff: str | None = None


ChunkId = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{16}$")]


class ChunkIdRequest(BaseModel):
    """List of valid chunk ids to look up directly in the online store."""

    chunk_ids: list[ChunkId] = Field(min_length=1, max_length=20)


class PolicyChunk(BaseModel):
    """One return policy chunk; cosine distance, smaller is closer to the question (None for id lookups)."""

    chunk_id: str
    doc_id: str | None
    heading: str | None
    text: str | None
    distance: float | None = None


class ChunkResponse(BaseModel):
    """List of policy chunks returned by a RAG query."""

    chunks: list[PolicyChunk]
