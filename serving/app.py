"""Web API that fetches SKU features (Feast/Redis) and policy chunks (Feast RAG/pgvector).

Run: `uvicorn serving.app:app` from the repo root; needs `FEAST_REPO_PATH` pointing at `feature_repo/`
plus the environment variables `feature_store.yaml` reads (FEAST_REGISTRY, FEAST_REDIS). Chunk search is
enabled only when `FEAST_RAG_REGISTRY` is set (+ RAG_PG_*, see `rag/feature_repo/feature_store.yaml`).
"""

import asyncio
import functools
import os
import time
from contextlib import asynccontextmanager
from typing import Annotated, Literal

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from pydantic import AfterValidator, BaseModel, Field, StringConstraints, ValidationError

from feature_repo.publication import load_published_store

# Look up each view separately: if one view fails or is not registered, its features are None
# instead of failing the whole request.
VIEWS = {
    "sku_return_daily": ["labeled_count_90d", "positive_count_90d", "return_or_refund_rate_90d"],
    "sku_return_24h": ["approved_returns_24h"],
}
MAX_SKUS = 100

# Web API metrics: Prometheus scrapes /metrics; `handler` is the path template to avoid label explosion.
REQUESTS = Counter("http_requests_total", "Number of requests", ["method", "handler", "status"])
LATENCY = Histogram("http_request_duration_seconds", "Processing time", ["handler"])
# Agent telemetry: every MCP tool call, split into success / error. MCP errors are still HTTP 200
# (isError in JSON-RPC), so http_requests_total cannot count them.
TOOL_CALLS = Counter("mcp_tool_calls_total", "Number of MCP tool calls", ["tool", "outcome"])


def counted(tool):
    """Wrap an MCP tool to count into TOOL_CALLS; keeps the signature (MCP builds the schema from it)."""

    @functools.wraps(tool)
    async def wrapper(*args, **kwargs):
        """Count the MCP tool call as success or error, then return the tool's result."""
        try:
            result = await tool(*args, **kwargs)
        except Exception:
            TOOL_CALLS.labels(tool.__name__, "error").inc()
            raise
        TOOL_CALLS.labels(tool.__name__, "ok").inc()
        return result

    return wrapper


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
    batch_status: Literal["ready", "insufficient_history", "not_available", "unavailable"]
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


class PredictRequest(BaseModel):
    """One new Order Line; the model reads SKU features from Feast online itself, no need to send them."""

    seller_sku: SellerSku
    channel: Literal["amazon", "walmart", "shopify", "tiktok_shop", "shopee"]
    # Ship-to country (ISO-2): present in the source files of all five Channels; required, never imputed.
    country: str = Field(pattern=r"^[A-Z]{2}$")
    ordered_basis_qty: int = Field(ge=1, le=1000)
    ordered_product_net_minor: int = Field(ge=0, le=1_000_000_000)


class Prediction(BaseModel):
    """score is a Return risk ranking signal in [0, 1], not a calibrated probability."""

    seller_sku: str
    score: Rounded | None
    abstain: bool
    reason: str | None
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


def load_store():
    """Load Feast only in a real run, so tests do not need feast installed."""
    from feast import FeatureStore

    repo = os.environ.get("FEAST_REPO_PATH", "feature_repo")
    if os.environ.get("FEAST_PUBLICATION_ROOT"):
        return load_published_store(repo)
    return FeatureStore(repo)


def load_rag():
    """Return an object with .search(query, top_k) and .get(chunk_ids) over Feast RAG; None if not configured."""
    if not os.environ.get("FEAST_RAG_REGISTRY"):
        return None
    from types import SimpleNamespace

    from rag.pipeline import _store, get_chunks, search

    store = _store()
    cache = os.environ.get("RAG_MODEL_CACHE")
    return SimpleNamespace(
        search=lambda query, top_k: search(store, query, top_k=top_k, cache_dir=cache),
        get=lambda chunk_ids: get_chunks(store, chunk_ids),
    )


async def fetch_chunks(rag, method, *args):
    """Feast/embedding code is synchronous ⇒ run it in a thread so it does not block the event loop."""
    if rag is None:
        raise HTTPException(503, "rag not configured")
    try:
        rows = await asyncio.to_thread(getattr(rag, method), *args)
    except Exception as exc:
        raise HTTPException(503, "vector store unavailable") from exc
    return ChunkResponse(chunks=[PolicyChunk(**r) for r in rows])


async def predict(http, url, body):
    """Call KServe (protocol v1) for 1 Order Line."""
    if not url:
        raise HTTPException(503, "model not configured")
    instance = body.model_dump()
    try:
        response = await http.post(url, json={"instances": [instance]}, timeout=10)
        response.raise_for_status()
        return Prediction(**response.json()["predictions"][0])
    except (httpx.HTTPError, KeyError, IndexError, ValueError) as exc:
        raise HTTPException(503, "model unavailable") from exc


async def fetch_features(store, requested_skus):
    """Look up features for a SKU list; lives outside `create_app` so mutmut can mutate it."""
    # Drop duplicate SKUs but keep order: repeated SKUs do not change the result.
    skus = list(dict.fromkeys(requested_skus))
    entity_rows = [{"seller_sku": s} for s in skus]
    results = await asyncio.gather(
        *(
            store.get_online_features_async(
                features=[f"{view}:{name}" for name in names], entity_rows=entity_rows
            )
            for view, names in VIEWS.items()
        ),
        return_exceptions=True,
    )
    if all(isinstance(r, Exception) for r in results):
        raise HTTPException(503, "online store unavailable")
    columns = {"seller_sku": skus}
    for names, result in zip(VIEWS.values(), results, strict=True):
        values = {} if isinstance(result, Exception) else result.to_dict()
        for name in names:
            columns[name] = values.get(name, [None] * len(skus))
    rows = []
    for i, sku in enumerate(skus):
        daily, stream = results
        release = None if isinstance(daily, Exception) else getattr(daily, "release", None)
        # Legacy/mock OnlineResponse has no real release metadata.
        release_id = getattr(release, "release_id", None)
        release = release if isinstance(release_id, str) else None
        count, positive, rate = (columns[name][i] for name in VIEWS["sku_return_daily"])
        if isinstance(daily, Exception):
            batch_status = "unavailable"
        elif count is None:
            batch_status = (
                "unavailable" if release and sku in release.expected_skus else "not_available"
            )
        elif positive is None or (count > 0 and rate is None):
            batch_status = "unavailable"
        elif count == 0:
            batch_status = "insufficient_history"
        else:
            batch_status = "ready"
        stream_status = (
            "unavailable"
            if isinstance(stream, Exception)
            else "not_available"
            if columns["approved_returns_24h"][i] is None
            else "ready"
        )
        rows.append(
            SkuFeatures(
                **{name: values[i] for name, values in columns.items()},
                batch_status=batch_status,
                stream_status=stream_status,
                batch_release=release.release_id if release else None,
                batch_cutoff=release.cutoff if release else None,
            )
        )
    return FeatureResponse(features=rows)


def create_app(
    store_factory=load_store, rag_factory=load_rag, http_factory=httpx.AsyncClient, surface=None
):
    """`store_factory`, `rag_factory`, `http_factory` let tests swap in fakes.

    `surface` (default env `API_SURFACE`): `rest` = Feature API pod, `mcp` = MCP server pod,
    `all` = both in one process (local/test runs). Both pods share the same image and code.
    """
    surface = surface or os.environ.get("API_SURFACE", "all")
    if surface not in {"all", "rest", "mcp"}:
        raise ValueError(f"API_SURFACE must be all|rest|mcp, got {surface!r}")
    # Same feature lookup functions, also exposed as MCP tools for the Agent.
    # stateless: any MCP request can reach any replica, no sticky session needed.
    mcp = MCPServer(
        "platform-features",
        instructions=(
            "Look up return rate features by SKU, score Return risk for new orders"
            " and search return policy chunks."
        ),
    )
    mcp_app = mcp.streamable_http_app(
        streamable_http_path="/", stateless_http=True, json_response=True, host="0.0.0.0"
    )

    @mcp.tool()
    @counted
    async def get_sku_features(seller_skus: list[str]) -> FeatureResponse:
        # Docstring = the tool description the LLM sees ⇒ English: a Vietnamese description made Qwen 1.5B not call the tool (0/3 vs 3/3).
        """Return features and per-group availability for 1-100 SKUs (e.g. APP-00042).

        Use only ready data. Missing values are null, never assumed zero; unavailable groups
        must be reported as unavailable. insufficient_history means confirmed zero labeled samples.
        """
        # ToolError passes its message to the LLM unchanged; MCP reports other errors only generically.
        try:
            body = FeatureRequest(seller_skus=seller_skus)
            return await fetch_features(app.state.store, body.seller_skus)
        except ValidationError as exc:
            raise ToolError(f"invalid seller_skus: {exc.errors()[0]['msg']}") from exc
        except HTTPException as exc:
            raise ToolError(exc.detail) from exc

    @mcp.tool()
    @counted
    async def search_policy_chunks(query: str, top_k: int = 3) -> ChunkResponse:
        """Search return/refund policy text of Amazon, Walmart, Shopify, TikTok Shop, Shopee and the
        seller playbook. Use for ANY policy, return window or refund question. Returns top_k (1-10) chunks."""
        try:
            body = ChunkRequest(query=query, top_k=top_k)
            return await fetch_chunks(app.state.rag, "search", body.query, body.top_k)
        except ValidationError as exc:
            raise ToolError(f"invalid input: {exc.errors()[0]['msg']}") from exc
        except HTTPException as exc:
            raise ToolError(exc.detail) from exc

    @mcp.tool()
    @counted
    async def get_policy_chunks(chunk_ids: list[str]) -> ChunkResponse:
        """Get the text of 1-20 policy chunks by chunk_id (16 hex characters)."""
        try:
            body = ChunkIdRequest(chunk_ids=chunk_ids)
            return await fetch_chunks(app.state.rag, "get", body.chunk_ids)
        except ValidationError as exc:
            raise ToolError(f"invalid chunk_ids: {exc.errors()[0]['msg']}") from exc
        except HTTPException as exc:
            raise ToolError(exc.detail) from exc

    @mcp.tool()
    @counted
    async def predict_return_risk(
        seller_sku: str,
        channel: str,
        ordered_basis_qty: int,
        ordered_product_net_minor: int,
        country: str,
    ) -> Prediction:
        """Return risk score (0-1, higher = riskier, for ranking) of one new order line.

        channel: amazon|walmart|shopify|tiktok_shop|shopee; country: ISO-2 ship-to country (e.g. US);
        ordered_product_net_minor: price in cents. abstain=true when the SKU has too little labeled history.
        """
        try:
            body = PredictRequest(
                seller_sku=seller_sku,
                channel=channel,
                country=country,
                ordered_basis_qty=ordered_basis_qty,
                ordered_product_net_minor=ordered_product_net_minor,
            )
            return await predict(app.state.http, os.environ.get("KSERVE_URL"), body)
        except ValidationError as exc:
            err = exc.errors()[0]
            raise ToolError(f"invalid {err['loc'][0]}: {err['msg']}") from exc
        except HTTPException as exc:
            raise ToolError(exc.detail) from exc

    @asynccontextmanager
    async def lifespan(app):
        """Load the stores and manage the lifecycle of the HTTP client and the MCP session."""
        app.state.store = store_factory()
        app.state.rag = rag_factory()
        app.state.http = http_factory()
        async with app.state.http, mcp.session_manager.run():
            yield

    app = FastAPI(title="Feature API", lifespan=lifespan)
    if surface != "rest":
        app.mount("/mcp", mcp_app)

    @app.middleware("http")
    async def metrics(request: Request, call_next):
        """Record request count, status and latency per handler path."""
        start = time.perf_counter()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            return response
        finally:
            # The MCP app is mounted ⇒ its inner route is "/"; group under "/mcp" for readability.
            route = request.scope.get("route")
            if request.url.path.startswith("/mcp"):
                handler = "/mcp"
            else:
                handler = getattr(route, "path", "other")
            REQUESTS.labels(request.method, handler, str(status)).inc()
            LATENCY.labels(handler).observe(time.perf_counter() - start)

    @app.get("/metrics", include_in_schema=False)
    async def metrics_endpoint():
        """Expose metrics in Prometheus format."""
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.get("/healthz")
    async def healthz():
        """Liveness for k8s: the process is alive."""
        return {"status": "ok"}

    @app.get("/readyz")
    async def readyz(request: Request):
        """Readiness for k8s: the Feast registry has been loaded."""
        if getattr(request.app.state, "store", None) is None:
            raise HTTPException(503, "feature store not loaded")
        return {"status": "ready"}

    if surface == "mcp":
        return app

    @app.post("/features", response_model=FeatureResponse)
    async def features(body: FeatureRequest, request: Request):
        """Look up online features for the SKUs in a REST request."""
        return await fetch_features(request.app.state.store, body.seller_skus)

    @app.post("/chunks", response_model=ChunkResponse)
    async def chunks(body: ChunkRequest, request: Request):
        """Find the policy chunks closest to the question in a REST request."""
        return await fetch_chunks(request.app.state.rag, "search", body.query, body.top_k)

    @app.post("/predict", response_model=Prediction)
    async def predict_endpoint(body: PredictRequest, request: Request):
        """Send the order attributes to KServe and return the risk prediction."""
        return await predict(request.app.state.http, os.environ.get("KSERVE_URL"), body)

    @app.post("/chunks/by-id", response_model=ChunkResponse)
    async def chunks_by_id(body: ChunkIdRequest, request: Request):
        """Look up policy chunks by id in a REST request."""
        return await fetch_chunks(request.app.state.rag, "get", body.chunk_ids)

    return app


app = create_app()
