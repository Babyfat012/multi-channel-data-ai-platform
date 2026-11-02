"""Web API kéo feature SKU (Feast/Redis) và chunk chính sách (Feast RAG/pgvector).

Chạy: `uvicorn serving.app:app` từ gốc repo, cần `FEAST_REPO_PATH` trỏ tới `feature_repo/`
cùng biến môi trường mà `feature_store.yaml` đọc (FEAST_REGISTRY, FEAST_REDIS). Tìm chunk chỉ bật khi
có `FEAST_RAG_REGISTRY` (+ RAG_PG_*, xem `rag/feature_repo/feature_store.yaml`).
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

# Tra từng view riêng: một view lỗi/chưa đăng ký thì feature của nó là None, không hỏng cả request.
VIEWS = {
    "sku_return_daily": ["labeled_count_90d", "positive_count_90d", "return_or_refund_rate_90d"],
    "sku_return_24h": ["approved_returns_24h"],
}
MAX_SKUS = 100

# Số đo Web API: Prometheus lấy ở /metrics; `handler` là mẫu đường dẫn để không nổ nhãn.
REQUESTS = Counter("http_requests_total", "Số request", ["method", "handler", "status"])
LATENCY = Histogram("http_request_duration_seconds", "Thời gian xử lý", ["handler"])
# Agent telemetry: mỗi lượt gọi MCP tool, tách thành công / lỗi. Lỗi MCP vẫn là HTTP 200
# (isError trong JSON-RPC) nên không đếm được bằng http_requests_total.
TOOL_CALLS = Counter("mcp_tool_calls_total", "Số lượt gọi MCP tool", ["tool", "outcome"])


def counted(tool):
    """Bọc MCP tool để đếm vào TOOL_CALLS; giữ nguyên chữ ký (MCP sinh schema từ đó)."""

    @functools.wraps(tool)
    async def wrapper(*args, **kwargs):
        """Đếm lượt gọi MCP tool thành công hoặc lỗi rồi trả kết quả của tool."""
        try:
            result = await tool(*args, **kwargs)
        except Exception:
            TOOL_CALLS.labels(tool.__name__, "error").inc()
            raise
        TOOL_CALLS.labels(tool.__name__, "ok").inc()
        return result

    return wrapper


# Dạng thật là `sku-00001`; cho rộng hơn để Channel khác vẫn qua, nhưng chặn ký tự lạ.
SellerSku = Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9_.-]{1,64}$")]
# Số thập phân dài (0.16980269175486737) bị guardrail phoneNumber của agentgateway che thành <PHONE_NUMBER>
# trong câu trả lời Agent ⇒ làm tròn 4 số, vẫn đủ để xếp hạng SKU.
Rounded = Annotated[float, AfterValidator(lambda v: round(v, 4))]


class FeatureRequest(BaseModel):
    """Danh sách SKU hợp lệ cần tra feature, giới hạn số SKU mỗi request."""

    seller_skus: list[SellerSku] = Field(min_length=1, max_length=MAX_SKUS)


class SkuFeatures(BaseModel):
    """Một SKU; feature là None khi Redis chưa có hoặc đã hết TTL."""

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
    """Kết quả tra feature cho danh sách SKU được yêu cầu."""

    features: list[SkuFeatures]


class ChunkRequest(BaseModel):
    """Câu hỏi và số chunk chính sách cần tìm theo độ tương đồng."""

    query: str = Field(min_length=3, max_length=500)
    top_k: int = Field(default=3, ge=1, le=10)


class PredictRequest(BaseModel):
    """Một Order Line mới; model tự đọc feature SKU từ Feast online, không cần gửi kèm."""

    seller_sku: SellerSku
    channel: Literal["amazon", "walmart", "shopify", "tiktok_shop", "shopee"]
    # Nước giao hàng (ISO-2): có trong file gốc của cả năm Channel; bắt buộc, không tự điền.
    country: str = Field(pattern=r"^[A-Z]{2}$")
    ordered_basis_qty: int = Field(ge=1, le=1000)
    ordered_product_net_minor: int = Field(ge=0, le=1_000_000_000)


class Prediction(BaseModel):
    """score là tín hiệu xếp hạng rủi ro Return trong [0, 1], không phải xác suất đã hiệu chỉnh."""

    seller_sku: str
    score: Rounded | None
    abstain: bool
    reason: str | None
    model_version: str | None
    batch_release: str | None = None
    batch_cutoff: str | None = None


ChunkId = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{16}$")]


class ChunkIdRequest(BaseModel):
    """Danh sách mã chunk hợp lệ cần tra trực tiếp trong online store."""

    chunk_ids: list[ChunkId] = Field(min_length=1, max_length=20)


class PolicyChunk(BaseModel):
    """Một đoạn chính sách trả hàng; distance cosine, càng nhỏ càng gần câu hỏi (None khi tra theo id)."""

    chunk_id: str
    doc_id: str | None
    heading: str | None
    text: str | None
    distance: float | None = None


class ChunkResponse(BaseModel):
    """Danh sách các đoạn chính sách trả về từ truy vấn RAG."""

    chunks: list[PolicyChunk]


def load_store():
    """Nạp Feast chỉ khi chạy thật, để test không cần cài feast."""
    from feast import FeatureStore

    repo = os.environ.get("FEAST_REPO_PATH", "feature_repo")
    if os.environ.get("FEAST_PUBLICATION_ROOT"):
        return load_published_store(repo)
    return FeatureStore(repo)


def load_rag():
    """Trả object có .search(query, top_k) và .get(chunk_ids) trên Feast RAG; None nếu chưa cấu hình."""
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
    """Feast/embedding là code đồng bộ ⇒ chạy trong thread để không chặn event loop."""
    if rag is None:
        raise HTTPException(503, "rag not configured")
    try:
        rows = await asyncio.to_thread(getattr(rag, method), *args)
    except Exception as exc:
        raise HTTPException(503, "vector store unavailable") from exc
    return ChunkResponse(chunks=[PolicyChunk(**r) for r in rows])


async def predict(http, url, body):
    """Gọi KServe (protocol v1) cho 1 Order Line."""
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
    """Tra feature cho danh sách SKU; nằm ngoài `create_app` để mutmut đột biến được."""
    # Bỏ SKU trùng nhưng giữ thứ tự: gửi lặp không đổi kết quả.
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
        # Legacy/mock OnlineResponse không có release metadata thực.
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


def create_app(store_factory=load_store, rag_factory=load_rag, http_factory=httpx.AsyncClient):
    """`store_factory`, `rag_factory`, `http_factory` để test thay bằng bản giả."""
    # Cùng hàm tra feature, phơi ra thêm dạng MCP tool cho Agent.
    # stateless: request MCP nào cũng tới được replica nào, không cần sticky session.
    mcp = MCPServer(
        "platform-features",
        instructions=(
            "Tra feature tỷ lệ trả hàng theo SKU, chấm điểm rủi ro Return cho đơn mới"
            " và tìm đoạn chính sách trả hàng."
        ),
    )
    mcp_app = mcp.streamable_http_app(
        streamable_http_path="/", stateless_http=True, json_response=True, host="0.0.0.0"
    )

    @mcp.tool()
    @counted
    async def get_sku_features(seller_skus: list[str]) -> FeatureResponse:
        # Docstring = mô tả tool LLM thấy ⇒ tiếng Anh: mô tả tiếng Việt làm Qwen 1,5B không gọi tool (0/3 so với 3/3).
        """Return features and per-group availability for 1-100 SKUs (e.g. APP-00042).

        Use only ready data. Missing values are null, never assumed zero; unavailable groups
        must be reported as unavailable. insufficient_history means confirmed zero labeled samples.
        """
        # ToolError giữ nguyên lời nhắn cho LLM; lỗi khác MCP chỉ báo chung chung.
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
        """Nạp các store và quản lý vòng đời HTTP client cùng phiên MCP."""
        app.state.store = store_factory()
        app.state.rag = rag_factory()
        app.state.http = http_factory()
        async with app.state.http, mcp.session_manager.run():
            yield

    app = FastAPI(title="Feature API", lifespan=lifespan)
    app.mount("/mcp", mcp_app)

    @app.middleware("http")
    async def metrics(request: Request, call_next):
        """Ghi số request, trạng thái và độ trễ theo đường dẫn xử lý."""
        start = time.perf_counter()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            return response
        finally:
            # App MCP được mount ⇒ route bên trong là "/"; gom về "/mcp" cho dễ đọc.
            route = request.scope.get("route")
            if request.url.path.startswith("/mcp"):
                handler = "/mcp"
            else:
                handler = getattr(route, "path", "other")
            REQUESTS.labels(request.method, handler, str(status)).inc()
            LATENCY.labels(handler).observe(time.perf_counter() - start)

    @app.get("/metrics", include_in_schema=False)
    async def metrics_endpoint():
        """Xuất các metric theo định dạng Prometheus."""
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.get("/healthz")
    async def healthz():
        """Liveness cho k8s: process còn sống."""
        return {"status": "ok"}

    @app.get("/readyz")
    async def readyz(request: Request):
        """Readiness cho k8s: đã nạp được Feast registry."""
        if getattr(request.app.state, "store", None) is None:
            raise HTTPException(503, "feature store not loaded")
        return {"status": "ready"}

    @app.post("/features", response_model=FeatureResponse)
    async def features(body: FeatureRequest, request: Request):
        """Tra feature online cho các SKU trong request REST."""
        return await fetch_features(request.app.state.store, body.seller_skus)

    @app.post("/chunks", response_model=ChunkResponse)
    async def chunks(body: ChunkRequest, request: Request):
        """Tìm các chunk chính sách gần câu hỏi trong request REST."""
        return await fetch_chunks(request.app.state.rag, "search", body.query, body.top_k)

    @app.post("/predict", response_model=Prediction)
    async def predict_endpoint(body: PredictRequest, request: Request):
        """Gửi thuộc tính đơn hàng tới KServe và trả dự đoán rủi ro."""
        return await predict(request.app.state.http, os.environ.get("KSERVE_URL"), body)

    @app.post("/chunks/by-id", response_model=ChunkResponse)
    async def chunks_by_id(body: ChunkIdRequest, request: Request):
        """Tra các chunk chính sách theo mã trong request REST."""
        return await fetch_chunks(request.app.state.rag, "get", body.chunk_ids)

    return app


app = create_app()
