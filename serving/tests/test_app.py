"""Test Web API feature: store Feast giả (mock), không cần Redis.

Chạy: `uv run --no-project --with-requirements serving/requirements-dev.txt \
    pytest serving/tests --cov=serving --cov-report=term-missing`
"""

import json
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from fastapi.testclient import TestClient
from hypothesis import given, settings
from hypothesis import strategies as st

from serving.app import MAX_SKUS, create_app

# Giá trị giả theo SKU; SKU không có trong đây = Redis chưa có → None.
ORDER = [
    "labeled_count_90d",
    "positive_count_90d",
    "return_or_refund_rate_90d",
    "approved_returns_24h",
]
KNOWN = {
    "sku-00001": (10, 3, 0.3, 2),
    "sku-00002": (0, 0, 0.0, 0),
    "sku-00003": (15306, 2599, 0.16980269175486737, None),
}


def fake_online_features(features, entity_rows):
    """Bắt chước OnlineResponse.to_dict() của Feast: dict cột → list, cùng thứ tự entity_rows."""
    skus = [row["seller_sku"] for row in entity_rows]
    columns = {"seller_sku": skus}
    for feature in features:
        name = feature.split(":")[1]
        i = ORDER.index(name)
        columns[name] = [KNOWN[s][i] if s in KNOWN else None for s in skus]
    response = MagicMock()
    response.to_dict.return_value = columns
    return response


def make_store():
    fake = MagicMock()
    fake.get_online_features_async = AsyncMock(side_effect=fake_online_features)
    return fake


@pytest.fixture
def store():
    return make_store()


@pytest.fixture
def client(store):
    with TestClient(create_app(store_factory=lambda: store)) as c:
        yield c


def test_healthz(client):
    assert client.get("/healthz").json() == {"status": "ok"}


def test_readyz(client):
    assert client.get("/readyz").status_code == 200


def test_readyz_not_loaded():
    with TestClient(create_app(store_factory=lambda: None)) as c:
        assert c.get("/readyz").status_code == 503


def test_features_known_and_missing(client):
    body = client.post("/features", json={"seller_skus": ["sku-00001", "sku-99999"]}).json()
    assert body["features"] == [
        {
            "seller_sku": "sku-00001",
            "labeled_count_90d": 10,
            "positive_count_90d": 3,
            "return_or_refund_rate_90d": 0.3,
            "approved_returns_24h": 2,
            "batch_status": "ready",
            "stream_status": "ready",
            "batch_release": None,
            "batch_cutoff": None,
        },
        {
            "seller_sku": "sku-99999",
            "labeled_count_90d": None,
            "positive_count_90d": None,
            "return_or_refund_rate_90d": None,
            "approved_returns_24h": None,
            "batch_status": "not_available",
            "stream_status": "not_available",
            "batch_release": None,
            "batch_cutoff": None,
        },
    ]


def test_duplicates_sent_once(client, store):
    client.post("/features", json={"seller_skus": ["sku-00001", "sku-00001"]})
    rows = store.get_online_features_async.call_args.kwargs["entity_rows"]
    assert rows == [{"seller_sku": "sku-00001"}]


def test_one_view_missing_gives_nulls(client, store):
    def daily_only(features, entity_rows):
        if features[0].startswith("sku_return_24h"):
            raise KeyError("Feature view sku_return_24h does not exist")
        return fake_online_features(features, entity_rows)

    store.get_online_features_async.side_effect = daily_only
    resp = client.post("/features", json={"seller_skus": ["sku-00001"]})
    assert resp.status_code == 200
    row = resp.json()["features"][0]
    assert row["labeled_count_90d"] == 10
    assert row["approved_returns_24h"] is None


def test_store_down_returns_503(client, store):
    store.get_online_features_async.side_effect = ConnectionError("redis down")
    resp = client.post("/features", json={"seller_skus": ["sku-00001"]})
    assert resp.status_code == 503
    assert resp.json() == {"detail": "online store unavailable"}


# Equivalence partitioning + boundary value: số SKU mỗi request.
# Lớp hợp lệ [1, MAX_SKUS]; hai lớp sai: rỗng và quá MAX_SKUS. Biên: 0, 1, MAX, MAX+1.
@pytest.mark.parametrize(
    ("count", "status"),
    [(0, 422), (1, 200), (50, 200), (MAX_SKUS, 200), (MAX_SKUS + 1, 422)],
)
def test_sku_count_boundaries(client, count, status):
    skus = [f"sku-{i:05d}" for i in range(count)]
    assert client.post("/features", json={"seller_skus": skus}).status_code == status


# Độ dài và ký tự của một SKU. Biên độ dài: 0, 1, 64, 65.
@pytest.mark.parametrize(
    ("sku", "status"),
    [
        ("", 422),
        ("a", 200),
        ("a" * 64, 200),
        ("a" * 65, 422),
        ("sku-00001", 200),
        ("Shop_A.sku-1", 200),
        ("sku 00001", 422),
        ("sku/../x", 422),
        ("sku';DROP", 422),
    ],
)
def test_sku_format(client, sku, status):
    assert client.post("/features", json={"seller_skus": [sku]}).status_code == status


@pytest.mark.parametrize("body", [{}, {"seller_skus": "sku-00001"}, {"seller_skus": [1]}])
def test_bad_body(client, body):
    assert client.post("/features", json=body).status_code == 422


# Property-based: gọi lại cùng request ra cùng kết quả (idempotent), và lặp SKU không đổi kết quả.
sku_lists = st.lists(
    st.sampled_from(["sku-00001", "sku-00002", "sku-00003"]), min_size=1, max_size=20
)


@settings(max_examples=50, deadline=None)
@given(skus=sku_lists)
def test_idempotent(skus):
    with TestClient(create_app(store_factory=make_store)) as c:
        first = c.post("/features", json={"seller_skus": skus}).json()
        second = c.post("/features", json={"seller_skus": skus}).json()
        doubled = c.post("/features", json={"seller_skus": skus + skus}).json()
    assert first == second == doubled
    assert [r["seller_sku"] for r in first["features"]] == list(dict.fromkeys(skus))


# MCP tool: cùng logic, gọi qua JSON-RPC (streamable HTTP, stateless, trả JSON).
MCP_HEADERS = {"Accept": "application/json, text/event-stream"}


def call_tool(client, arguments):
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": "get_sku_features", "arguments": arguments},
    }
    return client.post("/mcp/", json=body, headers=MCP_HEADERS).json()["result"]


def test_mcp_lists_tool(client):
    body = {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}
    tools = client.post("/mcp/", json=body, headers=MCP_HEADERS).json()["result"]["tools"]
    assert sorted(t["name"] for t in tools) == [
        "get_policy_chunks",
        "get_sku_features",
        "predict_return_risk",
        "search_policy_chunks",
    ]
    # Mô tả tool tiếng Việt làm Qwen 1,5B bỏ qua tool (đo trên EKS 30/10: 0/3, tiếng Anh 3/3).
    assert all(t["description"].isascii() for t in tools)


def test_mcp_tool_returns_features(client):
    result = call_tool(client, {"seller_skus": ["sku-00001"]})
    assert result["isError"] is False
    assert result["structuredContent"]["features"][0]["labeled_count_90d"] == 10


@pytest.mark.parametrize(
    ("skus", "message"),
    [([], "invalid seller_skus"), (["x y"], "invalid seller_skus"), (["a" * 65], "invalid")],
)
def test_mcp_tool_rejects_bad_input(client, skus, message):
    result = call_tool(client, {"seller_skus": skus})
    assert result["isError"] is True
    assert message in result["content"][0]["text"]


def test_mcp_tool_store_down(client, store):
    store.get_online_features_async.side_effect = ConnectionError("redis down")
    result = call_tool(client, {"seller_skus": ["sku-00001"]})
    assert result["isError"] is True
    assert "online store unavailable" in result["content"][0]["text"]


def test_mcp_server_identity(client):
    """Agent thấy tên + hướng dẫn này khi kết nối; đổi là đổi hợp đồng với Agent."""
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "t", "version": "0"},
        },
    }
    result = client.post("/mcp/", json=body, headers=MCP_HEADERS).json()["result"]
    assert result["serverInfo"]["name"] == "platform-features"
    assert result["instructions"] == (
        "Tra feature tỷ lệ trả hàng theo SKU, chấm điểm rủi ro Return cho đơn mới"
        " và tìm đoạn chính sách trả hàng."
    )


def test_openapi_title(client):
    assert client.get("/openapi.json").json()["info"]["title"] == "Feature API"


# Tìm chunk RAG: rag giả trả kết quả cố định, ghi lại lời gọi.
CHUNKS = [
    {
        "chunk_id": "a",
        "doc_id": "walmart-returns",
        "heading": "Return window",
        "text": "30 days",
        "distance": 0.2,
    },
    {
        "chunk_id": "b",
        "doc_id": "amazon-returns",
        "heading": "Return window",
        "text": "30 days",
        "distance": 0.4,
    },
]


class FakeRag:
    def __init__(self, error=None):
        self.calls, self.error = [], error

    def search(self, query, top_k):
        self.calls.append((query, top_k))
        if self.error:
            raise self.error
        return CHUNKS[:top_k]

    def get(self, chunk_ids):
        self.calls.append(tuple(chunk_ids))
        known = {"a" * 16: CHUNKS[0]}
        return [
            {k: v for k, v in known[c].items() if k != "distance"} | {"chunk_id": c}
            if c in known
            else {"chunk_id": c, "doc_id": None, "heading": None, "text": None}
            for c in chunk_ids
        ]


@pytest.fixture
def rag():
    return FakeRag()


@pytest.fixture
def rag_client(store, rag):
    with TestClient(create_app(store_factory=lambda: store, rag_factory=lambda: rag)) as c:
        yield c


def test_chunks_returns_ranked_chunks(rag_client, rag):
    body = rag_client.post("/chunks", json={"query": "Walmart return window?"}).json()
    assert [c["doc_id"] for c in body["chunks"]] == ["walmart-returns", "amazon-returns"]
    assert body["chunks"][0]["chunk_id"] == "a"
    assert rag.calls == [("Walmart return window?", 3)]


def test_chunks_not_configured(client):
    resp = client.post("/chunks", json={"query": "Walmart return window?"})
    assert resp.status_code == 503
    assert resp.json() == {"detail": "rag not configured"}


def test_chunks_store_down():
    with TestClient(
        create_app(store_factory=make_store, rag_factory=lambda: FakeRag(ConnectionError("pg")))
    ) as c:
        resp = c.post("/chunks", json={"query": "Walmart return window?"})
    assert resp.status_code == 503
    assert resp.json() == {"detail": "vector store unavailable"}


# Boundary value: query 3–500 ký tự, top_k 1–10.
@pytest.mark.parametrize(
    ("query", "top_k", "status"),
    [
        ("ab", 3, 422),
        ("abc", 3, 200),
        ("a" * 500, 3, 200),
        ("a" * 501, 3, 422),
        ("abc", 0, 422),
        ("abc", 1, 200),
        ("abc", 10, 200),
        ("abc", 11, 422),
    ],
)
def test_chunks_boundaries(rag_client, query, top_k, status):
    resp = rag_client.post("/chunks", json={"query": query, "top_k": top_k})
    assert resp.status_code == status


def test_mcp_search_tool(rag_client, rag):
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": "search_policy_chunks", "arguments": {"query": "Walmart?", "top_k": 1}},
    }
    result = rag_client.post("/mcp/", json=body, headers=MCP_HEADERS).json()["result"]
    assert result["isError"] is False
    assert result["structuredContent"]["chunks"][0]["doc_id"] == "walmart-returns"
    assert rag.calls == [("Walmart?", 1)]


@pytest.mark.parametrize(
    ("arguments", "message"),
    [({"query": "ab"}, "invalid input"), ({"query": "Walmart?", "top_k": 11}, "invalid input")],
)
def test_mcp_search_tool_bad_input(rag_client, arguments, message):
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": "search_policy_chunks", "arguments": arguments},
    }
    result = rag_client.post("/mcp/", json=body, headers=MCP_HEADERS).json()["result"]
    assert result["isError"] is True
    assert message in result["content"][0]["text"]


def test_mcp_search_tool_not_configured(client):
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": "search_policy_chunks", "arguments": {"query": "Walmart?"}},
    }
    result = client.post("/mcp/", json=body, headers=MCP_HEADERS).json()["result"]
    assert result["isError"] is True
    assert "rag not configured" in result["content"][0]["text"]


def test_load_rag_disabled_without_env(monkeypatch):
    from serving.app import load_rag

    monkeypatch.delenv("FEAST_RAG_REGISTRY", raising=False)
    assert load_rag() is None


def test_chunks_by_id_known_and_missing(rag_client, rag):
    ids = ["a" * 16, "b" * 16]
    body = rag_client.post("/chunks/by-id", json={"chunk_ids": ids}).json()
    assert [c["doc_id"] for c in body["chunks"]] == ["walmart-returns", None]
    assert body["chunks"][0]["distance"] is None
    assert rag.calls == [tuple(ids)]


# chunk_id đúng 16 ký tự hex thường; 1–20 id mỗi request.
@pytest.mark.parametrize(
    ("ids", "status"),
    [
        ([], 422),
        (["a" * 16], 200),
        (["a" * 15], 422),
        (["a" * 17], 422),
        (["A" * 16], 422),
        (["g" * 16], 422),
        (["a" * 16] * 20, 200),
        (["a" * 16] * 21, 422),
    ],
)
def test_chunks_by_id_boundaries(rag_client, ids, status):
    assert rag_client.post("/chunks/by-id", json={"chunk_ids": ids}).status_code == status


def test_mcp_get_policy_chunks(rag_client):
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": "get_policy_chunks", "arguments": {"chunk_ids": ["a" * 16]}},
    }
    result = rag_client.post("/mcp/", json=body, headers=MCP_HEADERS).json()["result"]
    assert result["isError"] is False
    assert result["structuredContent"]["chunks"][0]["heading"] == "Return window"


def test_mcp_get_policy_chunks_bad_id(rag_client):
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": "get_policy_chunks", "arguments": {"chunk_ids": ["nope"]}},
    }
    result = rag_client.post("/mcp/", json=body, headers=MCP_HEADERS).json()["result"]
    assert result["isError"] is True
    assert "invalid chunk_ids" in result["content"][0]["text"]


# Dự đoán: KServe giả bằng httpx.MockTransport, ghi lại body đã gửi.
KSERVE = "http://kserve.test/v1/models/return-risk:predict"
ORDER_LINE = {
    "seller_sku": "sku-00002",
    "channel": "tiktok_shop",
    "country": "GB",
    "ordered_basis_qty": 3,
    "ordered_product_net_minor": 30000,
}


def kserve(status=200, payload=None):
    sent = []

    def handler(request):
        sent.append((str(request.url), request.read()))
        body = payload or {
            "predictions": [
                {
                    "seller_sku": "sku-00002",
                    "score": 0.71,
                    "abstain": False,
                    "reason": None,
                    "model_version": "3",
                }
            ]
        }
        return httpx.Response(status, json=body)

    return sent, lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler))


@pytest.fixture
def predict_client(store, monkeypatch):
    monkeypatch.setenv("KSERVE_URL", KSERVE)
    sent, factory = kserve()
    with TestClient(create_app(store_factory=lambda: store, http_factory=factory)) as c:
        yield c, sent


def test_predict_sends_v1_instance(predict_client):
    client, sent = predict_client
    body = client.post("/predict", json=ORDER_LINE).json()
    assert body["score"] == 0.71 and body["model_version"] == "3"
    url, raw = sent[0]
    assert url == KSERVE
    instance = json.loads(raw)["instances"][0]
    assert instance == ORDER_LINE


def test_long_decimals_rounded_for_pii_guardrail(store, monkeypatch):
    """0.16980269175486737 bị guardrail phoneNumber của gateway che thành <PHONE_NUMBER> (30/10) ⇒ làm tròn 4 số."""
    monkeypatch.setenv("KSERVE_URL", KSERVE)
    prediction = {
        "seller_sku": "sku-00003",
        "score": 0.1661234567891234,
        "abstain": False,
        "reason": None,
        "model_version": "1",
    }
    _, factory = kserve(payload={"predictions": [prediction]})
    with TestClient(create_app(store_factory=lambda: store, http_factory=factory)) as c:
        rate = c.post("/features", json={"seller_skus": ["sku-00003"]}).json()["features"][0]
        score = c.post("/predict", json=ORDER_LINE).json()["score"]
    assert (rate["return_or_refund_rate_90d"], score) == (0.1698, 0.1661)


def test_predict_rejects_missing_country(predict_client):
    """Không tự điền feature: thiếu country thì 422, không gọi model."""
    client, sent = predict_client
    body = {k: v for k, v in ORDER_LINE.items() if k != "country"}
    assert client.post("/predict", json=body).status_code == 422
    assert not sent


def test_predict_not_configured(client, monkeypatch):
    monkeypatch.delenv("KSERVE_URL", raising=False)
    resp = client.post("/predict", json=ORDER_LINE)
    assert resp.status_code == 503
    assert resp.json() == {"detail": "model not configured"}


@pytest.mark.parametrize(
    ("status", "payload"),
    [(500, {"error": "boom"}), (200, {"predictions": []}), (200, {"nope": 1})],
)
def test_predict_bad_upstream(store, monkeypatch, status, payload):
    monkeypatch.setenv("KSERVE_URL", KSERVE)
    _, factory = kserve(status, payload)
    with TestClient(create_app(store_factory=lambda: store, http_factory=factory)) as c:
        resp = c.post("/predict", json=ORDER_LINE)
    assert resp.status_code == 503
    assert resp.json() == {"detail": "model unavailable"}


# Equivalence partitioning: channel (5 hợp lệ / ngoài danh sách); boundary: qty 1–1000, net ≥ 0.
@pytest.mark.parametrize(
    ("change", "status"),
    [
        ({"channel": "amazon"}, 200),
        ({"channel": "shopee"}, 200),
        ({"channel": "ebay"}, 422),
        ({"ordered_basis_qty": 0}, 422),
        ({"ordered_basis_qty": 1}, 200),
        ({"ordered_basis_qty": 1000}, 200),
        ({"ordered_basis_qty": 1001}, 422),
        ({"ordered_product_net_minor": -1}, 422),
        ({"ordered_product_net_minor": 0}, 200),
        ({"seller_sku": "sku 1"}, 422),
    ],
)
def test_predict_validation(predict_client, change, status):
    client, _ = predict_client
    assert client.post("/predict", json=ORDER_LINE | change).status_code == status


def test_mcp_predict_tool(predict_client):
    client, _ = predict_client
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": "predict_return_risk", "arguments": ORDER_LINE},
    }
    result = client.post("/mcp/", json=body, headers=MCP_HEADERS).json()["result"]
    assert result["isError"] is False
    assert result["structuredContent"]["score"] == 0.71


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"channel": "ebay"}, "invalid channel"),
        ({"ordered_basis_qty": 0}, "invalid ordered_basis_qty"),
    ],
)
def test_mcp_predict_tool_bad_input(predict_client, change, message):
    client, _ = predict_client
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": "predict_return_risk", "arguments": ORDER_LINE | change},
    }
    result = client.post("/mcp/", json=body, headers=MCP_HEADERS).json()["result"]
    assert result["isError"] is True
    assert message in result["content"][0]["text"]


def test_mcp_predict_tool_not_configured(client, monkeypatch):
    monkeypatch.delenv("KSERVE_URL", raising=False)
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": "predict_return_risk", "arguments": ORDER_LINE},
    }
    result = client.post("/mcp/", json=body, headers=MCP_HEADERS).json()["result"]
    assert result["isError"] is True
    assert "model not configured" in result["content"][0]["text"]


def test_load_rag_wires_store_and_cache(monkeypatch):
    """load_rag phải truyền đúng store, câu hỏi, top_k, cache model và id xuống rag.pipeline."""
    import rag.pipeline as pipeline
    from serving.app import load_rag

    store, calls = object(), []
    monkeypatch.setenv("FEAST_RAG_REGISTRY", "s3://x/rag-registry.db")
    monkeypatch.setenv("RAG_MODEL_CACHE", "/models")
    monkeypatch.setattr(pipeline, "_store", lambda: store)
    monkeypatch.setattr(
        pipeline,
        "search",
        lambda s, q, top_k, cache_dir: calls.append(("search", s, q, top_k, cache_dir)) or ["hit"],
    )
    monkeypatch.setattr(
        pipeline, "get_chunks", lambda s, ids: calls.append(("get", s, ids)) or ["chunk"]
    )
    rag = load_rag()
    assert rag.search("Walmart?", 2) == ["hit"]
    assert rag.get(["a" * 16]) == ["chunk"]
    assert calls == [("search", store, "Walmart?", 2, "/models"), ("get", store, ["a" * 16])]


def test_predict_uses_10s_timeout(store, monkeypatch):
    monkeypatch.setenv("KSERVE_URL", KSERVE)
    seen = []

    def handler(request):
        seen.append(request.extensions["timeout"])
        return httpx.Response(
            200,
            json={
                "predictions": [
                    {
                        "seller_sku": "s",
                        "score": 0.1,
                        "abstain": False,
                        "reason": None,
                        "model_version": "1",
                    }
                ]
            },
        )

    factory = lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler))  # noqa: E731
    with TestClient(create_app(store_factory=lambda: store, http_factory=factory)) as c:
        assert c.post("/predict", json=ORDER_LINE).status_code == 200
    assert seen[0]["read"] == 10


def test_metrics_count_requests_by_handler_and_status(client):
    client.post("/features", json={"seller_skus": ["sku-00001"]})
    client.post("/features", json={"seller_skus": []})
    text = client.get("/metrics").text
    assert 'http_requests_total{handler="/features",method="POST",status="200"}' in text
    assert 'http_requests_total{handler="/features",method="POST",status="422"}' in text
    assert 'http_request_duration_seconds_count{handler="/features"}' in text


def test_metrics_group_mcp_under_one_handler(client):
    client.post(
        "/mcp/", json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"}, headers=MCP_HEADERS
    )
    assert (
        'http_requests_total{handler="/mcp",method="POST",status="200"}'
        in client.get("/metrics").text
    )


def test_tool_calls_counted_ok_and_error(client):
    def tool_count(text, outcome):
        line = f'mcp_tool_calls_total{{outcome="{outcome}",tool="get_sku_features"}}'
        return next((float(x.split()[-1]) for x in text.splitlines() if x.startswith(line)), 0.0)

    before = client.get("/metrics").text
    call_tool(client, {"seller_skus": ["sku-00001"]})
    call_tool(client, {"seller_skus": []})
    after = client.get("/metrics").text
    assert tool_count(after, "ok") - tool_count(before, "ok") == 1
    assert tool_count(after, "error") - tool_count(before, "error") == 1


def test_partial_batch_failure_preserves_stream_status(client, store):
    """Nhóm batch lỗi không chặn stream hợp lệ, không biến dữ liệu thiếu thành zero."""

    def read(features, entity_rows):
        """Giả lập registry/batch hỏng nhưng stream vẫn tra được."""
        if features[0].startswith("sku_return_daily:"):
            raise RuntimeError("batch unavailable")
        return fake_online_features(features, entity_rows)

    store.get_online_features_async.side_effect = read
    response = client.post("/features", json={"seller_skus": ["sku-00001"]})
    row = response.json()["features"][0]
    assert response.status_code == 200
    assert row["batch_status"] == "unavailable"
    assert row["labeled_count_90d"] is None
    assert row["stream_status"] == "ready" and row["approved_returns_24h"] == 2


def test_zero_history_and_zero_stream_are_not_system_failure(client):
    """Có số zero được xác nhận khác với null và lỗi store."""
    row = client.post("/features", json={"seller_skus": ["sku-00002"]}).json()["features"][0]
    assert row["batch_status"] == "insufficient_history"
    assert row["stream_status"] == "ready" and row["approved_returns_24h"] == 0


def test_published_snapshot_distinguishes_missing_expected_sku(client, store):
    """SKU có trong bản công bố mà thiếu online là unavailable; SKU ngoài bản chỉ là not_available."""
    from feature_repo.publication import FeatureResult, Release

    release = Release("release-a", 1, "registry", "offline", "2026-06-30", frozenset({"sku-00001"}))

    def read(features, entity_rows):
        """Batch trả null cho mọi SKU, kèm phiếu đã kiểm tra."""
        if features[0].startswith("sku_return_daily:"):
            return FeatureResult(
                {
                    "seller_sku": [r["seller_sku"] for r in entity_rows],
                    **{f.split(":")[1]: [None] * len(entity_rows) for f in features},
                },
                release,
            )
        return fake_online_features(features, entity_rows)

    store.get_online_features_async.side_effect = read
    rows = client.post("/features", json={"seller_skus": ["sku-00001", "sku-99999"]}).json()[
        "features"
    ]
    assert [r["batch_status"] for r in rows] == ["unavailable", "not_available"]
    assert all(
        r["batch_release"] == "release-a" and r["batch_cutoff"] == "2026-06-30" for r in rows
    )


def test_predict_forwards_unavailable_without_inventing_score(store, monkeypatch):
    """HTTP API giữ nguyên null/reason của predictor để agent không thấy điểm giả."""
    monkeypatch.setenv("KSERVE_URL", KSERVE)
    payload = {
        "predictions": [
            {
                "seller_sku": "sku-00002",
                "score": None,
                "abstain": True,
                "reason": "feature_data_unavailable",
                "model_version": "3",
                "batch_release": "a",
                "batch_cutoff": "2026-06-30",
            }
        ]
    }
    _, factory = kserve(payload=payload)
    with TestClient(create_app(store_factory=lambda: store, http_factory=factory)) as c:
        result = c.post("/predict", json=ORDER_LINE).json()
    assert result == payload["predictions"][0]


@pytest.mark.parametrize(
    ("surface", "features", "mcp", "health"),
    [("rest", 200, 404, 200), ("mcp", 404, 200, 200), ("all", 200, 200, 200)],
)
def test_surface_splits_rest_and_mcp_pods(store, surface, features, mcp, health):
    """Pod REST không phơi /mcp, pod MCP không phơi REST; cả hai giữ probe."""
    app = create_app(store_factory=lambda: store, surface=surface)
    with TestClient(app) as c:
        assert c.post("/features", json={"seller_skus": ["sku-00001"]}).status_code == features
        body = {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}
        assert c.post("/mcp/", json=body, headers=MCP_HEADERS).status_code == mcp
        assert c.get("/readyz").status_code == health


def test_surface_from_env_and_rejects_unknown(store, monkeypatch):
    monkeypatch.setenv("API_SURFACE", "mcp")
    with TestClient(create_app(store_factory=lambda: store)) as c:
        assert c.post("/features", json={"seller_skus": ["sku-00001"]}).status_code == 404
    with pytest.raises(ValueError, match="API_SURFACE"):
        create_app(store_factory=lambda: store, surface="both")
