"""Phần không cần mạng của gateway_smoke: đọc SSE MCP, chấm đạt/không, chuỗi gọi MCP có session."""

import asyncio
import json

import httpx
import pytest

from scripts.evidence.gateway_smoke import (
    grade,
    mcp_agent2_tools,
    mcp_calls,
    mcp_denied,
    policy_table,
    sse_json,
)


def test_sse_json_takes_last_data_line_or_plain_json():
    """SSE nhiều dòng data ⇒ lấy dòng cuối; JSON thường vẫn đọc được."""
    assert sse_json('event: message\ndata: {"a": 1}\n\ndata: {"a": 2}\n') == {"a": 2}
    assert sse_json('{"a": 3}') == {"a": 3}


@pytest.mark.parametrize(
    ("got", "want", "ok"),
    [
        (401, 401, True),
        (200, 401, False),
        (True, True, True),
        (False, True, False),
        (429, [200, 429], True),
    ],
)
def test_grade_marks_each_row_and_fails_on_any(got, want, ok):
    """Mỗi dòng có cờ ok; một dòng hỏng là cả lượt hỏng."""
    rows = [{"check": "x", "got": got, "want": want}]
    assert grade(rows) is ok and rows[0]["ok"] is ok
    assert (
        grade([{"check": "a", "got": 1, "want": 1}, {"check": "b", "got": 2, "want": 1}]) is False
    )


def _gateway(features, structured=True):
    """MCP giả: initialize trả session; mọi lời gọi sau phải mang đúng session."""

    def handle(request):
        body = json.loads(request.content)
        if body["method"] == "initialize":
            return httpx.Response(200, headers={"mcp-session-id": "s1"}, json={"result": {}})
        assert request.headers["mcp-session-id"] == "s1"
        if body["method"] == "tools/list":
            return httpx.Response(
                200, text='data: {"result": {"tools": [{"name": "get_sku_features"}]}}\n'
            )
        if body["method"] == "tools/call":
            payload = {"features": [features]}
            result = (
                {"structuredContent": payload}
                if structured
                else {"content": [{"type": "text", "text": json.dumps(payload)}]}
            )
            return httpx.Response(200, text=f"data: {json.dumps({'result': result})}\n")
        return httpx.Response(202)

    return httpx.AsyncClient(base_url="http://gw", transport=httpx.MockTransport(handle))


@pytest.mark.parametrize("structured", [True, False])
def test_mcp_calls_reports_null_features_as_missing(structured):
    """Mang session sau initialize; feature toàn null (Redis rỗng) bị báo là thiếu."""

    async def go(features):
        async with _gateway(features, structured) as client:
            return await mcp_calls(client, "k", "APP-00042")

    assert asyncio.run(go({"seller_sku": "APP-00042", "returns_30d": 3})) == (
        200,
        ["get_sku_features"],
        True,
    )
    # Redis rỗng: feature toàn null ⇒ phải báo KHÔNG có giá trị (lỗi im lặng).
    assert asyncio.run(go({"seller_sku": "APP-00042", "returns_30d": None}))[2] is False


def _denying_gateway(call_result):
    """MCP giả: initialize có session, tools/call trả `call_result` (JSON-RPC)."""

    def handle(request):
        body = json.loads(request.content)
        if body["method"] == "initialize":
            return httpx.Response(200, headers={"mcp-session-id": "s1"}, json={"result": {}})
        if body["method"] == "tools/call":
            return httpx.Response(200, text=f"data: {json.dumps(call_result)}\n")
        return httpx.Response(202)

    return httpx.AsyncClient(base_url="http://gw", transport=httpx.MockTransport(handle))


@pytest.mark.parametrize(
    ("reply", "blocked"),
    [
        ({"error": {"code": -32602, "message": "tool not allowed"}}, True),
        ({"result": {"isError": True, "content": []}}, True),
        ({"result": {"content": [{"type": "text", "text": "ok"}]}}, False),
    ],
)
def test_mcp_denied_detects_blocked_tool(reply, blocked):
    """Tool ngoài danh sách: lỗi JSON-RPC hoặc isError là bị chặn; trả kết quả thường là KHÔNG chặn."""

    async def go():
        async with _denying_gateway(reply) as client:
            return await mcp_denied(client, "k")

    got, detail = asyncio.run(go())
    assert got is blocked and detail.startswith("HTTP 200")


def test_policy_table_labels_allow_and_deny_per_policy():
    """Mỗi dòng có policy, ALLOW/DENY kỳ vọng và PASS/FAIL; dòng Prometheus bị bỏ."""
    rows = [
        {"check": "LLM không key", "got": 401, "want": 401, "ok": True},
        {"check": "LLM có key", "got": 200, "want": 200, "ok": True},
        {"check": "MCP tool ngoài danh sách (x) bị chặn", "got": False, "want": True, "ok": False},
        {"check": "A2A card không key (model-agent)", "got": 401, "want": 401, "ok": True},
        {"check": "Prometheus llm_tokens > 0", "got": True, "want": True, "ok": True},
    ]
    table = policy_table(rows)
    assert "| API key | LLM không key | DENY | 401 | PASS |" in table
    assert "| API key | LLM có key | ALLOW | 200 | PASS |" in table
    assert (
        "| mcpAuthorization | MCP tool ngoài danh sách (x) bị chặn | DENY | False | FAIL |" in table
    )
    assert "| API key | A2A card không key (model-agent) | DENY | 401 | PASS |" in table
    assert "Prometheus" not in table


def _agent2_gateway(bodies):
    """MCP giả: tools/call trả structuredContent theo tên tool (None = tool báo isError)."""

    def handle(request):
        body = json.loads(request.content)
        if body["method"] == "initialize":
            return httpx.Response(200, headers={"mcp-session-id": "s1"}, json={"result": {}})
        if body["method"] == "tools/call":
            payload = bodies[body["params"]["name"]]
            result = (
                {"isError": True, "content": []}
                if payload is None
                else {"structuredContent": payload}
            )
            return httpx.Response(200, text=f"data: {json.dumps({'result': result})}\n")
        return httpx.Response(202)

    return httpx.AsyncClient(base_url="http://gw", transport=httpx.MockTransport(handle))


@pytest.mark.parametrize(
    ("predicted", "chunks", "want"),
    [
        ({"score": 0.17, "model_version": "1", "abstain": False}, [{"chunk_id": "a"}], (True, 1)),
        # Abstain (không có score) hoặc thiếu model_version không được tính là dự đoán thành công.
        ({"score": None, "model_version": "1", "abstain": True}, [], (False, 0)),
        ({"score": 0.17, "model_version": None, "abstain": False}, [{"chunk_id": "a"}], (False, 1)),
        (None, None, (False, 0)),
    ],
)
def test_mcp_agent2_tools_scores_prediction_and_counts_chunks(predicted, chunks, want):
    """Dự đoán chỉ đạt khi có score và model_version; tool lỗi cho kết quả rỗng, không làm smoke sập."""
    bodies = {
        "predict_return_risk": predicted,
        "search_policy_chunks": None if chunks is None else {"chunks": chunks},
    }

    async def go():
        async with _agent2_gateway(bodies) as client:
            return await mcp_agent2_tools(client, "k", "APP-00042")

    assert asyncio.run(go()) == want
