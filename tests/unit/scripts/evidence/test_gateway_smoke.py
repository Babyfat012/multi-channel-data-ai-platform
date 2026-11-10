"""The network-free parts of gateway_smoke: MCP SSE parsing, pass/fail grading, the session-bearing MCP call chain."""

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
    """SSE with several data lines ⇒ take the last one; plain JSON still parses."""
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
    """Each row gets an ok flag; one failed row fails the whole run."""
    rows = [{"check": "x", "got": got, "want": want}]
    assert grade(rows) is ok and rows[0]["ok"] is ok
    assert (
        grade([{"check": "a", "got": 1, "want": 1}, {"check": "b", "got": 2, "want": 1}]) is False
    )


def _gateway(features, structured=True):
    """Fake MCP: initialize returns a session; every later call must carry that session."""

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
    """Carries the session after initialize; all-null features (empty Redis) are reported as missing."""

    async def go(features):
        async with _gateway(features, structured) as client:
            return await mcp_calls(client, "k", "APP-00042")

    assert asyncio.run(go({"seller_sku": "APP-00042", "returns_30d": 3})) == (
        200,
        ["get_sku_features"],
        True,
    )
    # Empty Redis: all-null features ⇒ must report NO values (a silent failure).
    assert asyncio.run(go({"seller_sku": "APP-00042", "returns_30d": None}))[2] is False


def _denying_gateway(call_result):
    """Fake MCP: initialize has a session, tools/call returns `call_result` (JSON-RPC)."""

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
    """Tool not in allowlist: a JSON-RPC error or isError means blocked; a normal result means NOT blocked."""

    async def go():
        async with _denying_gateway(reply) as client:
            return await mcp_denied(client, "k")

    got, detail = asyncio.run(go())
    assert got is blocked and detail.startswith("HTTP 200")


def test_policy_table_labels_allow_and_deny_per_policy():
    """Each row has a policy, expected ALLOW/DENY and PASS/FAIL; Prometheus rows are skipped."""
    rows = [
        {"check": "LLM without key", "got": 401, "want": 401, "ok": True},
        {"check": "LLM with key", "got": 200, "want": 200, "ok": True},
        {"check": "MCP tool not in allowlist (x) blocked", "got": False, "want": True, "ok": False},
        {"check": "A2A card without key (model-agent)", "got": 401, "want": 401, "ok": True},
        {"check": "Prometheus llm_tokens > 0", "got": True, "want": True, "ok": True},
    ]
    table = policy_table(rows)
    assert "| API key | LLM without key | DENY | 401 | PASS |" in table
    assert "| API key | LLM with key | ALLOW | 200 | PASS |" in table
    assert (
        "| mcpAuthorization | MCP tool not in allowlist (x) blocked | DENY | False | FAIL |"
        in table
    )
    assert "| API key | A2A card without key (model-agent) | DENY | 401 | PASS |" in table
    assert "Prometheus" not in table


def _agent2_gateway(bodies):
    """Fake MCP: tools/call returns structuredContent by tool name (None = tool reports isError)."""

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
        # Abstain (no score) or a missing model_version does not count as a successful prediction.
        ({"score": None, "model_version": "1", "abstain": True}, [], (False, 0)),
        ({"score": 0.17, "model_version": None, "abstain": False}, [{"chunk_id": "a"}], (False, 1)),
        (None, None, (False, 0)),
    ],
)
def test_mcp_agent2_tools_scores_prediction_and_counts_chunks(predicted, chunks, want):
    """A prediction passes only with score and model_version; a failing tool yields an empty result without crashing the smoke."""
    bodies = {
        "predict_return_risk": predicted,
        "search_policy_chunks": None if chunks is None else {"chunks": chunks},
    }

    async def go():
        async with _agent2_gateway(bodies) as client:
            return await mcp_agent2_tools(client, "k", "APP-00042")

    assert asyncio.run(go()) == want
