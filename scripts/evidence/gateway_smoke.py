"""Check agentgateway (LLM / MCP / A2A) + telemetry in Prometheus, write a JSON receipt.

Replaces the hand-typed `curl` table in docs/ml-llm-platform/reference/llm_platform_agents.md. Run on a laptop via port-forward:

kubectl -n agentgateway port-forward svc/agentgateway 18300:3000 &
kubectl -n observability port-forward svc/prometheus-server 19090:80 &
export AGW_AGENT_KEY=$(kubectl -n agentgateway get secret agentgateway-keys -o jsonpath='{.data.AGW_AGENT_KEY}' | base64 -d)
python scripts/evidence/gateway_smoke.py --out docs/evidence/aws/gateway-smoke-<date>.json

The key is read only from the environment and never printed. Exits with code 1 if any row fails.
"""

import argparse
import asyncio
import json
import os
import sys
import time

import httpx

MODEL = "qwen2.5-1.5b-instruct"
MCP_ACCEPT = {"Accept": "application/json, text/event-stream"}
# Tool not in the `mcpAuthorization` list of agentgateway.yaml.
DENIED_TOOL = "delete_all_features"
# One with-key / without-key pair per Agent ⇒ the dashboard splits success and failure per Agent route.
AGENTS = ("data-agent", "model-agent", "coordinator")
# Prometheus scrapes every 15 s ⇒ wait more than one cycle before querying.
SCRAPE_WAIT_S = 20
QUERIES = {
    "llm_tokens": "sum(agentgateway_gen_ai_client_token_usage_sum)",
    "ttft_requests": "sum(agentgateway_gen_ai_server_time_to_first_token_count)",
    "mcp_requests": "sum(agentgateway_mcp_requests_total)",
    "a2a_requests": 'sum(agentgateway_requests_total{route=~"default/a2a-.*"})',  # route has a namespace prefix
    "pii_checks": "sum(agentgateway_guardrail_checks_total)",
    # The llama.cpp gauge drops to 0 on scrapes with no new tokens ⇒ take the 5-minute peak.
    "llamacpp_tokens_per_s": "max(max_over_time(llamacpp:predicted_tokens_seconds[5m]))",
}


def sse_json(text):
    """MCP streamable HTTP returns plain JSON or SSE (`data: {...}`); take the last JSON."""
    data = [line[5:].strip() for line in text.splitlines() if line.startswith("data:")]
    return json.loads(data[-1] if data else text)


def grade(rows):
    """A row passes when `got` is in `want` (list) or equals `want`."""
    for row in rows:
        want = row["want"]
        row["ok"] = row["got"] in want if isinstance(want, list) else row["got"] == want
    return all(row["ok"] for row in rows)


POLICY = {
    "LLM without key": "API key",
    "LLM with key": "API key",
    "LLM stream with key (TTFT)": "API key",
    "PII (email) blocked": "PII request reject",
    "MCP without key": "API key",
    "MCP initialize with key": "API key",
}


def policy_table(rows):
    """Markdown table policy → request → expected and actual ALLOW/DENY, for an evidence screenshot."""
    lines = ["| Policy | Request | Expected | Actual | Result |", "|---|---|---|---|---|"]
    for row in rows:
        if row["check"].startswith("Prometheus"):
            continue
        policy = POLICY.get(
            row["check"],
            "mcpAuthorization"
            if "not in allowlist" in row["check"]
            else "rate limit"
            if "429" in row["check"]
            else "API key + a2a"
            if row["check"].startswith("A2A card with key")
            else "API key"
            if row["check"].startswith("A2A card without key")
            else "MCP tools",
        )
        denied = (
            row["want"] in (401, 403, 429) or "blocked" in row["check"] or "429" in row["check"]
        )
        lines.append(
            f"| {policy} | {row['check']} | {'DENY' if denied else 'ALLOW'} "
            f"| {row['got']}{' ' + str(row['tools']) if 'tools' in row else ''} "
            f"| {'PASS' if row['ok'] else 'FAIL'} |"
        )
    return "\n".join(lines)


def chat(model=MODEL, content="Say OK.", **extra):
    """Short OpenAI-style chat body; `extra` overrides (stream, max_tokens…)."""
    return {
        "model": model,
        "max_tokens": 8,
        "temperature": 0,
        "messages": [{"role": "user", "content": content}],
        **extra,
    }


async def mcp_head(client, key):
    """initialize + notifications/initialized; returns (headers carrying the session, initialize status)."""
    head = {**MCP_ACCEPT, "Authorization": f"Bearer {key}"}
    init = await client.post(
        "/mcp",
        headers=head,
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "gateway-smoke", "version": "1"},
            },
        },
    )
    session = init.headers.get("mcp-session-id")
    if session:
        head["mcp-session-id"] = session
    await client.post(
        "/mcp", headers=head, json={"jsonrpc": "2.0", "method": "notifications/initialized"}
    )
    return head, init.status_code


async def mcp_denied(client, key, tool=DENIED_TOOL):
    """Call a tool not in the `mcpAuthorization` list; returns (blocked?, response excerpt for the reader to judge)."""
    head, _ = await mcp_head(client, key)
    response = await client.post(
        "/mcp",
        headers=head,
        json={
            "jsonrpc": "2.0",
            "id": 9,
            "method": "tools/call",
            "params": {"name": tool, "arguments": {}},
        },
    )
    text = response.text
    try:
        body = sse_json(text)
    except ValueError:
        body = {}
    result = body.get("result") or {}
    blocked = response.status_code in (401, 403) or "error" in body or bool(result.get("isError"))
    return blocked, f"HTTP {response.status_code}: {text[:160]}"


async def mcp_calls(client, key, sku):
    """initialize → tools/list → tools/call get_sku_features; returns (initialize status, tool names, features have values?)."""
    head, init_status = await mcp_head(client, key)
    tools = sse_json(
        (
            await client.post(
                "/mcp", headers=head, json={"jsonrpc": "2.0", "id": 2, "method": "tools/list"}
            )
        ).text
    )
    names = sorted(t["name"] for t in tools["result"]["tools"])
    called = sse_json(
        (
            await client.post(
                "/mcp",
                headers=head,
                json={
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "tools/call",
                    "params": {"name": "get_sku_features", "arguments": {"seller_skus": [sku]}},
                },
            )
        ).text
    )
    result = called["result"]
    # FastMCP returns structuredContent; older versions only have content[0].text as JSON.
    body = result.get("structuredContent") or json.loads(result["content"][0]["text"])
    features = body.get("features", [{}])[0]
    has_values = any(v is not None for k, v in features.items() if k != "seller_sku")
    return init_status, names, has_values


async def mcp_agent2_tools(client, key, sku):
    """Two Agent 2 tools via the gateway: predict_return_risk has a score; search_policy_chunks has chunks.

    Returns (predict has score and model_version?, number of policy chunks found). A failing tool counts as no result.
    """
    head, _ = await mcp_head(client, key)

    async def call(call_id, name, arguments):
        """tools/call one tool; returns the structured body, or {} when the tool reports isError."""
        reply = sse_json(
            (
                await client.post(
                    "/mcp",
                    headers=head,
                    json={
                        "jsonrpc": "2.0",
                        "id": call_id,
                        "method": "tools/call",
                        "params": {"name": name, "arguments": arguments},
                    },
                )
            ).text
        )
        result = reply.get("result", {})
        if result.get("isError") or "error" in reply:
            return {}
        return result.get("structuredContent") or json.loads(result["content"][0]["text"])

    predicted = await call(
        4,
        "predict_return_risk",
        {
            "seller_sku": sku,
            "channel": "amazon",
            "country": "US",
            "ordered_basis_qty": 1,
            "ordered_product_net_minor": 2500,
        },
    )
    found = await call(5, "search_policy_chunks", {"query": "Amazon return window", "top_k": 2})
    scored = predicted.get("score") is not None and predicted.get("model_version") is not None
    return scored, len(found.get("chunks", []))


async def run(gateway, prometheus, key, sku, burst):
    """Run the full check table through the gateway, wait for a scrape, then query Prometheus; returns ungraded rows."""
    auth = {"Authorization": f"Bearer {key}"}
    # The gateway closes the connection after a /mcp 401; reusing it through port-forward ⇒ "Server disconnected"
    # and kubectl port-forward dies. One new connection per request.
    no_reuse = httpx.Limits(max_keepalive_connections=0)
    async with httpx.AsyncClient(base_url=gateway, timeout=120, limits=no_reuse) as c:
        rows = [
            {
                "check": "LLM without key",
                "got": (await c.post("/v1/chat/completions", json=chat())).status_code,
                "want": 401,
            },
            {
                "check": "LLM with key",
                "got": (
                    await c.post("/v1/chat/completions", headers=auth, json=chat())
                ).status_code,
                "want": 200,
            },
            # stream=true so the gateway records TTFT.
            {
                "check": "LLM stream with key (TTFT)",
                "got": (
                    await c.post("/v1/chat/completions", headers=auth, json=chat(stream=True))
                ).status_code,
                "want": 200,
            },
            {
                "check": "PII (email) blocked",
                "got": (
                    await c.post(
                        "/v1/chat/completions",
                        headers=auth,
                        json=chat(content="Mail me at a.b@example.com"),
                    )
                ).status_code,
                "want": 403,
            },
            {
                "check": "MCP without key",
                "got": (
                    await c.post(
                        "/mcp",
                        headers=MCP_ACCEPT,
                        json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
                    )
                ).status_code,
                "want": 401,
            },
        ]
        init, names, has_values = await mcp_calls(c, key, sku)
        rows += [
            {"check": "MCP initialize with key", "got": init, "want": 200},
            {
                "check": "MCP tools/list includes get_sku_features",
                "got": "get_sku_features" in names,
                "want": True,
                "tools": names,
            },
            {
                "check": f"MCP get_sku_features {sku} has non-null values",
                "got": has_values,
                "want": True,
            },
        ]
        scored, chunks = await mcp_agent2_tools(c, key, sku)
        rows += [
            {
                "check": f"MCP predict_return_risk {sku} returns score and model_version",
                "got": scored,
                "want": True,
            },
            {
                "check": "MCP search_policy_chunks returns at least 1 chunk",
                "got": chunks >= 1,
                "want": True,
            },
        ]
        for agent in AGENTS:
            card = f"/a2a/{agent}/.well-known/agent-card.json"
            rows += [
                {
                    "check": f"A2A card with key ({agent})",
                    "got": (await c.get(card, headers=auth)).status_code,
                    "want": 200,
                },
                {
                    "check": f"A2A card without key ({agent})",
                    "got": (await c.get(card)).status_code,
                    "want": 401,
                },
            ]
        blocked, detail = await mcp_denied(c, key)
        rows.append(
            {
                "check": f"MCP tool not in allowlist ({DENIED_TOOL}) blocked",
                "got": blocked,
                "want": True,
                "detail": detail,
            }
        )
        start = time.perf_counter()
        codes = [
            r.status_code
            for r in await asyncio.gather(
                *(
                    c.post("/v1/chat/completions", headers=auth, json=chat(max_tokens=1))
                    for _ in range(burst)
                )
            )
        ]
        counts = {str(code): codes.count(code) for code in sorted(set(codes))}
        rows.append(
            {
                "check": f"{burst} burst LLM requests: got 429 (rate limit)",
                "got": "429" in counts,
                "want": True,
                "statuses": counts,
                "seconds": round(time.perf_counter() - start, 1),
            }
        )
    await asyncio.sleep(SCRAPE_WAIT_S)
    async with httpx.AsyncClient(base_url=prometheus, timeout=30) as p:
        metrics = {}
        for name, query in QUERIES.items():
            result = (await p.get("/api/v1/query", params={"query": query})).json()["data"][
                "result"
            ]
            metrics[name] = float(result[0]["value"][1]) if result else 0.0
    rows += [
        {"check": f"Prometheus {name} > 0", "got": value > 0, "want": True, "value": value}
        for name, value in metrics.items()
    ]
    return rows


def main():
    """CLI: run, grade, write the receipt, print PASS/FAIL per row."""
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--gateway", default="http://127.0.0.1:18300")
    ap.add_argument("--prometheus", default="http://127.0.0.1:19090")
    ap.add_argument("--sku", default="APP-00042")
    ap.add_argument("--burst", type=int, default=80)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    key = os.environ.get("AGW_AGENT_KEY") or sys.exit(
        "Set AGW_AGENT_KEY (read from the agentgateway-keys Secret)"
    )
    rows = asyncio.run(run(args.gateway, args.prometheus, key, args.sku, args.burst))
    ok = grade(rows)
    with open(args.out, "w") as f:
        json.dump(
            {
                "recorded": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "passed": ok,
                "checks": rows,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )
    print(policy_table(rows))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
