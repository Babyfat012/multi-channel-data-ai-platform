"""Kiểm agentgateway (LLM / MCP / A2A) + telemetry trên Prometheus, ghi một receipt JSON.

Thay bảng `curl` gõ tay ở docs/edai-2/reference/llm_platform_agents.md. Chạy trên laptop qua port-forward:

kubectl -n agentgateway port-forward svc/agentgateway 18300:3000 &
kubectl -n observability port-forward svc/prometheus-server 19090:80 &
export AGW_AGENT_KEY=$(kubectl -n agentgateway get secret agentgateway-keys -o jsonpath='{.data.AGW_AGENT_KEY}' | base64 -d)
python scripts/evidence/gateway_smoke.py --out docs/evidence/aws/gateway-smoke-<ngày>.json

Key chỉ đọc từ biến môi trường, không in ra. Thoát mã 1 nếu có dòng không đạt.
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
# Prometheus scrape 15 s ⇒ chờ hơn một chu kỳ trước khi hỏi.
SCRAPE_WAIT_S = 20
QUERIES = {
    "llm_tokens": "sum(agentgateway_gen_ai_client_token_usage_sum)",
    "ttft_requests": "sum(agentgateway_gen_ai_server_time_to_first_token_count)",
    "mcp_requests": "sum(agentgateway_mcp_requests_total)",
    "a2a_requests": 'sum(agentgateway_requests_total{route=~"default/a2a-.*"})',  # route có tiền tố namespace
    "pii_checks": "sum(agentgateway_guardrail_checks_total)",
    # Gauge llama.cpp về 0 ở lần scrape không có token mới ⇒ lấy đỉnh 5 phút.
    "llamacpp_tokens_per_s": "max(max_over_time(llamacpp:predicted_tokens_seconds[5m]))",
}


def sse_json(text):
    """MCP streamable HTTP trả JSON thường hoặc SSE (`data: {...}`); lấy JSON cuối cùng."""
    data = [line[5:].strip() for line in text.splitlines() if line.startswith("data:")]
    return json.loads(data[-1] if data else text)


def grade(rows):
    """Một dòng đạt khi `got` nằm trong `want` (list) hoặc bằng `want`."""
    for row in rows:
        want = row["want"]
        row["ok"] = row["got"] in want if isinstance(want, list) else row["got"] == want
    return all(row["ok"] for row in rows)


def chat(model=MODEL, content="Say OK.", **extra):
    """Body chat ngắn kiểu OpenAI; `extra` ghi đè (stream, max_tokens…)."""
    return {
        "model": model,
        "max_tokens": 8,
        "temperature": 0,
        "messages": [{"role": "user", "content": content}],
        **extra,
    }


async def mcp_calls(client, key, sku):
    """initialize → tools/list → tools/call get_sku_features; trả (mã initialize, tên tool, feature có giá trị?)."""
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
    # FastMCP trả structuredContent; bản cũ chỉ có content[0].text là JSON.
    body = result.get("structuredContent") or json.loads(result["content"][0]["text"])
    features = body.get("features", [{}])[0]
    has_values = any(v is not None for k, v in features.items() if k != "seller_sku")
    return init.status_code, names, has_values


async def run(gateway, prometheus, key, sku, burst):
    """Gọi đủ bảng kiểm qua gateway, chờ scrape rồi hỏi Prometheus; trả list dòng chưa chấm."""
    auth = {"Authorization": f"Bearer {key}"}
    # Gateway đóng kết nối sau 401 của /mcp; dùng lại kết nối đó qua port-forward ⇒ "Server disconnected"
    # và kubectl port-forward chết. Mỗi request một kết nối mới.
    no_reuse = httpx.Limits(max_keepalive_connections=0)
    async with httpx.AsyncClient(base_url=gateway, timeout=120, limits=no_reuse) as c:
        rows = [
            {
                "check": "LLM không key",
                "got": (await c.post("/v1/chat/completions", json=chat())).status_code,
                "want": 401,
            },
            {
                "check": "LLM có key",
                "got": (
                    await c.post("/v1/chat/completions", headers=auth, json=chat())
                ).status_code,
                "want": 200,
            },
            # stream=true để gateway ghi TTFT.
            {
                "check": "LLM stream có key (TTFT)",
                "got": (
                    await c.post("/v1/chat/completions", headers=auth, json=chat(stream=True))
                ).status_code,
                "want": 200,
            },
            {
                "check": "PII (email) bị chặn",
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
                "check": "MCP không key",
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
            {"check": "MCP initialize có key", "got": init, "want": 200},
            {
                "check": "MCP tools/list có get_sku_features",
                "got": "get_sku_features" in names,
                "want": True,
                "tools": names,
            },
            {
                "check": f"MCP get_sku_features {sku} có giá trị khác null",
                "got": has_values,
                "want": True,
            },
            {
                "check": "A2A card có key",
                "got": (
                    await c.get("/a2a/data-agent/.well-known/agent-card.json", headers=auth)
                ).status_code,
                "want": 200,
            },
            {
                "check": "A2A card không key",
                "got": (await c.get("/a2a/data-agent/.well-known/agent-card.json")).status_code,
                "want": 401,
            },
        ]
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
                "check": f"{burst} request LLM dồn: có 429 (rate limit)",
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
    """CLI: chạy, chấm, ghi receipt, in PASS/FAIL từng dòng."""
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--gateway", default="http://127.0.0.1:18300")
    ap.add_argument("--prometheus", default="http://127.0.0.1:19090")
    ap.add_argument("--sku", default="APP-00042")
    ap.add_argument("--burst", type=int, default=80)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    key = os.environ.get("AGW_AGENT_KEY") or sys.exit(
        "Đặt AGW_AGENT_KEY (đọc từ Secret agentgateway-keys)"
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
    for row in rows:
        print(("PASS" if row["ok"] else "FAIL"), row["check"], row["got"])
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
