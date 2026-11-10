"""Benchmark an OpenAI-style LLM server (llama.cpp).

Send `--requests` chat requests at each concurrency level (`--concurrency 1,2,4`) with a prompt the size of a real
Agent prompt (system + tool descriptions); measure p50/p95 latency, requests/second and generated tokens/second
(from `usage`). Prints one JSON line per level.

python scripts/evidence/bench_llm.py --url http://qwen.llm:8080/v1/chat/completions --label A
"""

import argparse
import json
import os
import statistics
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

SYSTEM = (
    "You are the Model Agent for an online seller. You MUST call a tool before answering. "
    "Case A: new order -> predict_return_risk. Case B: policy questions -> search_policy_chunks. "
) * 4  # ~200 tokens, close to the Agent's system prompt + tool descriptions
TOOLS = [
    {
        "type": "function",
        "function": {
            "name": name,
            "description": desc,
            "parameters": {"type": "object", "properties": props, "required": list(props)},
        },
    }
    for name, desc, props in [
        (
            "get_sku_features",
            "Return features of 1-100 SKUs",
            {"seller_skus": {"type": "array", "items": {"type": "string"}}},
        ),
        ("search_policy_chunks", "Search return policy chunks", {"query": {"type": "string"}}),
        (
            "predict_return_risk",
            "Return risk score of a new order line",
            {
                "seller_sku": {"type": "string"},
                "channel": {"type": "string"},
                "ordered_basis_qty": {"type": "integer"},
                "ordered_product_net_minor": {"type": "integer"},
            },
        ),
    ]
]
QUESTIONS = [
    "A buyer on amazon ordered 2 units of sku-00003 for 4500 cents. What is the return risk?",
    "How many days does a Walmart buyer have to return an item?",
    "What are the return features of sku-00042?",
    "Can a TikTok Shop buyer get a refund without sending the item back?",
]


def one(url, model, i, max_tokens):
    """One chat request; returns (latency in seconds, generated tokens, prompt tokens)."""
    body = {
        "model": model,
        "max_tokens": max_tokens,
        "temperature": 0,
        "messages": [
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": QUESTIONS[i % 4]},
        ],
        "tools": TOOLS,
    }
    start = time.perf_counter()
    headers = {"Content-Type": "application/json"}
    # Going through agentgateway (e.g. the virtual model `platform-chat` for A/B) needs an API key; calling llama.cpp directly does not.
    if os.environ.get("AGW_AGENT_KEY"):
        headers["Authorization"] = f"Bearer {os.environ['AGW_AGENT_KEY']}"
    request = urllib.request.Request(url, data=json.dumps(body).encode(), headers=headers)
    data = json.load(urllib.request.urlopen(request, timeout=600))
    return (
        time.perf_counter() - start,
        data["usage"]["completion_tokens"],
        data["usage"]["prompt_tokens"],
    )


def main():
    """Warm up once, then measure each concurrency level, printing one JSON line per level."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--url", required=True)
    parser.add_argument("--model", default="qwen2.5-1.5b-instruct")
    parser.add_argument("--label", default="run")
    parser.add_argument("--concurrency", default="1,2,4")
    parser.add_argument("--requests", type=int, default=8)
    parser.add_argument("--max-tokens", type=int, default=64)
    args = parser.parse_args()
    one(args.url, args.model, 0, 8)  # warm-up (loads the prompt cache)
    for c in map(int, args.concurrency.split(",")):
        start = time.perf_counter()
        with ThreadPoolExecutor(c) as pool:
            rows = list(
                pool.map(
                    lambda i: one(args.url, args.model, i, args.max_tokens), range(args.requests)
                )
            )
        wall = time.perf_counter() - start
        latencies = sorted(r[0] for r in rows)
        print(
            json.dumps(
                {
                    "label": args.label,
                    "concurrency": c,
                    "requests": len(rows),
                    "p50_s": round(statistics.median(latencies), 2),
                    "p95_s": round(latencies[max(0, int(len(latencies) * 0.95) - 1)], 2),
                    "req_per_s": round(len(rows) / wall, 3),
                    "output_tok_per_s": round(sum(r[1] for r in rows) / wall, 1),
                    "avg_prompt_tokens": round(sum(r[2] for r in rows) / len(rows)),
                }
            ),
            flush=True,
        )


if __name__ == "__main__":
    main()
