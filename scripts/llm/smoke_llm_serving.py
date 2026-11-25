"""Smoke test of model A's serving (Jenkins llm-serving): the llm-d EPP sees every llama.cpp replica, and one chat request
through agentgateway (→ loopback route → EPP → replica) answers.

Needs a port-forward to agentgateway and the agent key in the environment (never printed):

    kubectl -n agentgateway port-forward svc/agentgateway 18300:3000 &
    AGW_AGENT_KEY=... KCTX=<context> python3 -m scripts.llm.smoke_llm_serving
"""

from __future__ import annotations

import json
import os
import sys
import urllib.request

from scripts.common import kubectl, run

MODEL = "qwen2.5-1.5b-instruct"
EPP_METRICS = "/api/v1/namespaces/llm/services/epp:metrics/proxy/metrics"


def epp_endpoints(metrics: str) -> set[str]:
    """Replicas the EPP is tracking: one llm_d_epp_per_endpoint_queue_size series each."""
    prefix = "llm_d_epp_per_endpoint_queue_size{"
    return {
        line.split('model_server_endpoint="', 1)[1].split('"', 1)[0]
        for line in metrics.splitlines()
        if line.startswith(prefix) and 'model_server_endpoint="' in line
    }


def chat(base: str, key: str) -> str:
    """One short chat completion through the gateway; returns the answer text."""
    request = urllib.request.Request(
        f"{base}/v1/chat/completions",
        data=json.dumps(
            {"model": MODEL, "max_tokens": 8, "messages": [{"role": "user", "content": "Say OK."}]}
        ).encode(),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {key}"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=180) as response:
        return json.load(response)["choices"][0]["message"]["content"]


def main() -> int:
    """Exit 1 when the EPP misses a replica or the request through it fails."""
    kc = kubectl()
    expected = int(
        run(
            [*kc, "-n", "llm", "get", "deploy/qwen", "-o", "jsonpath={.spec.replicas}"],
            capture=True,
        )
    )
    seen = epp_endpoints(run([*kc, "get", "--raw", EPP_METRICS], capture=True))
    print(f"EPP endpoints: {len(seen)}/{expected} {sorted(seen)}")
    answer = chat(os.environ.get("AGW_URL", "http://localhost:18300"), os.environ["AGW_AGENT_KEY"])
    print(f"chat via agentgateway → EPP: {answer!r}")
    return 0 if len(seen) == expected and answer.strip() else 1


if __name__ == "__main__":
    sys.exit(main())
