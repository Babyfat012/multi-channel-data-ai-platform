"""CI gate: ask an agent through agentgateway (A2A) and score its answers against the services' own answers.

Ground truth: the Feature API (features), KServe (predict, protocol v1) or fixed policy facts (policy).

Read-only: never modifies or flushes Redis. Exits non-zero below 95 % accuracy. One result file per (agent, task)
because Jenkins runs it several times in a row and archives out/agent-eval*.json*.

    EVAL_AGENT=data-agent EVAL_TASK=features python3 -m scripts.llm.eval_agent_ci

Env: EVAL_AGENT (data-agent), EVAL_TASK (features | predict | policy), EVAL_SKUS (AWS: APP-…), EVAL_TIMEOUT (180),
EVAL_FEATURE_PORT (18000), EVAL_PREDICT_PORT (18100), EVAL_GATEWAY_PORT (18300), VAULT_ADDR + VAULT_TOKEN
(the gateway API key), KCTX.
"""

import os
import subprocess
import sys
import time
import urllib.request

from scripts.common import ROOT, env, kubectl, tee, vault_secret

DEFAULT_SKUS = ",".join(f"sku-{n:05d}" for n in range(1, 13))
MIN_ACCURACY = "0.95"


def reachable(url: str, headers: dict | None = None) -> bool:
    """True when the URL answers 2xx."""
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=headers or {}), timeout=2):
            return True
    except OSError:
        return False


def port_forward(
    kc: list[str], namespace: str, service: str, ports: str, log: str
) -> subprocess.Popen:
    """Background `kubectl port-forward`, logging to a file under out/."""
    with open(log, "w") as out:
        return subprocess.Popen(
            [*kc, "-n", namespace, "port-forward", service, ports],
            stdout=out,
            stderr=subprocess.STDOUT,
        )


def main() -> None:
    """Port-forward the Feature API + gateway, wait until both answer, run ml.agent_eval and keep its summary."""
    os.chdir(ROOT)
    (ROOT / "out").mkdir(exist_ok=True)
    agent = os.environ.get("EVAL_AGENT") or "data-agent"
    task = os.environ.get("EVAL_TASK") or "features"
    tag = f"{agent}-{task}"
    results, summary = (
        ROOT / f"out/agent-eval-{tag}.jsonl",
        ROOT / f"out/agent-eval-{tag}-summary.json",
    )
    results.unlink(missing_ok=True)
    summary.unlink(missing_ok=True)
    env("VAULT_ADDR")
    key = (vault_secret("agentgateway", env("VAULT_TOKEN")) or {}).get("AGW_AGENT_KEY")
    if not key:
        sys.exit("missing AGW_AGENT_KEY in Vault secret/platform/agentgateway")
    os.environ["AGW_AGENT_KEY"] = key  # read by ml.agent_eval; never printed
    feature_port = os.environ.get("EVAL_FEATURE_PORT") or "18000"
    gateway_port = os.environ.get("EVAL_GATEWAY_PORT") or "18300"
    predict_port = os.environ.get("EVAL_PREDICT_PORT") or "18100"
    kc = kubectl()
    forwards = [
        port_forward(
            kc,
            "serving-api",
            "svc/feature-api",
            f"{feature_port}:8000",
            "out/eval-feature-forward.log",
        ),
        port_forward(
            kc,
            "agentgateway",
            "svc/agentgateway",
            f"{gateway_port}:3000",
            "out/eval-gateway-forward.log",
        ),
    ]
    # The ready checks: Feature API health, the agent card behind the gateway, KServe's model when predicting.
    checks = [
        (f"http://localhost:{feature_port}/healthz", None),
        (
            f"http://localhost:{gateway_port}/a2a/{agent}/.well-known/agent-card.json",
            {"Authorization": f"Bearer {key}"},
        ),
    ]
    extra = []
    if task == "predict":
        forwards.append(
            port_forward(
                kc,
                "ml-serving",
                "svc/return-risk-predictor",
                f"{predict_port}:80",
                "out/eval-predict-forward.log",
            )
        )
        checks.append((f"http://localhost:{predict_port}/v1/models/return-risk", None))
        extra = ["--predict-url", f"http://localhost:{predict_port}/v1/models/return-risk:predict"]
    try:
        for _ in range(30):
            if any(f.poll() is not None for f in forwards):
                sys.exit("Eval port-forward stopped; see out/eval-*-forward.log")
            if all(reachable(url, headers) for url, headers in checks):
                break
            time.sleep(1)
        else:
            sys.exit("Eval port-forward not ready")
        tee(
            [
                sys.executable,
                "-m",
                "ml.agent_eval",
                "--agent",
                agent,
                "--task",
                task,
                "--skus",
                os.environ.get("EVAL_SKUS") or DEFAULT_SKUS,
                "--gateway",
                f"http://localhost:{gateway_port}",
                "--feature-api",
                f"http://localhost:{feature_port}",
                "--min-accuracy",
                MIN_ACCURACY,
                "--timeout",
                os.environ.get("EVAL_TIMEOUT") or "180",
                "--out",
                str(results),
                *extra,
            ],
            summary,
            merge_stderr=False,
        )  # summary = stdout JSON only
    finally:
        for forward in forwards:
            forward.terminate()


if __name__ == "__main__":
    main()
