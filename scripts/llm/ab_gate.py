"""A/B gate for the small LLM (diagram: Qwen 0.5B "B: candidate, gets 50%, kept only if it wins A/B").

Scores data-agent (A, Qwen 1.5B) and data-agent-b (B, Qwen 0.5B) on the same SKUs with the agent eval, then records
the decision in agentregistry: data-agent-b@stable with label ab.platform.dev/decision = keep | drop.
check_gateway_allowlist.py reads that label on EVERY gateway apply, so a later deploy cannot put a losing B back.

B wins only if it reaches the eval threshold AND is not less accurate than A: B is there to be faster at the same
quality. A missing result (a branch down, the eval crashed) is a loss: nothing proves B is good enough.

Order matters: decision → gateway apply (drops or restores B in the split) → only then scale B to 0, so the gateway
never sends half the traffic to a branch with no pods.

    python3 -m scripts.llm.ab_gate        (Jenkins job llm-serving, stage A/B gate; same env as scripts.llm.eval_agent_ci)
"""

import json
import os
import sys
import tempfile

from scripts.common import ROOT, kubectl, run
from scripts.llm.check_gateway_allowlist import DECISION_LABEL
from scripts.llm.eval_agent_ci import MIN_ACCURACY
from scripts.llm.publish_agentregistry import REGISTRY, registry, tagged

A, B = "data-agent", "data-agent-b"
# Branch B's pods: its LLM and its agent (A is shared with production and never scaled here).
B_WORKLOADS = (("llm", "deploy/qwen-small"), ("kagent", "deploy/data-agent-b"))


def decide(
    a: float | None, b: float | None, minimum: float = float(MIN_ACCURACY)
) -> tuple[bool, str]:
    """(keep B?, why). Missing results lose: the gate keeps B only on evidence."""
    if a is None or b is None:
        return False, f"no eval result for {'A' if a is None else 'B'}: cannot show B is as good"
    if b < minimum:
        return False, f"B accuracy {b} < threshold {minimum}"
    if b < a:
        return False, f"B accuracy {b} < A accuracy {a}"
    return True, f"B accuracy {b} ≥ threshold {minimum} and ≥ A accuracy {a}"


def accuracy(agent: str) -> float | None:
    """Accuracy from eval_agent_ci's summary for this agent, None when it is missing or unreadable."""
    summary = ROOT / f"out/agent-eval-{agent}-features-summary.json"
    try:
        return float(json.loads(summary.read_text())["accuracy"])
    except (OSError, ValueError, KeyError, TypeError):
        return None


def with_decision(manifest: str, decision: str) -> str:
    """The registry manifest with the decision label (the gate owns the labels of data-agent-b)."""
    if "\n  labels:" in manifest:
        raise ValueError("data-agent-b.yaml sets labels itself; the A/B gate owns them")
    return manifest.replace(
        "metadata:\n", f"metadata:\n  labels:\n    {DECISION_LABEL}: {decision}\n", 1
    )


def scale(kc: list[str], replicas: int) -> None:
    """Scale branch B and, when scaling up, wait until it serves."""
    for namespace, workload in B_WORKLOADS:
        run([*kc, "-n", namespace, "scale", workload, f"--replicas={replicas}"])
    if replicas:
        for namespace, workload in B_WORKLOADS:
            run([*kc, "-n", namespace, "rollout", "status", workload, "--timeout=300s"])


def main() -> None:
    """Scale B up, score A and B, record the decision, apply the gateway, scale B down if it lost."""
    os.chdir(ROOT)
    kc = kubectl()
    scale(kc, 1)
    for agent in (A, B):
        # eval_agent_ci exits non-zero below its threshold; here a low score is a result, not a failure.
        os.environ.update(EVAL_AGENT=agent, EVAL_TASK="features")
        run([sys.executable, "-m", "scripts.llm.eval_agent_ci"], check=False)
    keep, reason = decide(accuracy(A), accuracy(B))
    decision = "keep" if keep else "drop"
    print(f"A/B decision: {decision} ({reason})")
    with registry(), tempfile.NamedTemporaryFile("w", suffix=".yaml") as manifest:
        manifest.write(
            tagged(with_decision((REGISTRY / f"{B}.yaml").read_text(), decision), "stable")
        )
        manifest.flush()
        run(["arctl", "apply", "-f", manifest.name])
    run([sys.executable, "-m", "scripts.llm.check_gateway_allowlist", "--tag", "stable", "--apply"])
    if not keep:
        scale(kc, 0)


if __name__ == "__main__":
    main()
