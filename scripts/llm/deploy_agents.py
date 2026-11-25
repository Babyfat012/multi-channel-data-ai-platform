"""Deploy the agent stack: LLM (llama.cpp model A × 2 + llm-d EPP), agentgateway, kagent (CRDs + controller/UI) and the Data Agent.

One API key for every agent calling the gateway, generated once and then read back from the Secret. Safe to rerun.

    python3 -m scripts.llm.deploy_agents          (KCTX, default kind-platform)
"""

import os
import secrets

from scripts.common import ROOT, apply, kubectl, run, secret_manifest, secret_value

KAGENT_VERSION = "0.10.3"  # deploy_substrate upgrades this same release; keep them in one place


def main() -> None:
    """LLM + gateway, the shared key, then kagent and the first agent."""
    os.chdir(ROOT)
    kc = kubectl()
    kctx = kc[2]
    run([*kc, "apply", "-f", "infra/k8s/agents/llm.yaml"])
    run([*kc, "apply", "-f", "infra/k8s/agents/agentgateway.yaml"])
    key = (
        secret_value(kc, "agentgateway", "agentgateway-keys", "AGW_AGENT_KEY")
        or f"agw_{secrets.token_hex(16)}"
    )
    apply(kc, secret_manifest("agentgateway", "agentgateway-keys", {"AGW_AGENT_KEY": key}))
    run(
        [*kc, "-n", "agentgateway", "rollout", "restart", "deploy/agentgateway"]
    )  # reload ConfigMap/Secret

    helm = ["helm", "--kube-context", kctx, "upgrade", "--install"]
    chart = "oci://ghcr.io/kagent-dev/kagent/helm"
    run(
        [
            *helm,
            "kagent-crds",
            f"{chart}/kagent-crds",
            "--version",
            KAGENT_VERSION,
            "-n",
            "kagent",
            "--create-namespace",
            "--wait",
        ]
    )
    # Same key on the kagent side: ModelConfig reads AGW_AGENT_KEY, RemoteMCPServer sends AUTH_HEADER.
    apply(
        kc,
        secret_manifest(
            "kagent", "kagent-agw-key", {"AGW_AGENT_KEY": key, "AUTH_HEADER": f"Bearer {key}"}
        ),
    )
    run(
        [
            *helm,
            "kagent",
            f"{chart}/kagent",
            "--version",
            KAGENT_VERSION,
            "-n",
            "kagent",
            "-f",
            "infra/k8s/agents/kagent-values.yaml",
            "--wait",
            "--timeout",
            "10m",
        ]
    )
    run([*kc, "apply", "-f", "infra/k8s/agents/data-agent.yaml"])
    run([*kc, "-n", "kagent", "get", "agents,remotemcpservers,modelconfigs"])


if __name__ == "__main__":
    main()
