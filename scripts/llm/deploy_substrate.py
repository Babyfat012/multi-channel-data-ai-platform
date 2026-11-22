"""Agent Substrate for kagent: agents run in a gVisor sandbox on a small shared WorkerPool.

Substrate's node agent sets the gVisor runtime itself (the node's containerd is untouched). Idle actors snapshot to
object storage and go "Suspended", so many agents share a few workers (Warm Up / Agent Fleet).
See kagent.dev/docs/kagent/examples/agent-substrate. Requires kagent (scripts/llm/deploy_agents.py). Safe to rerun.

    python3 -m scripts.llm.deploy_substrate          (KCTX, SUBSTRATE_WORKERS default 1)
"""

import json
import os

from scripts.common import ROOT, kubectl, run
from scripts.llm.deploy_agents import (
    KAGENT_VERSION,
)  # versions < 0.9.7 silently ignore the substrate values

SUBSTRATE_VERSION = "0.0.9"
SUBSTRATE_CHARTS = "oci://ghcr.io/kagent-dev/substrate/helm"


def substrate_install(issuer: str) -> list[str]:
    """Helm args for the Substrate release; the JWT issuer must be the cluster's own (chart default fits kind only)."""
    return [
        "upgrade",
        "--install",
        "substrate",
        f"{SUBSTRATE_CHARTS}/substrate",
        "--version",
        SUBSTRATE_VERSION,
        "-n",
        "ate-system",
        "--set-string",
        f"auth.jwt.issuer={issuer}",
        "--wait",
        "--timeout",
        "10m",
    ]


def kagent_upgrade(workers: str) -> list[str]:
    """Helm args that keep kagent's values (model via agentgateway, A2A/MCP proxy) and only add the Substrate wiring."""
    settings = {
        "controller.substrate.enabled": "true",
        "controller.substrate.ateApiEndpoint": "dns:///api.ate-system.svc:443",
        "controller.substrate.ateApiInsecure": "true",
        "substrateWorkerPool.create": "true",
        "substrateWorkerPool.replicas": workers,
        "substrateWorkerPool.ateomImage": f"ghcr.io/kagent-dev/substrate/ateom-gvisor:v{SUBSTRATE_VERSION}",
    }
    return [
        "upgrade",
        "kagent",
        "oci://ghcr.io/kagent-dev/kagent/helm/kagent",
        "--version",
        KAGENT_VERSION,
        "-n",
        "kagent",
        "--reuse-values",
        *[a for k, v in settings.items() for a in ("--set", f"{k}={v}")],
        "--wait",
        "--timeout",
        "10m",
    ]


def main() -> None:
    """CRDs, Substrate, kagent wiring, then the SandboxAgents (the first run builds the golden snapshot, ~60–90 s)."""
    os.chdir(ROOT)
    kc = kubectl()
    helm = ["helm", "--kube-context", kc[2]]
    # SA token issuer from the cluster itself: kind = kubernetes.default.svc…, EKS = oidc.eks.<region>…
    issuer = json.loads(
        run([*kc, "get", "--raw", "/.well-known/openid-configuration"], capture=True)
    )["issuer"]
    run(
        [
            *helm,
            "upgrade",
            "--install",
            "substrate-crds",
            f"{SUBSTRATE_CHARTS}/substrate-crds",
            "--version",
            SUBSTRATE_VERSION,
            "-n",
            "ate-system",
            "--create-namespace",
            "--wait",
        ]
    )
    run([*helm, *substrate_install(issuer)])
    run([*helm, *kagent_upgrade(os.environ.get("SUBSTRATE_WORKERS") or "1")])
    run([*kc, "apply", "-f", "infra/k8s/agents/sandbox/"])
    run(
        [
            *kc,
            "-n",
            "kagent",
            "wait",
            "sandboxagent",
            "--all",
            "--for=condition=Ready",
            "--timeout=10m",
        ]
    )
    run([*kc, "-n", "kagent", "get", "workerpool,sandboxagent"])
    run([*kc, "-n", "ate-system", "get", "pods"])


if __name__ == "__main__":
    main()
