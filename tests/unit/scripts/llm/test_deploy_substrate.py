"""Agent Substrate: pinned version matches the running kagent, wiring flags follow the docs, every SandboxAgent runs Go on 1 pool."""

from pathlib import Path

import yaml

from scripts.llm import deploy_agents, deploy_substrate

ROOT = Path(__file__).resolve().parents[4]


def test_versions_are_pinned_and_match_running_kagent():
    """Substrate 0.0.9 is the version the docs pair with kagent 0.10.3; one kagent version for both scripts."""
    assert deploy_substrate.KAGENT_VERSION == deploy_agents.KAGENT_VERSION == "0.10.3"
    assert deploy_substrate.SUBSTRATE_VERSION == "0.0.9"
    upgrade = deploy_substrate.kagent_upgrade("1")
    assert (
        "substrateWorkerPool.ateomImage=ghcr.io/kagent-dev/substrate/ateom-gvisor:v0.0.9" in upgrade
    )


def test_kagent_upgrade_keeps_values_and_wires_substrate():
    """--reuse-values keeps the model behind the gateway; missing any flag leaves SandboxAgent nowhere to run."""
    upgrade = deploy_substrate.kagent_upgrade("3")
    assert "--reuse-values" in upgrade
    for flag in (
        "controller.substrate.enabled=true",
        "controller.substrate.ateApiEndpoint=dns:///api.ate-system.svc:443",
        "substrateWorkerPool.create=true",
        "substrateWorkerPool.replicas=3",
    ):
        assert upgrade[upgrade.index(flag) - 1] == "--set", flag


def test_every_sandbox_agent_runs_go_on_the_shared_pool():
    """Fleet = many SandboxAgents, one WorkerPool; data-agent-sandbox keeps exactly the MCP tools via the gateway."""
    docs = [
        d
        for f in sorted((ROOT / "infra/k8s/agents/sandbox").glob("*.yaml"))
        for d in yaml.safe_load_all(f.read_text())
        if d
    ]
    assert len(docs) >= 6 and {d["kind"] for d in docs} == {"SandboxAgent"}
    for d in docs:
        assert d["spec"]["declarative"]["runtime"] == "go", d["metadata"]["name"]
        assert d["spec"]["substrate"]["workerPoolRef"] == {"name": "kagent-default"}
        assert d["spec"]["declarative"]["modelConfig"] == "default-model-config"
    data = next(d for d in docs if d["metadata"]["name"] == "data-agent-sandbox")
    assert data["spec"]["declarative"]["tools"][0]["mcpServer"]["name"] == "feature-tools"


def test_substrate_jwt_issuer_comes_from_the_cluster():
    """The chart default = kind issuer; EKS signs SA tokens with its own OIDC issuer ⇒ ateapi rejects every worker unless changed."""
    install = deploy_substrate.substrate_install("https://oidc.eks.example/id/X")
    assert (
        install[install.index("auth.jwt.issuer=https://oidc.eks.example/id/X") - 1]
        == "--set-string"
    )
    source = (ROOT / "scripts/llm/deploy_substrate.py").read_text()
    assert '"get", "--raw", "/.well-known/openid-configuration"' in source
