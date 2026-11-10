"""Agent Substrate: pinned version matches the running kagent, wiring flags follow the docs, every SandboxAgent runs Go on 1 pool."""

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[4]
SCRIPT = (ROOT / "scripts/setup/deploy_substrate.sh").read_text()


def test_versions_are_pinned_and_match_running_kagent():
    """Substrate 0.0.9 is the version the docs pair with kagent 0.10.3; kagent must not drift from deploy_kind_agents."""
    agents = (ROOT / "scripts/setup/deploy_kind_agents.sh").read_text()
    running = re.search(r"KAGENT_VERSION=(\S+)", agents).group(1)
    assert re.search(r"KAGENT_VERSION=(\S+)", SCRIPT).group(1) == running == "0.10.3"
    assert "SUBSTRATE_VERSION=0.0.9" in SCRIPT
    assert 'ateom-gvisor:v"$SUBSTRATE_VERSION"' in SCRIPT


def test_kagent_upgrade_keeps_values_and_wires_substrate():
    """--reuse-values keeps the model behind the gateway; missing any flag leaves SandboxAgent nowhere to run."""
    upgrade = SCRIPT[SCRIPT.index("upgrade kagent ") :].split("--wait")[0]
    assert "--reuse-values" in upgrade
    for flag in (
        "controller.substrate.enabled=true",
        "controller.substrate.ateApiEndpoint=dns:///api.ate-system.svc:443",
        "substrateWorkerPool.create=true",
        'substrateWorkerPool.replicas="${SUBSTRATE_WORKERS:-1}"',
    ):
        assert f"--set {flag}" in upgrade, flag


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
    install = SCRIPT[SCRIPT.index("upgrade --install substrate ") :].split("--timeout")[0]
    assert '--set-string auth.jwt.issuer="$ISSUER"' in install
    assert "get --raw /.well-known/openid-configuration" in SCRIPT
