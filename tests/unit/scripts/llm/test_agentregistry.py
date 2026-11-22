"""agentregistry in CI: tagged publishing and the gateway allowlist gate, checked offline (no registry, no cluster)."""

from pathlib import Path

import pytest
import yaml

from scripts.llm.check_gateway_allowlist import (
    GATEWAY,
    gateway_allowlist,
    registered_tools,
    unapproved,
)
from scripts.llm.publish_agentregistry import ENTRIES, REGISTRY, tagged

ROOT = Path(__file__).resolve().parents[4]


def registry_files() -> dict[str, dict]:
    """The registry manifests in the repo, as the registry would store them."""
    return {name: yaml.safe_load((REGISTRY / f"{name}.yaml").read_text()) for name in ENTRIES}


def test_repo_gateway_is_approved_by_repo_registry_entries():
    """What CI will publish under the commit tag approves exactly what the gateway exposes."""
    entries = registry_files()
    agents = {e["metadata"]["name"] for e in entries.values() if e["kind"] == "Agent"}
    tools = registered_tools(entries["feature-tools"])
    assert unapproved(GATEWAY.read_text(), agents, tools) == []
    exposed_agents, exposed_tools = gateway_allowlist(GATEWAY.read_text())
    assert exposed_agents == {
        "data-agent",
        "model-agent",
        "coordinator",
    }  # A/B routes map to data-agent
    assert (
        exposed_tools
        == tools
        == {
            "get_sku_features",
            "get_policy_chunks",
            "search_policy_chunks",
            "predict_return_risk",
        }
    )


def test_unregistered_agent_or_tool_blocks_the_gateway():
    """A new route or tool that the registry does not declare under the tag fails the stage."""
    config = GATEWAY.read_text()
    problems = unapproved(config, {"data-agent", "model-agent"}, {"get_sku_features"})
    assert "agent 'coordinator' has an A2A route but is not registered" in problems
    assert (
        "tool 'predict_return_risk' is allowed on /mcp but not declared on feature-tools"
        in problems
    )
    extra = config + "\n          - 'mcp.tool.name == \"drop_tables\"'\n"
    every_tool = registered_tools(registry_files()["feature-tools"])
    assert unapproved(extra, {"data-agent", "model-agent", "coordinator"}, every_tool) == [
        "tool 'drop_tables' is allowed on /mcp but not declared on feature-tools"
    ]


def test_only_allowed_tool_labels_count():
    """Labels outside the tool prefix, or with another value, declare nothing."""
    entry = {
        "metadata": {
            "labels": {
                "tool.platform.dev/get_sku_features": "allowed",
                "tool.platform.dev/predict_return_risk": "blocked",
                "team": "ml",
            }
        }
    }
    assert registered_tools(entry) == {"get_sku_features"}
    assert registered_tools({"metadata": {}}) == set()


def test_tagged_sets_metadata_tag_once():
    """The publish step owns metadata.tag; the manifest stays valid YAML with the tag under metadata."""
    manifest = (REGISTRY / "feature-tools.yaml").read_text()
    doc = yaml.safe_load(tagged(manifest, "0123abcd"))
    assert doc["metadata"]["tag"] == "0123abcd" and doc["metadata"]["name"] == "feature-tools"
    assert doc["metadata"]["labels"]["tool.platform.dev/get_sku_features"] == "allowed"
    with pytest.raises(ValueError, match="owns it"):
        tagged(tagged(manifest, "a"), "b")


@pytest.mark.parametrize("tag", ["", "Stable", "a/b", "x" * 64, "-lead"])
def test_tagged_rejects_invalid_tags(tag):
    with pytest.raises(ValueError, match="invalid tag"):
        tagged("metadata:\n  name: x\n", tag)


def test_pipelines_register_before_the_gateway_and_promote_after_eval():
    """Candidate under the commit tag before the gateway is applied; `stable` only after the eval passed."""
    import re

    for name, deploy in (("feature-api-agents", "Gateway allowlist"), ("coordinator", "Deploy")):
        text = (ROOT / f"infra/jenkins/{name}.Jenkinsfile").read_text()
        stages = re.findall(r"stage\('([^']+)'\)", text)
        order = [
            stages.index(s)
            for s in ("Register candidate", deploy, "Eval agent", "Promote to stable")
        ]
        assert order == sorted(order), name
        assert "kubectl --context $KCTX apply -f infra/k8s/agents/agentgateway.yaml" not in text, (
            name
        )
