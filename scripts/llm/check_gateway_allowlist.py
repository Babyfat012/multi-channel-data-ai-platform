"""CI gate: agentgateway may only expose agents and MCP tools that agentregistry declares for this version.

The gateway config stays hand-written (infra/k8s/agents/agentgateway.yaml: A2A routes incl. A/B variants, the MCP tool
allowlist). Before it is applied, every agent behind an A2A route must be registered under TAG, and every tool in
`mcpAuthorization` must be declared on the `feature-tools` entry under TAG with a label
`tool.platform.dev/<tool>: allowed`. Anything else fails the stage and the gateway is not updated.

    python3 -m scripts.llm.check_gateway_allowlist --tag <sha> [--apply]

--apply: after the check passes, apply the gateway config and restart agentgateway (KCTX).
"""

import argparse
import json
import os
import re
import sys

from scripts.common import ROOT, kubectl, run
from scripts.llm.publish_agentregistry import registry

GATEWAY = ROOT / "infra/k8s/agents/agentgateway.yaml"
TOOL_LABEL = "tool.platform.dev/"
MCP_ENTRY = "feature-tools"
# Gateway-only variants of a registered agent: the A/B routes of the data agent.
VARIANTS = {"data-agent-b": "data-agent", "data-agent-ab": "data-agent"}


def gateway_allowlist(config: str) -> tuple[set[str], set[str]]:
    """(agents behind an A2A route, tools in the MCP authorization rules) of the gateway config."""
    routes = re.findall(r"^\s*- name: a2a-([a-z0-9-]+?)(?:-path)?\s*$", config, re.M)
    agents = {VARIANTS.get(route, route) for route in routes}
    tools = set(re.findall(r"mcp\.tool\.name == \"([a-z_]+)\"", config))
    return agents, tools


def registered_tools(entry: dict) -> set[str]:
    """Tools an MCPServer entry declares through its `tool.platform.dev/<tool>: allowed` labels."""
    labels = entry.get("metadata", {}).get("labels") or {}
    return {
        key[len(TOOL_LABEL) :]
        for key, value in labels.items()
        if key.startswith(TOOL_LABEL) and value == "allowed"
    }


def unapproved(config: str, registered_agents: set[str], tools: set[str]) -> list[str]:
    """What the gateway would expose without a registry entry, one message per item (empty = approved)."""
    agents, exposed_tools = gateway_allowlist(config)
    problems = [
        f"agent {a!r} has an A2A route but is not registered"
        for a in sorted(agents - registered_agents)
    ]
    problems += [
        f"tool {t!r} is allowed on /mcp but not declared on {MCP_ENTRY}"
        for t in sorted(exposed_tools - tools)
    ]
    return problems


def registry_state(tag: str) -> tuple[set[str], set[str]]:
    """(agent names, declared tools) registered under `tag` (call inside `registry()`)."""
    agents = json.loads(
        run(["arctl", "get", "agents", "--tag", tag, "-o", "json"], capture=True) or "[]"
    )
    mcp = run(
        ["arctl", "get", "mcp", MCP_ENTRY, "--tag", tag, "-o", "json"], capture=True, check=False
    )
    return {a["metadata"]["name"] for a in agents}, registered_tools(json.loads(mcp) if mcp else {})


def main() -> None:
    """Compare, report, and apply the gateway only when nothing is unapproved."""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--tag", required=True, help="registry tag to check against (the commit SHA in CI)"
    )
    parser.add_argument(
        "--apply", action="store_true", help="apply + restart agentgateway when the check passes"
    )
    args = parser.parse_args()
    os.chdir(ROOT)
    config = GATEWAY.read_text()
    with registry():
        agents, tools = registry_state(args.tag)
    problems = unapproved(config, agents, tools)
    if problems:
        print(f"gateway allowlist NOT approved by agentregistry tag {args.tag}:", file=sys.stderr)
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        sys.exit(1)
    exposed_agents, exposed_tools = gateway_allowlist(config)
    print(
        f"gateway allowlist approved by tag {args.tag}: agents {sorted(exposed_agents)}, tools {sorted(exposed_tools)}"
    )
    if args.apply:
        kc = kubectl()
        run([*kc, "apply", "-f", str(GATEWAY)])
        run([*kc, "-n", "agentgateway", "rollout", "restart", "deploy/agentgateway"])
        run(
            [
                *kc,
                "-n",
                "agentgateway",
                "rollout",
                "status",
                "deploy/agentgateway",
                "--timeout=180s",
            ]
        )


if __name__ == "__main__":
    main()
