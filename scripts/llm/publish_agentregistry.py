"""Publish the MCP server + the 3 agents to agentregistry (governance) with `arctl apply`, through a port-forward.

Requires arctl v0.4.0 (matching the server). Rerunning updates the entries under the same names.

    python3 -m scripts.llm.publish_agentregistry          (KCTX, default kind-platform)
"""

import os
import subprocess
import time
import urllib.request

from scripts.common import ROOT, kubectl, run

REGISTRY = "infra/k8s/agents/registry"


def main() -> None:
    """MCP server first (the agents reference it), then the agents, then list both."""
    os.chdir(ROOT)
    forward = subprocess.Popen(
        [*kubectl(), "-n", "agentregistry", "port-forward", "svc/agentregistry", "12121:12121"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        for _ in range(20):
            try:
                urllib.request.urlopen("http://localhost:12121/openapi.json", timeout=2).close()
                break
            except OSError:
                time.sleep(1)
        os.environ["ARCTL_API_BASE_URL"] = "http://localhost:12121"
        for name in ("feature-tools", "data-agent", "model-agent", "coordinator"):
            run(["arctl", "apply", "-f", f"{REGISTRY}/{name}.yaml"])
        run(["arctl", "get", "mcpservers"])
        run(["arctl", "get", "agents"])
    finally:
        forward.terminate()


if __name__ == "__main__":
    main()
