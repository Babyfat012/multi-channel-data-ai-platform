"""Publish the MCP server and the agents to agentregistry (governance), versioned by tag.

Each entry is applied with `metadata.tag` set: CI publishes a candidate under the commit SHA before the gateway is
updated, then the same entries under `stable` once the agent eval passed. `arctl get agent <name> --all-tags` lists the
history. Requires arctl v0.4.0 (matching the server); reached through a port-forward.

    python3 -m scripts.llm.publish_agentregistry --tag <sha|stable> [NAME ...]   (default: all four entries)
"""

import argparse
import contextlib
import os
import re
import subprocess
import tempfile
import time
import urllib.request

from scripts.common import ROOT, kubectl, run

REGISTRY = ROOT / "infra/k8s/agents/registry"
# MCP server first: the agents reference it.
ENTRIES = ("feature-tools", "data-agent", "model-agent", "coordinator")
TAG = re.compile(r"[a-z0-9][a-z0-9.-]{0,62}")


@contextlib.contextmanager
def registry():
    """Port-forward agentregistry and point arctl at it for the duration of the block."""
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
        yield
    finally:
        forward.terminate()


def tagged(manifest: str, tag: str) -> str:
    """The registry manifest with `metadata.tag` set (inserted right under `metadata:`)."""
    if not TAG.fullmatch(tag):
        raise ValueError(
            f"invalid tag {tag!r}: lowercase letters, digits, '.' and '-', at most 63 characters"
        )
    if re.search(r"^  tag:", manifest, re.M):
        raise ValueError("the manifest sets metadata.tag itself; the publish step owns it")
    return re.sub(r"^metadata:\n", f"metadata:\n  tag: {tag}\n", manifest, count=1, flags=re.M)


def publish(tag: str, names: tuple[str, ...] = ENTRIES) -> None:
    """Apply each entry under `tag` (call inside `registry()`)."""
    for name in names:
        with tempfile.NamedTemporaryFile("w", suffix=".yaml") as manifest:
            manifest.write(tagged((REGISTRY / f"{name}.yaml").read_text(), tag))
            manifest.flush()
            run(["arctl", "apply", "-f", manifest.name])


def main() -> None:
    """Publish the chosen entries under one tag, then list what the registry holds for that tag."""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--tag", required=True, help="commit SHA (candidate) or 'stable' (passed the agent eval)"
    )
    parser.add_argument(
        "names", nargs="*", help=f"entries to publish (default: all of {', '.join(ENTRIES)})"
    )
    args = parser.parse_args()
    unknown = set(args.names) - set(ENTRIES)
    if unknown:
        parser.error(f"unknown entries {sorted(unknown)}; choose from {', '.join(ENTRIES)}")
    names = tuple(
        n for n in ENTRIES if n in (args.names or ENTRIES)
    )  # keep the MCP-server-first order
    os.chdir(ROOT)
    with registry():
        publish(args.tag, names)
        run(["arctl", "get", "mcps", "--tag", args.tag])
        run(["arctl", "get", "agents", "--tag", args.tag])


if __name__ == "__main__":
    main()
