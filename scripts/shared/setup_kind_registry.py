"""Connect an EXISTING kind cluster to the local registry (localhost:5001 → compose service registry:5000).

Never creates or deletes nodes. A node without containerd `config_path` gets it enabled once and containerd restarted
(running containers survive thanks to the shim; kubelet reconnects). See https://kind.sigs.k8s.io/docs/user/local-registry/.

    python3 -m scripts.shared.setup_kind_registry          (KIND_CLUSTER, default platform)
"""

import os
import re
import sys
import time

from scripts.common import apply, kubectl, run, succeeds

CONTAINERD_CONFIG = """
[plugins."io.containerd.grpc.v1.cri".registry]
  config_path = "/etc/containerd/certs.d"
"""
HOSTS_TOML = """server = "http://registry:5000"
[host."http://registry:5000"]
  capabilities = ["pull", "resolve"]
"""
REGISTRY_CONFIGMAP = """apiVersion: v1
kind: ConfigMap
metadata:
  name: local-registry-hosting
  namespace: kube-public
data:
  localRegistryHosting.v1: |
    host: "localhost:5001"
    help: "https://kind.sigs.k8s.io/docs/user/local-registry/"
"""


def enable_config_path(node: str) -> None:
    """Turn on containerd's certs.d lookup once per node, then wait until containerd answers again."""
    config = run(["docker", "exec", node, "containerd", "config", "dump"], capture=True)
    if re.search(r"config_path = [\"']/etc/containerd/certs\.d", config):
        return
    print(f"{node}: enabling config_path=/etc/containerd/certs.d, then restarting containerd")
    run(
        ["docker", "exec", "-i", node, "sh", "-c", "cat >> /etc/containerd/config.toml"],
        stdin=CONTAINERD_CONFIG,
    )
    run(["docker", "exec", node, "systemctl", "restart", "containerd"])
    while not succeeds(["docker", "exec", node, "crictl", "info"]):
        time.sleep(2)


def main() -> None:
    """Point every node at the registry and publish the local-registry-hosting ConfigMap."""
    cluster = os.environ.get("KIND_CLUSTER", "platform")
    nodes = run(["kind", "get", "nodes", "--name", cluster], capture=True).split()
    if not nodes:
        sys.exit(f"Cluster {cluster} does not exist yet; leaving infrastructure untouched.")
    for node in nodes:
        enable_config_path(node)
    run(["docker", "compose", "--profile", "ml", "up", "-d", "--wait", "registry"])
    for node in nodes:
        run(["docker", "exec", node, "mkdir", "-p", "/etc/containerd/certs.d/localhost:5001"])
        run(
            [
                "docker",
                "exec",
                "-i",
                node,
                "cp",
                "/dev/stdin",
                "/etc/containerd/certs.d/localhost:5001/hosts.toml",
            ],
            stdin=HOSTS_TOML,
        )
    apply(kubectl(f"kind-{cluster}"), REGISTRY_CONFIGMAP)
    print(f"Registry localhost:5001 → registry:5000, configured {cluster}")


if __name__ == "__main__":
    main()
