"""Create the kind cluster `platform` + Redis (Feast online store), then attach the node to the Compose network
so Airflow can reach platform-control-plane:30379. Safe to rerun.

    python3 -m scripts.ml.setup_kind_feature_store
"""

import os

from scripts.common import ROOT, kubectl, run, succeeds


def main() -> None:
    """Cluster (if missing), Redis, network attachment."""
    os.chdir(ROOT)
    if "platform" not in run(["kind", "get", "clusters"], capture=True).split():
        run(["kind", "create", "cluster", "--config", "infra/k8s/kind-config.yaml"])
    kc = kubectl("kind-platform")
    run([*kc, "apply", "-f", "infra/k8s/feature-store/redis.yaml"])
    run([*kc, "-n", "feature-store", "rollout", "status", "deploy/redis", "--timeout=180s"])
    succeeds(
        ["docker", "network", "connect", "platform_default", "platform-control-plane"]
    )  # already connected = ok
    print("Redis ready: platform-control-plane:30379 (from the platform_default network)")


if __name__ == "__main__":
    main()
