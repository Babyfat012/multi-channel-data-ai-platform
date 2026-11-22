"""Bring back the shared ML/LLM base after a machine restart or an accidental `docker compose stop`.

MinIO + PostgreSQL + Lakekeeper, kind registry, Vault (unseal), Jenkins, then reload Redis when it is empty (Redis in kind
does not persist to disk). Does not start the LLM/agents (RAM-hungry). Safe to rerun.

    python3 -m scripts.shared.up_ml_base
"""

import os

from scripts.common import ROOT, kubectl, run
from scripts.shared import setup_jenkins, setup_kind_registry, setup_vault


def redis_keys() -> str:
    """Number of keys in the online store (Redis in ns feature-store)."""
    return run(
        [*kubectl(), "-n", "feature-store", "exec", "deploy/redis", "--", "redis-cli", "dbsize"],
        capture=True,
    )


def main() -> None:
    """Start each piece in dependency order."""
    os.chdir(ROOT)
    run(
        [
            "docker",
            "compose",
            "--profile",
            "catalog",
            "up",
            "-d",
            "--wait",
            "minio",
            "postgres",
            "lakekeeper",
        ],
        capture=True,
    )
    setup_kind_registry.main()
    setup_vault.main()
    setup_jenkins.main()
    keys = redis_keys()
    if keys == "0":
        cutoff = os.environ.get(
            "FEAST_ONLINE_END", "2026-10-13T00:00:00"
        )  # online snapshot point = Gold Cutoff
        run(
            [
                "docker",
                "compose",
                "--profile",
                "ml-stream",
                "run",
                "--rm",
                "--no-deps",
                "feast-pusher",
                "feast",
                "materialize",
                "2025-12-01T00:00:00",
                cutoff,
                "--views",
                "sku_return_daily",
            ],
            capture=True,
        )
        keys = redis_keys()
    print(f"ML Platform base ready: Redis {keys} keys, Vault unsealed, Jenkins :8090")


if __name__ == "__main__":
    main()
