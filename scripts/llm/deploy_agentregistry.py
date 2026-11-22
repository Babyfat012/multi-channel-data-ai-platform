"""Install agentregistry (ns agentregistry) with its database `agentregistry` on pgvector (ns rag).

python3 -m scripts.llm.deploy_agentregistry          (KCTX, default kind-platform)
"""

import os

from scripts.common import ROOT, apply, kubectl, run, secret_manifest, secret_value


def pgvector_database_url(kc: list[str], database: str) -> str:
    """Create the database on pgvector if missing and return its URL.

    The account is pgvector's own (Secret rag/rag-pg), never .env or shell variables: any drift locks the app out.
    """
    user = secret_value(kc, "rag", "rag-pg", "POSTGRES_USER")
    password = secret_value(kc, "rag", "rag-pg", "POSTGRES_PASSWORD")
    create = (
        f'psql -U "$POSTGRES_USER" -d rag -tc "SELECT 1 FROM pg_database WHERE datname=\'{database}\'" '
        f"| grep -q 1 || psql -U \"$POSTGRES_USER\" -d rag -c 'CREATE DATABASE {database}'"
    )
    run([*kc, "-n", "rag", "exec", "deploy/pgvector", "--", "sh", "-c", create])
    return f"{user}:{password}@pgvector.rag.svc.cluster.local:5432/{database}"


def main() -> None:
    """DB + Secret, then the pinned Helm chart."""
    os.chdir(ROOT)
    kc = kubectl()
    url = f"postgres://{pgvector_database_url(kc, 'agentregistry')}?sslmode=disable"
    apply(kc, {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "agentregistry"}})
    apply(
        kc,
        secret_manifest("agentregistry", "agentregistry-db", {"AGENT_REGISTRY_DATABASE_URL": url}),
    )
    run(
        [
            "helm",
            "--kube-context",
            kc[2],
            "-n",
            "agentregistry",
            "upgrade",
            "--install",
            "agentregistry",
            "oci://ghcr.io/agentregistry-dev/agentregistry/charts/agentregistry",
            "--version",
            "0.4.0",
            "-f",
            "infra/k8s/agents/agentregistry-values.yaml",
            "--wait",
            "--timeout",
            "5m",
        ]
    )
    run([*kc, "-n", "agentregistry", "get", "pods,svc"])


if __name__ == "__main__":
    main()
