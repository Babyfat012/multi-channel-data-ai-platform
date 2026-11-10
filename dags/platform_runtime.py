"""Pure mapping from Airflow metadata to environment variables for Spark jobs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

_GROUPS = {
    "iceberg": "platform_minio",
    "postgres": "platform_postgres",
    "kafka": "platform_kafka",
}


def resolve_runtime_env(
    connections: Mapping[str, Mapping[str, object]],
    variables: Mapping[str, str],
    groups: Sequence[str],
    *,
    storage: str = "minio",
) -> dict[str, str]:
    """Build a minimal Spark environment from selected Airflow connection groups."""
    unknown = set(groups) - _GROUPS.keys()
    if unknown:
        raise ValueError(f"unknown runtime connection group(s): {', '.join(sorted(unknown))}")
    if storage not in {"minio", "s3"}:
        raise ValueError(f"unknown runtime storage: {storage!r}")

    env: dict[str, str] = {}
    if "iceberg" in groups:
        env.update(
            {
                "WAREHOUSE_NAME": variables["platform_warehouse_name"],
                "LAKEKEEPER_CATALOG_URI": variables["platform_lakekeeper_catalog_uri"],
            }
        )
        if storage == "minio":
            conn = connections[_GROUPS["iceberg"]]
            env.update(
                {
                    "AWS_ACCESS_KEY_ID": str(conn["login"]),
                    "AWS_SECRET_ACCESS_KEY": str(conn["password"]),
                    "MINIO_INTERNAL_ENDPOINT": variables["platform_minio_internal_endpoint"],
                }
            )
    if "postgres" in groups:
        conn = connections[_GROUPS["postgres"]]
        env.update(
            {
                "POSTGRES_HOST": str(conn["host"]),
                "POSTGRES_PORT": str(conn["port"] or 5432),
                "POSTGRES_DB": str(conn["schema"]),
                "POSTGRES_USER": str(conn["login"]),
                "POSTGRES_PASSWORD": str(conn["password"]),
            }
        )
    if "kafka" in groups:
        conn = connections[_GROUPS["kafka"]]
        env.update(
            {
                "KAFKA_BOOTSTRAP_SERVERS": f"{conn['host']}:{conn['port'] or 9092}",
                "KAFKA_TOPIC": variables["platform_dp1_cdc_topic"],
            }
        )
    return env


def resolve_rag_env(
    connections: Mapping[str, Mapping[str, object]],
    variables: Mapping[str, str],
    *,
    storage: str = "minio",
) -> dict[str, str]:
    """Env for `rag.pipeline`: pgvector + RAG registry; MinIO needs keys, S3 (AWS) uses the machine's IAM role."""
    if storage not in {"minio", "s3"}:
        raise ValueError(f"unknown runtime storage: {storage!r}")
    pg = connections["platform_rag_pg"]
    if not pg.get("host"):
        # AWS: pgvector is a NodePort on an EKS node, the IP changes whenever the node is replaced ⇒ Ansible -e rag_pg=<IP>:30432.
        raise ValueError(
            "platform_rag_pg has no host: run Ansible with -e rag_pg=<EKS node IP>:30432"
        )
    root = variables["platform_rag_root"]
    env = {
        "RAG_ROOT": root,
        "FEAST_RAG_REGISTRY": variables["platform_feast_root"] + "/rag-registry.db",
        "FEAST_RAG_CHUNKS_PATH": f"{root}/chunks.parquet",
        "RAG_PG_HOST": str(pg["host"]),
        "RAG_PG_PORT": str(pg["port"] or 5432),
        "RAG_PG_USER": str(pg["login"]),
        "RAG_PG_PASSWORD": str(pg["password"]),
    }
    if storage == "minio":
        minio = connections["platform_minio"]
        env.update(
            {
                "AWS_ACCESS_KEY_ID": str(minio["login"]),
                "AWS_SECRET_ACCESS_KEY": str(minio["password"]),
                "FEAST_S3_ENDPOINT_URL": variables["platform_minio_internal_endpoint"],
            }
        )
    return env
