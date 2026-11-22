"""Deploy the Feature API (REST) and the MCP server, two Helm releases of one chart/image/Secret.

S3 (EKS) reads storage through Pod Identity; MinIO (kind) uses local credentials. Secrets reach the cluster via stdin,
never argv. All inputs are validated before anything in the cluster changes.

    python3 -m scripts.ml.deploy_feature_api [TAG]       (default TAG env, else the current commit)

Env: PLATFORM_STORAGE=minio (default) | s3 (+ KCTX, REGISTRY=ECR prefix, AWS_REGION, LAKEHOUSE_BUCKET),
POSTGRES_USER/POSTGRES_PASSWORD (RAG DB), SKIP_BUILD_PUSH=1, REPLICAS (2), FEATURE_API_NAMESPACE (feature-store),
optional overrides FEAST_REGISTRY, FEAST_REDIS, FEAST_PUBLICATION_ROOT, FEAST_RAG_REGISTRY, RAG_PG_HOST/PORT, KSERVE_URL.
"""

import hashlib
import os
import sys

from scripts.common import ROOT, apply, env, load_env, run

CHART = "infra/helm/feature-api"
KSERVE_URL = (
    "http://return-risk-predictor.ml-serving.svc.cluster.local/v1/models/return-risk:predict"
)


def target(storage: str) -> tuple[str, str]:
    """(kube context, registry) for the storage mode; a cloud deploy must name a cloud context and registry."""
    if storage == "minio":
        return os.environ.get("KCTX") or "kind-platform", os.environ.get(
            "REGISTRY"
        ) or "localhost:5001"
    if storage != "s3":
        sys.exit(f"Unsupported PLATFORM_STORAGE: {storage}")
    kctx, registry = env("KCTX"), env("REGISTRY")
    env("AWS_REGION"), env("LAKEHOUSE_BUCKET")
    if kctx.startswith("kind-") or registry.startswith("localhost"):
        sys.exit("S3 cloud deployment requires a cloud context and registry.")
    return kctx, registry


def secret_env(storage: str) -> dict[str, str]:
    """Env for the pods (Secret feature-api-env). Cloud: no static keys, region instead."""
    e = os.environ
    cloud = storage == "s3"
    bucket = e["LAKEHOUSE_BUCKET"] if cloud else e.get("MINIO_BUCKET", "platform")
    values = {
        "FEAST_REGISTRY": e.get("FEAST_REGISTRY", f"s3://{bucket}/feature-store/registry.db"),
        "FEAST_REDIS": e.get("FEAST_REDIS", "redis.feature-store.svc.cluster.local:6379"),
        "FEAST_PUBLICATION_ROOT": e.get(
            "FEAST_PUBLICATION_ROOT", f"s3://{bucket}/feature-store/publications"
        ),
        "FEAST_RAG_REGISTRY": e.get(
            "FEAST_RAG_REGISTRY", f"s3://{bucket}/feature-store/rag-registry.db"
        ),
        "RAG_PG_HOST": e.get("RAG_PG_HOST", "pgvector.rag.svc.cluster.local"),
        "RAG_PG_PORT": e.get("RAG_PG_PORT", "5432"),
        "RAG_PG_USER": env("POSTGRES_USER"),
        "RAG_PG_PASSWORD": env("POSTGRES_PASSWORD"),
        "KSERVE_URL": e.get("KSERVE_URL", KSERVE_URL),
    }
    if cloud:
        # RAG only when FEAST_RAG_REGISTRY is passed: a missing file makes Feast create a registry (an S3 write) while
        # Pod Identity is read-only ⇒ the app dies on startup. Without it /chunks returns 503 and the rest runs.
        if not e.get("FEAST_RAG_REGISTRY"):
            del values["FEAST_RAG_REGISTRY"]
        values["AWS_REGION"] = values["AWS_DEFAULT_REGION"] = e["AWS_REGION"]
    else:
        values |= {
            "AWS_ACCESS_KEY_ID": env("MINIO_ROOT_USER"),
            "AWS_SECRET_ACCESS_KEY": env("MINIO_ROOT_PASSWORD"),
            "FEAST_S3_ENDPOINT_URL": e.get("FEAST_S3_ENDPOINT_URL", "http://minio:9000"),
        }
    for key, value in values.items():
        if "\n" in value or "\r" in value:
            sys.exit(f"Multiline value is not supported for {key}")
    return values


def main() -> None:
    """Validate, (build + push), write the Secret, then upgrade both releases with automatic rollback."""
    os.chdir(ROOT)
    storage = os.environ.get("PLATFORM_STORAGE") or "minio"
    if storage == "minio" and not os.environ.get("MINIO_ROOT_USER"):
        load_env(required=False)
    kctx, registry = target(storage)
    tag = (
        (sys.argv[1] if len(sys.argv) > 1 else "")
        or os.environ.get("TAG")
        or run(["git", "rev-parse", "HEAD"], capture=True)
    )
    if tag == "latest":
        sys.exit("Use an immutable TAG.")
    namespace = os.environ.get("FEATURE_API_NAMESPACE") or "feature-store"
    env_file = "".join(f"{k}={v}\n" for k, v in secret_env(storage).items())

    if os.environ.get("SKIP_BUILD_PUSH") != "1":
        run(
            [
                "docker",
                "build",
                "-f",
                "infra/docker/Dockerfile.serving",
                "-t",
                f"platform-feature-api:{tag}",
                ".",
            ]
        )
        run([sys.executable, "-m", "scripts.shared.push_images", f"platform-feature-api:{tag}"])
    kc = ["kubectl", "--context", kctx, "-n", namespace]
    apply(kc, {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": namespace}})
    secret = run(
        [
            *kc,
            "create",
            "secret",
            "generic",
            "feature-api-env",
            "--from-env-file=/dev/stdin",
            "--dry-run=client",
            "-o",
            "yaml",
        ],
        stdin=env_file,
        capture=True,
    )
    apply(kc, secret)
    # Env fingerprint in the pod template: a Secret change alone does not replace pods, a template change does.
    checksum = hashlib.sha256(env_file.encode()).hexdigest()[:16]
    helm = ["helm", "--kube-context", kctx, "-n", namespace]
    aws_values = ["-f", f"{CHART}/values-aws.yaml"] if storage == "s3" else []
    common = [
        "--set-string",
        f"image.repository={registry.rstrip('/')}/platform-feature-api",
        "--set-string",
        f"image.tag={tag}",
        "--set",
        f"replicas={os.environ.get('REPLICAS') or 2}",
        "--set-string",
        f"envChecksum={checksum}",
        "--atomic",
        "--timeout",
        "4m",
    ]
    # MCP server: same chart/image/Secret, API_SURFACE=mcp; values-mcp.yaml last so it reuses the ServiceAccount.
    for release, extra in (
        ("feature-api", []),
        ("feature-mcp", ["-f", f"{CHART}/values-mcp.yaml"]),
    ):
        run([*helm, "upgrade", "--install", release, CHART, *aws_values, *extra, *common])
        run([*helm, "history", release, "--max", "5"])
    print(
        f"Feature API + MCP server {tag} deployed to {kctx}/{namespace}; "
        "verify /readyz through the configured ingress or port-forward."
    )


if __name__ == "__main__":
    main()
