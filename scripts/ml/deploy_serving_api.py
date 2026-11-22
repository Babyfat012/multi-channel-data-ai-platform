"""Deploy the serving API: three Helm releases of one chart/image in namespace serving-api.

  feature-api  SKU features from Feast/Redis       env: Feast registry + Redis (+ MinIO keys on kind)
  policy-api   policy chunks from Feast/pgvector   env: RAG registry + pgvector account (+ MinIO keys on kind)
  mcp-server   agent tools over HTTP               env: the URLs of the two APIs and KServe, no data credentials

Each release gets its own Secret <release>-env holding only what that service needs. S3 (EKS) goes through Pod
Identity per ServiceAccount; MinIO (kind) uses local keys. Secrets reach the cluster via stdin, never argv. All inputs are
validated before anything in the cluster changes.

    python3 -m scripts.ml.deploy_serving_api [TAG]       (default TAG env, else the current commit)

Env: PLATFORM_STORAGE=minio (default) | s3 (+ KCTX, REGISTRY=ECR prefix, AWS_REGION, LAKEHOUSE_BUCKET),
POSTGRES_USER/POSTGRES_PASSWORD (pgvector), SKIP_BUILD_PUSH=1, REPLICAS (2), SERVING_NAMESPACE (serving-api),
optional overrides FEAST_REGISTRY, FEAST_REDIS, FEAST_PUBLICATION_ROOT, FEAST_RAG_REGISTRY, RAG_PG_HOST/PORT, KSERVE_URL.
"""

import hashlib
import os
import sys

from scripts.common import ROOT, apply, env, load_env, run

CHART = "infra/helm/serving-api"
IMAGE = "platform-serving-api"
RELEASES = {"feature-api": "features", "policy-api": "policy", "mcp-server": "mcp"}
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


def secret_envs(storage: str, namespace: str) -> dict[str, dict[str, str]]:
    """Env per release (Secret <release>-env): each service only gets the credentials it uses."""
    e = os.environ
    cloud = storage == "s3"
    bucket = e["LAKEHOUSE_BUCKET"] if cloud else e.get("MINIO_BUCKET", "platform")
    root = f"s3://{bucket}/feature-store"
    if cloud:
        storage_access = {"AWS_REGION": e["AWS_REGION"], "AWS_DEFAULT_REGION": e["AWS_REGION"]}
    else:
        storage_access = {
            "AWS_ACCESS_KEY_ID": env("MINIO_ROOT_USER"),
            "AWS_SECRET_ACCESS_KEY": env("MINIO_ROOT_PASSWORD"),
            "FEAST_S3_ENDPOINT_URL": e.get("FEAST_S3_ENDPOINT_URL", "http://minio:9000"),
        }
    features = {
        "FEAST_REGISTRY": e.get("FEAST_REGISTRY", f"{root}/registry.db"),
        "FEAST_REDIS": e.get("FEAST_REDIS", "redis.feature-store.svc.cluster.local:6379"),
        "FEAST_PUBLICATION_ROOT": e.get("FEAST_PUBLICATION_ROOT", f"{root}/publications"),
        **storage_access,
    }
    policy = {
        "RAG_PG_HOST": e.get("RAG_PG_HOST", "pgvector.rag.svc.cluster.local"),
        "RAG_PG_PORT": e.get("RAG_PG_PORT", "5432"),
        "RAG_PG_USER": env("POSTGRES_USER"),
        "RAG_PG_PASSWORD": env("POSTGRES_PASSWORD"),
        **storage_access,
    }
    # Cloud: RAG only when FEAST_RAG_REGISTRY is passed. A missing file makes Feast create a registry (an S3 write)
    # while Pod Identity is read-only ⇒ the app dies on startup. Without it policy-api stays not Ready.
    if e.get("FEAST_RAG_REGISTRY") or not cloud:
        policy["FEAST_RAG_REGISTRY"] = e.get("FEAST_RAG_REGISTRY", f"{root}/rag-registry.db")
    services = f"{namespace}.svc.cluster.local:8000"
    mcp = {
        "FEATURE_API_URL": f"http://feature-api.{services}",
        "POLICY_API_URL": f"http://policy-api.{services}",
        "KSERVE_URL": e.get("KSERVE_URL", KSERVE_URL),
    }
    envs = {"feature-api": features, "policy-api": policy, "mcp-server": mcp}
    for release, values in envs.items():
        for key, value in values.items():
            if "\n" in value or "\r" in value:
                sys.exit(f"Multiline value is not supported for {key} ({release})")
    return envs


def main() -> None:
    """Validate, (build + push), write each Secret, then upgrade each release with automatic rollback."""
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
    namespace = os.environ.get("SERVING_NAMESPACE") or "serving-api"
    env_files = {
        release: "".join(f"{k}={v}\n" for k, v in values.items())
        for release, values in secret_envs(storage, namespace).items()
    }

    if os.environ.get("SKIP_BUILD_PUSH") != "1":
        run(
            [
                "docker",
                "build",
                "-f",
                "infra/docker/Dockerfile.serving",
                "-t",
                f"{IMAGE}:{tag}",
                ".",
            ]
        )
        run([sys.executable, "-m", "scripts.shared.push_images", f"{IMAGE}:{tag}"])
    kc = ["kubectl", "--context", kctx, "-n", namespace]
    apply(kc, {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": namespace}})
    helm = ["helm", "--kube-context", kctx, "-n", namespace]
    aws_values = ["-f", f"{CHART}/values-aws.yaml"] if storage == "s3" else []
    for release, service in RELEASES.items():
        secret = run(
            [
                *kc,
                "create",
                "secret",
                "generic",
                f"{release}-env",
                "--from-env-file=/dev/stdin",
                "--dry-run=client",
                "-o",
                "yaml",
            ],
            stdin=env_files[release],
            capture=True,
        )
        apply(kc, secret)
        # Env fingerprint in the pod template: a Secret change alone does not replace pods, a template change does.
        checksum = hashlib.sha256(env_files[release].encode()).hexdigest()[:16]
        run(
            [
                *helm,
                "upgrade",
                "--install",
                release,
                CHART,
                "-f",
                f"{CHART}/values-{service}.yaml",
                *aws_values,
                "--set-string",
                f"image.repository={registry.rstrip('/')}/{IMAGE}",
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
        )
        run([*helm, "history", release, "--max", "5"])
    print(
        f"Serving API {tag} deployed to {kctx}/{namespace} ({', '.join(RELEASES)}); "
        "verify /readyz through the ingress or a port-forward."
    )


if __name__ == "__main__":
    main()
