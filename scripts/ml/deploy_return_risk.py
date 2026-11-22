#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["mlflow-skinny==3.17.0", "python-dotenv==1.1.1"]
# ///
"""Deploy the model holding the `production` alias in MLflow to KServe.

Flow: MLflow alias → S3 artifact URI → KServe `STORAGE_URI` (storage-initializer downloads it at pod start).
The image only carries code: changing the model needs no rebuild.

Env:
  MODEL_VERSION     deploy this version, bypassing the `production` gate (infra smoke tests only)
  PLATFORM_STORAGE  minio (kind, default) | s3 (EKS: also KCTX, REGISTRY=ECR, LAKEHOUSE_BUCKET, AWS_REGION)
  TRAIN_IMAGE, TAG, MLFLOW_TRACKING_URI / MLFLOW_PORT
"""

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv
from mlflow import MlflowClient

ROOT = Path(__file__).resolve().parents[2]
NS = "ml-serving"


def run(cmd: list[str], stdin: str | None = None) -> str:
    """Run a command and stop on the first error (like `set -e`)."""
    return subprocess.run(
        cmd, input=stdin, text=True, capture_output=True, check=True
    ).stdout.strip()


def env(key: str, default: str | None = None) -> str:
    """Read an env var; stop with a clear message when it is required and missing."""
    value = os.environ.get(key, default)
    if not value:
        sys.exit(f"{key} is required")
    return value


def code_digest() -> str:
    """Hash of the code baked into the image: same code ⇒ same tag, changed code ⇒ new tag."""
    files = [
        *sorted((ROOT / "ml").glob("*.py")),
        *sorted((ROOT / "feature_repo").glob("*.py")),
        ROOT / "feature_repo/feature_store.yaml",
        ROOT / "feature_repo/.feastignore",
        ROOT / "docker/Dockerfile.serve",
    ]
    hasher = hashlib.sha256()
    for path in files:
        hasher.update(path.read_bytes())
    return hasher.hexdigest()[:8]


def resolve_model(bucket: str) -> tuple[str, str]:
    """Return (version, storage URI) of the model to deploy; the URI is where MLflow stored model.lgb."""
    uri = os.environ.get(
        "MLFLOW_TRACKING_URI", "http://localhost:" + os.environ.get("MLFLOW_PORT", "5050")
    )
    client = MlflowClient(uri)
    pinned = os.environ.get("MODEL_VERSION")
    if pinned:
        print(
            f"WARNING: deploying v{pinned} via MODEL_VERSION, bypassing the production gate",
            file=sys.stderr,
        )
        version = client.get_model_version("return-risk", pinned)
    else:
        version = client.get_model_version_by_alias("return-risk", "production")
    experiment = client.get_run(version.run_id).info.experiment_id
    model_id = version.source.removeprefix("models:/")
    # MLflow server runs with --artifacts-destination s3://<bucket>/mlflow (docker-compose*.yml).
    return version.version, f"s3://{bucket}/mlflow/{experiment}/models/{model_id}/artifacts/"


def apply(kubectl: list[str], *objects: dict) -> None:
    """kubectl apply via stdin: secrets never appear in process argv."""
    run(
        [*kubectl, "apply", "-f", "-"],
        stdin=json.dumps({"apiVersion": "v1", "kind": "List", "items": list(objects)}),
    )


def main() -> None:
    os.chdir(ROOT)
    load_dotenv(ROOT / ".env")  # values already exported in the shell win over .env

    storage = os.environ.get("PLATFORM_STORAGE", "minio")
    registry = os.environ.get("REGISTRY", "localhost:5001")
    if storage == "s3":
        bucket, region = env("LAKEHOUSE_BUCKET"), env("AWS_REGION")
        if ".amazonaws.com/" not in registry:
            sys.exit("s3: REGISTRY must be ECR")
    elif storage == "minio":
        bucket, region = env("MINIO_BUCKET"), "us-east-1"
    else:
        sys.exit(f"Unsupported PLATFORM_STORAGE: {storage}")

    version, storage_uri = resolve_model(bucket)

    # Image = code only; tag = commit + code hash (reusing a tag makes kubelet keep the old image).
    code_tag = os.environ.get("TAG") or run(["git", "rev-parse", "HEAD"])
    if code_tag == "latest":
        sys.exit("Do not use TAG=latest")
    source_image = f"platform-return-risk:{code_tag}-{code_digest()}"
    image = f"{registry}/{source_image}"
    run(
        [
            "docker",
            "build",
            "-q",
            "-f",
            "docker/Dockerfile.serve",
            "--build-arg",
            f"BASE_IMAGE={os.environ.get('TRAIN_IMAGE', 'platform-train:0.1.0')}",
            "-t",
            source_image,
            ".",
        ]
    )
    run(["docker", "tag", source_image, image])
    run(["docker", "push", image])

    kubectl = ["kubectl", "--context", os.environ.get("KCTX", "kind-platform")]
    namespace = {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": NS}}
    # The ServiceAccount is owned here, not by return-risk.yaml: on kind it must list the MinIO secret so
    # the storage-initializer can read the model; on EKS, Pod Identity binds by SA name, nothing to attach.
    sa = {
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": {"name": "return-risk", "namespace": NS},
    }
    if storage == "s3":
        apply(kubectl, namespace, sa)
    else:
        secret = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": "platform-minio",
                "namespace": NS,
                "annotations": {
                    "serving.kserve.io/s3-endpoint": "minio:9000",
                    "serving.kserve.io/s3-usehttps": "0",
                    "serving.kserve.io/s3-region": region,
                },
            },
            "stringData": {
                "AWS_ACCESS_KEY_ID": env("MINIO_ROOT_USER"),
                "AWS_SECRET_ACCESS_KEY": env("MINIO_ROOT_PASSWORD"),
                "FEAST_S3_ENDPOINT_URL": "http://minio:9000",
                "AWS_ENDPOINT_URL_S3": "http://minio:9000",
            },
        }
        apply(kubectl, namespace, secret, sa | {"secrets": [{"name": "platform-minio"}]})

    template = (ROOT / "infra/k8s/ml/return-risk.yaml").read_text()
    if "@@STORAGE_URI@@" not in template or "kind: ServiceAccount" in template:
        sys.exit(
            "return-risk.yaml: add STORAGE_URI=@@STORAGE_URI@@ and drop the ServiceAccount (this script owns it)"
        )
    manifest = (
        template.replace("@@IMAGE@@", image)
        .replace("@@VERSION@@", str(version))
        .replace("@@STORAGE_URI@@", storage_uri)
        .replace("@@FEAST_ROOT@@", f"s3://{bucket}/feature-store")
        .replace("@@AWS_REGION@@", region)
    )
    if "@@" in manifest:
        sys.exit("return-risk.yaml has a placeholder this script does not fill")
    run([*kubectl, "apply", "-f", "-"], stdin=manifest)
    run(
        [
            *kubectl,
            "-n",
            NS,
            "wait",
            "--for=condition=Ready",
            "inferenceservice/return-risk",
            "--timeout=300s",
        ]
    )
    print(f"deployed return-risk v{version} from {storage_uri} ({image})")


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as exc:
        sys.exit(f"{' '.join(exc.cmd[:3])}… failed:\n{exc.stderr}")
