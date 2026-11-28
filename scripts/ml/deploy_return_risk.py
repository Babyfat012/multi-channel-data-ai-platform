"""Deploy the model holding the `production` alias in MLflow to KServe (InferenceService return-risk).

Flow: MLflow alias → S3 artifact URI → KServe `STORAGE_URI`; the storage-initializer downloads the model at pod start.
The image only carries code, so changing the model needs no rebuild. Needs the MLflow client:

    uv run --no-project --with mlflow-skinny==3.17.0 python -m scripts.ml.deploy_return_risk

Env: MODEL_VERSION (deploy this version, bypassing the production gate: infra smoke tests only),
PLATFORM_STORAGE=minio (kind, default) | s3 (EKS: KCTX, REGISTRY=ECR, LAKEHOUSE_BUCKET, AWS_REGION),
TRAIN_IMAGE, TAG, MLFLOW_TRACKING_URI / MLFLOW_PORT. Values exported in the shell win over .env.
"""

import hashlib
import os
import sys

from scripts.common import ROOT, apply, env, kubectl, load_env, run

NS = "ml-serving"
CODE_FILES = [
    "feature_store/feature_store.yaml",
    "feature_store/.feastignore",
    "infra/docker/Dockerfile.serve",
]


def code_digest() -> str:
    """Hash of the code baked into the image: same code ⇒ same tag, changed code ⇒ new tag."""
    files = [
        *sorted((ROOT / "ml").glob("*.py")),
        *sorted((ROOT / "feature_store").glob("*.py")),
        *(ROOT / f for f in CODE_FILES),
    ]
    hasher = hashlib.sha256()
    for path in files:
        hasher.update(path.read_bytes())
    return hasher.hexdigest()[:8]


def mlflow_uri() -> str:
    """MLflow server: MLFLOW_TRACKING_URI, else the loopback port Compose publishes."""
    return (
        os.environ.get("MLFLOW_TRACKING_URI")
        or f"http://localhost:{os.environ.get('MLFLOW_PORT', '5050')}"
    )


def resolve_model(bucket: str) -> tuple[str, str]:
    """(version, storage URI) of the model to deploy; the URI is where MLflow stored model.lgb."""
    from mlflow import MlflowClient  # imported here: only this step needs MLflow

    client = MlflowClient(mlflow_uri())
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
    # The MLflow server runs with --artifacts-destination s3://<bucket>/mlflow (docker-compose*.yml).
    return version.version, f"s3://{bucket}/mlflow/{experiment}/models/{model_id}/artifacts/"


def cluster_objects(storage: str, region: str) -> list[dict]:
    """Namespace + ServiceAccount (+ MinIO Secret on kind) the InferenceService needs.

    The ServiceAccount is owned here, not by return-risk.yaml: on kind it lists the MinIO secret so the
    storage-initializer can read the model; on EKS Pod Identity binds by SA name, nothing to attach and no Secret.
    """
    namespace = {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": NS}}
    sa = {
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": {"name": "return-risk", "namespace": NS},
    }
    if storage == "s3":
        return [namespace, sa]
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
    return [namespace, secret, sa | {"secrets": [{"name": "platform-minio"}]}]


def main() -> None:
    """Resolve the model in MLflow, build + push the code image, then apply the InferenceService."""
    os.chdir(ROOT)
    load_env(override=False)
    storage = os.environ.get("PLATFORM_STORAGE") or "minio"
    registry = os.environ.get("REGISTRY") or "localhost:5001"
    if storage == "s3":
        bucket, region = env("LAKEHOUSE_BUCKET"), env("AWS_REGION")
        if ".amazonaws.com/" not in registry:
            sys.exit("s3: REGISTRY must be ECR")
    elif storage == "minio":
        bucket, region = env("MINIO_BUCKET"), "us-east-1"
    else:
        sys.exit(f"Unsupported PLATFORM_STORAGE: {storage}")
    template = (ROOT / "infra/k8s/ml/return-risk.yaml").read_text()
    if "@@STORAGE_URI@@" not in template or "kind: ServiceAccount" in template:
        sys.exit(
            "return-risk.yaml: add STORAGE_URI=@@STORAGE_URI@@ and drop the ServiceAccount (this script owns it)"
        )

    version, storage_uri = resolve_model(bucket)
    # Image = code only; tag = commit + code hash (reusing a tag makes kubelet keep the old image).
    code_tag = os.environ.get("TAG") or run(["git", "rev-parse", "HEAD"], capture=True)
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
            "infra/docker/Dockerfile.serve",
            "--build-arg",
            f"BASE_IMAGE={os.environ.get('TRAIN_IMAGE') or 'platform-train:0.1.0'}",
            "-t",
            source_image,
            ".",
        ],
        capture=True,
    )
    run(["docker", "tag", source_image, image])
    run(["docker", "push", image])

    kc = kubectl()
    apply(kc, {"apiVersion": "v1", "kind": "List", "items": cluster_objects(storage, region)})
    manifest = template
    for key, value in {
        "IMAGE": image,
        "VERSION": str(version),
        "STORAGE_URI": storage_uri,
        "FEAST_ROOT": f"s3://{bucket}/feature-store",
        "AWS_REGION": region,
    }.items():
        manifest = manifest.replace(f"@@{key}@@", value)
    if "@@" in manifest:
        sys.exit("return-risk.yaml has a placeholder this script does not fill")
    apply(kc, manifest)
    run(
        [
            *kc,
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
    main()
