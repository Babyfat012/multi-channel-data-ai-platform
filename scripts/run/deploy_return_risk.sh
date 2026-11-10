#!/usr/bin/env bash
# Model holding the `production` alias in MLflow → KServe image (model-inside-image) → registry → kind pull → apply.
# MODEL_VERSION=N is only for infra smoke tests with a SPECIFIC version (bypasses the production gate).
# Requires: MLflow (:5050; EKS via tunnel), the platform-train image (EKS: TRAIN_IMAGE=<ECR>/platform-train:<tag>),
# KServe installed. EKS: PLATFORM_STORAGE=s3 KCTX REGISTRY(ECR) LAKEHOUSE_BUCKET AWS_REGION.
set -euo pipefail
cd "$(dirname "$0")/../.."
set -a; . ./.env; set +a
ctx=$(mktemp -d); trap 'rm -rf "$ctx"' EXIT
# Download model.lgb through the server's proxy endpoint (the MLflow client by default requests a presigned URL pointing to
# minio:9000 — a name that only exists in the Docker network and the host cannot resolve).
version=$(uv run --no-project --with "mlflow-skinny==3.17.0" python - "$ctx" <<'PY'
import os, sys, urllib.request
from mlflow import MlflowClient
uri = os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:" + os.environ.get("MLFLOW_PORT", "5050"))
client = MlflowClient(uri)
pinned = os.environ.get("MODEL_VERSION")
v = client.get_model_version("return-risk", pinned) if pinned else client.get_model_version_by_alias("return-risk", "production")
if pinned:
    print(f"WARNING: deploying v{pinned} via MODEL_VERSION, bypassing the production gate", file=sys.stderr)
experiment = client.get_run(v.run_id).info.experiment_id
model_id = v.source.removeprefix("models:/")
url = f"{uri}/api/2.0/mlflow-artifacts/artifacts/{experiment}/models/{model_id}/artifacts/model.lgb"
urllib.request.urlretrieve(url, sys.argv[1] + "/model.lgb")
print(v.version)
PY
)
cp -r ml "$ctx/ml"
cp -r feature_repo "$ctx/feature_repo"
# Tag = model version + code/model hash: reusing an old tag makes kind/kubelet keep the old image for that tag.
digest=$(cat "$ctx/model.lgb" ml/*.py feature_repo/*.py feature_repo/feature_store.yaml feature_repo/.feastignore docker/Dockerfile.serve | sha256sum | cut -c1-8)
code_tag=${TAG:-$(git rev-parse HEAD)}
[[ "$code_tag" != latest ]] || { echo "Do not use TAG=latest" >&2; exit 1; }
REGISTRY="${REGISTRY:-localhost:5001}"
source_image="platform-return-risk:v$version-$code_tag-$digest"
image="$REGISTRY/$source_image"
docker build -q -f docker/Dockerfile.serve --build-arg MODEL_VERSION="$version" \
  --build-arg BASE_IMAGE="${TRAIN_IMAGE:-platform-train:0.1.0}" -t "$source_image" "$ctx"
docker tag "$source_image" "$image"
docker push "$image"
K="kubectl --context ${KCTX:-kind-platform}"
$K create namespace ml-serving --dry-run=client -o yaml | $K apply -f - >/dev/null
if [[ "${PLATFORM_STORAGE:-minio}" == s3 ]]; then
  # EKS: Feast reads the S3 registry via ml-serving/return-risk's Pod Identity; no MinIO Secret is created.
  feast_root="s3://${LAKEHOUSE_BUCKET:?LAKEHOUSE_BUCKET is required}/feature-store"
  region="${AWS_REGION:?AWS_REGION is required}"
  [[ "$REGISTRY" == *.amazonaws.com/* ]] || { echo "s3: REGISTRY must be ECR" >&2; exit 1; }
else
  feast_root="s3://platform/feature-store"
  region="us-east-1"
  $K -n ml-serving create secret generic platform-minio --from-literal=AWS_ACCESS_KEY_ID="$MINIO_ROOT_USER" \
    --from-literal=AWS_SECRET_ACCESS_KEY="$MINIO_ROOT_PASSWORD" \
    --from-literal=FEAST_S3_ENDPOINT_URL=http://minio:9000 --from-literal=AWS_ENDPOINT_URL_S3=http://minio:9000 \
    --dry-run=client -o yaml | $K apply -f - >/dev/null
fi
sed -e "s|@@IMAGE@@|$image|" -e "s|@@VERSION@@|$version|" -e "s|@@FEAST_ROOT@@|$feast_root|g" \
  -e "s|@@AWS_REGION@@|$region|" infra/k8s/ml/return-risk.yaml | $K apply -f -
$K -n ml-serving wait --for=condition=Ready inferenceservice/return-risk --timeout=300s
echo "deployed return-risk v$version ($image)"
