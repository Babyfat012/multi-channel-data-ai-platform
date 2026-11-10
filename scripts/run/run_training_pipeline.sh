#!/usr/bin/env bash
# Compile ml/training_pipeline.py, then submit a run to KFP: kind (MinIO, Secret from .env) or
# PIPELINE_TARGET=aws (EKS: ECR, S3 via Pod Identity, MLflow on EC2).
# Requires: KFP (infra/k8s/ml), MLflow (`docker compose --profile ml up -d mlflow`), a registry configured for kind.
set -euo pipefail
cd "$(dirname "$0")/../.."
set -a; . ./.env; set +a
export REGISTRY="${REGISTRY:-localhost:5001}"
export TAG="${TAG:-$(git rev-parse HEAD)}"
[[ "$TAG" != latest ]] || { echo 'TAG must be immutable, do not use latest' >&2; exit 1; }
# SKIP_BUILD_PUSH=1: image TAG already exists in the registry (immutable ECR tags reject re-pushing the same tag).
if [[ "${SKIP_BUILD_PUSH:-0}" != 1 ]]; then
  docker build -f docker/Dockerfile.train -t "platform-train:$TAG" .
  scripts/run/push_images.sh "platform-train:$TAG"
fi
K="kubectl --context ${KCTX:-kind-platform} -n kubeflow"
export PIPELINE_TARGET="${PIPELINE_TARGET:-kind}"
if [[ "$PIPELINE_TARGET" == aws ]]; then
  # EKS: S3 via pipeline-runner's Pod Identity, MLflow on EC2; no MinIO Secret is created.
  : "${LAKEHOUSE_BUCKET:?LAKEHOUSE_BUCKET is required}" "${MLFLOW_TRACKING_URI:?MLFLOW_TRACKING_URI is required, e.g. http://10.42.0.12:5050}"
  [[ "$REGISTRY" == *.amazonaws.com/* ]] || { echo 'aws: REGISTRY must be ECR' >&2; exit 1; }
else
  $K create secret generic platform-minio --from-literal=AWS_ACCESS_KEY_ID="$MINIO_ROOT_USER" \
    --from-literal=AWS_SECRET_ACCESS_KEY="$MINIO_ROOT_PASSWORD" --dry-run=client -o yaml | $K apply -f - >/dev/null
fi
mkdir -p out/kfp
$K port-forward svc/ml-pipeline-ui 8888:80 >/dev/null 2>&1 &
trap 'kill $!' EXIT
# Wait for the port-forward to actually open (EKS over the Internet is slower than kind), up to 30 seconds.
for _ in $(seq 30); do
  curl -sf -o /dev/null http://localhost:8888/apis/v2beta1/healthz && break
  sleep 1
done
uv run --no-project --with "kfp==2.17.0" --with "kfp-kubernetes==2.17.0" python - "$@" <<'PY'
import sys
import kfp
from kfp import compiler
sys.path.insert(0, ".")
from ml.training_pipeline import return_risk_training

package = "out/kfp/return_risk_training.yaml"
compiler.Compiler().compile(return_risk_training, package)
client = kfp.Client(host="http://localhost:8888")
run = client.create_run_from_pipeline_package(package, arguments={}, run_name=sys.argv[1] if len(sys.argv) > 1 else None,
                                              experiment_name="return-risk")
print("run_id", run.run_id)
result = client.wait_for_run_completion(run.run_id, timeout=1800)
print("state", result.state)
PY
