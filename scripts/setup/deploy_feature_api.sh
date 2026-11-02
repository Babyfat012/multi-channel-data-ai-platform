#!/usr/bin/env bash
# Deploy Feature API vào kubecontext đã chọn; S3 dùng Pod Identity, MinIO dùng credential local.
set -euo pipefail
set +x
cd "$(dirname "$0")/../.."
storage=${PLATFORM_STORAGE:-minio}
if [[ "$storage" == minio && -z "${MINIO_ROOT_USER:-}" && -f .env ]]; then
  set -a; . ./.env; set +a
fi
case "$storage" in
  minio) KCTX=${KCTX:-kind-platform}; REGISTRY=${REGISTRY:-localhost:5001} ;;
  s3)
    : "${KCTX:?Set the AWS kubecontext explicitly}"
    : "${REGISTRY:?Set the ECR namespace prefix}"
    : "${AWS_REGION:?Set AWS_REGION}"
    : "${LAKEHOUSE_BUCKET:?Set LAKEHOUSE_BUCKET}"
    [[ "$KCTX" != kind-* && "$REGISTRY" != localhost* ]] || {
      echo 'S3 cloud deployment requires a cloud context and registry.' >&2; exit 1;
    }
    ;;
  *) echo "Unsupported PLATFORM_STORAGE: $storage" >&2; exit 1 ;;
esac
export PLATFORM_STORAGE="$storage" KCTX REGISTRY
export TAG="${1:-${TAG:-$(git rev-parse HEAD)}}"
[[ "$TAG" != latest ]] || { echo 'Use an immutable TAG.' >&2; exit 1; }
: "${POSTGRES_USER:?Set POSTGRES_USER}"
: "${POSTGRES_PASSWORD:?Set POSTGRES_PASSWORD}"
namespace=${FEATURE_API_NAMESPACE:-feature-store}
K=(kubectl --context "$KCTX" -n "$namespace")
# Secrets travel through stdin, not process argv or traced shell output.
secret_env=$(python3 - <<'PY'
import os

env = os.environ
cloud = env["PLATFORM_STORAGE"] == "s3"
bucket = env["LAKEHOUSE_BUCKET"] if cloud else env.get("MINIO_BUCKET", "platform")
values = {
    "FEAST_REGISTRY": env.get("FEAST_REGISTRY", f"s3://{bucket}/feature-store/registry.db"),
    "FEAST_REDIS": env.get("FEAST_REDIS", "redis.feature-store.svc.cluster.local:6379"),
    "FEAST_PUBLICATION_ROOT": env.get("FEAST_PUBLICATION_ROOT", f"s3://{bucket}/feature-store/publications"),
    "FEAST_RAG_REGISTRY": env.get("FEAST_RAG_REGISTRY", f"s3://{bucket}/feature-store/rag-registry.db"),
    "RAG_PG_HOST": env.get("RAG_PG_HOST", "pgvector.rag.svc.cluster.local"),
    "RAG_PG_PORT": env.get("RAG_PG_PORT", "5432"),
    "RAG_PG_USER": env["POSTGRES_USER"],
    "RAG_PG_PASSWORD": env["POSTGRES_PASSWORD"],
    "KSERVE_URL": env.get("KSERVE_URL", "http://return-risk-predictor.ml-serving.svc.cluster.local/v1/models/return-risk:predict"),
}
if cloud:
    # RAG chỉ bật khi truyền FEAST_RAG_REGISTRY: thiếu file, Feast tự tạo registry mới (ghi S3) mà Pod
    # Identity chỉ đọc ⇒ app chết lúc khởi động. Không có biến thì /chunks trả 503, phần còn lại chạy.
    if not env.get("FEAST_RAG_REGISTRY"):
        del values["FEAST_RAG_REGISTRY"]
    values["AWS_REGION"] = env["AWS_REGION"]
    values["AWS_DEFAULT_REGION"] = env["AWS_REGION"]
else:
    values.update(AWS_ACCESS_KEY_ID=env["MINIO_ROOT_USER"], AWS_SECRET_ACCESS_KEY=env["MINIO_ROOT_PASSWORD"],
                  FEAST_S3_ENDPOINT_URL=env.get("FEAST_S3_ENDPOINT_URL", "http://minio:9000"))
for key, value in values.items():
    if "\n" in value or "\r" in value:
        raise SystemExit(f"Multiline value is not supported for {key}")
for key, value in values.items():
    print(f"{key}={value}")
PY
)
if [[ "${SKIP_BUILD_PUSH:-0}" != 1 ]]; then
  docker build -f docker/Dockerfile.serving -t "platform-feature-api:$TAG" .
  scripts/run/push_images.sh "platform-feature-api:$TAG"
fi
kubectl --context "$KCTX" create namespace "$namespace" --dry-run=client -o yaml | "${K[@]}" apply -f -
printf '%s\n' "$secret_env" | "${K[@]}" create secret generic feature-api-env \
  --from-env-file=/dev/stdin --dry-run=client -o yaml | "${K[@]}" apply -f -
# Dấu vân tay Secret gắn vào pod: env đổi ⇒ pod template đổi ⇒ Kubernetes thay pod (Secret đổi một mình thì không).
env_checksum=$(printf '%s\n' "$secret_env" | sha256sum | cut -c1-16)
unset secret_env
H=(helm --kube-context "$KCTX" -n "$namespace")
extra=()
[[ "$storage" != s3 ]] || extra=(-f infra/helm/feature-api/values-aws.yaml)
"${H[@]}" upgrade --install feature-api infra/helm/feature-api "${extra[@]}" \
  --set-string image.repository="${REGISTRY%/}/platform-feature-api" --set-string image.tag="$TAG" \
  --set replicas="${REPLICAS:-2}" --set-string envChecksum="$env_checksum" --atomic --timeout 4m
"${H[@]}" history feature-api --max 5
printf 'Feature API %s deployed to %s/%s; verify /readyz through the configured ingress or port-forward.\n' "$TAG" "$KCTX" "$namespace"
