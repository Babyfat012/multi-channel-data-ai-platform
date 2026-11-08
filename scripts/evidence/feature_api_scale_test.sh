#!/usr/bin/env bash
# Đo KEDA autoscale trên cluster thật: pod tải TRONG cluster gọi qua Service, ghi target/pods theo thời gian
# tới khi scale lên rồi về lại tối thiểu.
# Chạy: KCTX=<kube-context> [TARGET=feature-api|return-risk] scripts/evidence/feature_api_scale_test.sh [luồng=4] [giây tải=150]
#   feature-api (mặc định): POST /features, release Helm feature-api (values-aws.yaml bật autoscaling).
#   return-risk: POST KServe :predict (backend predict của Model Agent), ScaledObject return-risk-predictor.
# Ghi docs/evidence/aws/<target>-autoscale.txt.
set -euo pipefail
cd "$(dirname "$0")/../.."
conc=${1:-4}
load_s=${2:-150}
interval=${INTERVAL:-15}
target=${TARGET:-feature-api}
case "$target" in
  feature-api)
    ns=${FEATURE_API_NAMESPACE:-feature-store}; scaled=feature-api; selector=app=feature-api
    url=http://feature-api:8000/features; body='{"seller_skus":["APP-00042","BEA-01708"]}' ;;
  return-risk)
    ns=ml-serving; scaled=return-risk-predictor; selector=serving.kserve.io/inferenceservice=return-risk
    url=http://return-risk-predictor/v1/models/return-risk:predict
    body='{"instances":[{"seller_sku":"APP-00042","channel":"amazon","country":"US","ordered_basis_qty":1,"ordered_product_net_minor":2500}]}' ;;
  *) echo "TARGET phải là feature-api hoặc return-risk" >&2; exit 1 ;;
esac
hpa="keda-hpa-$scaled"
load="$target-load"
out=${OUT:-docs/evidence/aws/$target-autoscale.txt}
K=(kubectl --context "${KCTX:?đặt KCTX=<kube-context>}" -n "$ns")
trap '"${K[@]}" delete pod "$load" --ignore-not-found --wait=false >/dev/null 2>&1 || true' EXIT

hpa_get() { "${K[@]}" get hpa "$hpa" -o "jsonpath=$1"; }
min=$(hpa_get '{.spec.minReplicas}')
thr=$(hpa_get '{.spec.metrics[0].external.target.averageValue}')
running() { "${K[@]}" get pods -l "$selector" --no-headers | awk '$2=="1/1" && $3=="Running"{print $1}'; }

{
  echo "# KEDA autoscale $scaled ($KCTX, $(date -u +%Y-%m-%dT%H:%M:%SZ))"
  echo "# Tải: $conc luồng curl song song trong cluster, POST $url, ${load_s} s; ngưỡng $thr/giây mỗi pod."
  "${K[@]}" get scaledobject "$scaled"
} | tee "$out"

"${K[@]}" delete pod "$load" --ignore-not-found >/dev/null
"${K[@]}" run "$load" --restart=Never --image=curlimages/curl:8.11.1 --command -- sh -c \
  "end=\$((\$(date +%s) + $load_s)); for i in \$(seq $conc); do
     (while [ \$(date +%s) -lt \$end ]; do curl -s -o /dev/null -m 3 -X POST -H 'Content-Type: application/json' \
        -d '$body' $url; done) & done; wait"
"${K[@]}" wait --for=condition=Ready "pod/$load" --timeout=120s >/dev/null

t0=$SECONDS
peak=0
while :; do
  t=$((SECONDS - t0))
  p=$(running | wc -l)
  cur=$(hpa_get '{.status.currentMetrics[0].external.current.averageValue}' || true)
  names=$(running | sed 's/.*-//' | paste -sd, -)
  echo "t=${t}s target=${cur:-0}/$thr pods=$p [$names]" | tee -a "$out"
  ((p > peak)) && peak=$p
  if ((t > load_s)); then
    ((peak > min && p <= min)) && break
    ((peak <= min && t > load_s + 180)) && { echo "# Không scale quá $min: kiểm Prometheus/KEDA." | tee -a "$out"; break; }
    ((t > load_s + 900)) && { echo "# Quá 900 s sau tải mà chưa về $min pod." | tee -a "$out"; break; }
  fi
  sleep "$interval"
done
{
  "${K[@]}" get hpa "$hpa"
  "${K[@]}" describe hpa "$hpa" | sed -n '/^Events:/,$p'
} 2>&1 | tee -a "$out"
