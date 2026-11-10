#!/usr/bin/env bash
# Measure KEDA autoscaling on a real cluster: a load pod INSIDE the cluster calls through the Service, recording target/pods over time
# until it scales up and then back down to the minimum.
# Run: KCTX=<kube-context> [TARGET=feature-api|feature-mcp|return-risk] scripts/evidence/feature_api_scale_test.sh [threads=4] [load seconds=150]
#   feature-api (default): POST /features, Helm release feature-api (values-aws.yaml enables autoscaling).
#   feature-mcp: POST /mcp/ tools/list (separate MCP pod, release feature-mcp).
#   return-risk: POST KServe :predict (the Model Agent's predict backend), ScaledObject return-risk-predictor.
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
  feature-mcp)
    ns=${FEATURE_API_NAMESPACE:-feature-store}; scaled=feature-mcp; selector=app=feature-mcp
    url=http://feature-mcp:8000/mcp/; body='{"jsonrpc":"2.0","id":1,"method":"tools/list"}' ;;
  return-risk)
    ns=ml-serving; scaled=return-risk-predictor; selector=serving.kserve.io/inferenceservice=return-risk
    url=http://return-risk-predictor/v1/models/return-risk:predict
    body='{"instances":[{"seller_sku":"APP-00042","channel":"amazon","country":"US","ordered_basis_qty":1,"ordered_product_net_minor":2500}]}' ;;
  *) echo "TARGET must be feature-api, feature-mcp or return-risk" >&2; exit 1 ;;
esac
hpa="keda-hpa-$scaled"
load="$target-load"
out=${OUT:-docs/evidence/aws/$target-autoscale.txt}
K=(kubectl --context "${KCTX:?set KCTX=<kube-context>}" -n "$ns")
trap '"${K[@]}" delete pod "$load" --ignore-not-found --wait=false >/dev/null 2>&1 || true' EXIT

hpa_get() { "${K[@]}" get hpa "$hpa" -o "jsonpath=$1"; }
min=$(hpa_get '{.spec.minReplicas}')
thr=$(hpa_get '{.spec.metrics[0].external.target.averageValue}')
running() { "${K[@]}" get pods -l "$selector" --no-headers | awk '$2=="1/1" && $3=="Running"{print $1}'; }

{
  echo "# KEDA autoscale $scaled ($KCTX, $(date -u +%Y-%m-%dT%H:%M:%SZ))"
  echo "# Load: $conc parallel curl threads inside the cluster, POST $url, ${load_s} s; threshold $thr/second per pod."
  "${K[@]}" get scaledobject "$scaled"
} | tee "$out"

"${K[@]}" delete pod "$load" --ignore-not-found >/dev/null
"${K[@]}" run "$load" --restart=Never --image=curlimages/curl:8.11.1 --command -- sh -c \
  "end=\$((\$(date +%s) + $load_s)); for i in \$(seq $conc); do
     (while [ \$(date +%s) -lt \$end ]; do curl -s -o /dev/null -m 3 -X POST -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
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
    ((peak <= min && t > load_s + 180)) && { echo "# Did not scale above $min: check Prometheus/KEDA." | tee -a "$out"; break; }
    ((t > load_s + 900)) && { echo "# Over 900 s after load and not back to $min pods." | tee -a "$out"; break; }
  fi
  sleep "$interval"
done
{
  "${K[@]}" get hpa "$hpa"
  "${K[@]}" describe hpa "$hpa" | sed -n '/^Events:/,$p'
} 2>&1 | tee -a "$out"
