#!/usr/bin/env bash
# Đo KEDA scale của một Agent trên cluster thật: tạo tải A2A vào agentgateway, ghi target/pods theo thời gian.
# Chạy: KCTX=<kube-context> scripts/evidence/keda_scale_test.sh <coordinator|model-agent|data-agent> [rps=3] [giây tải=150]
# Cần VAULT_ADDR + VAULT_TOKEN (lấy API key gateway như eval_agent_ci.sh) hoặc đặt sẵn AGW_AGENT_KEY.
# Ghi docs/evidence/aws/keda-<agent>.txt, cùng định dạng keda-data-agent-20261030.txt.
# Không bật xtrace: không in secret vào log.
set -euo pipefail
cd "$(dirname "$0")/../.."
agent=${1:?usage: keda_scale_test.sh <coordinator|model-agent|data-agent> [rps] [giây tải]}
rps=${2:-3}
load_s=${3:-150}
interval=${INTERVAL:-20}
ns=kagent
hpa="keda-hpa-$agent"
port=${EVAL_GATEWAY_PORT:-18300}
out=${OUT:-docs/evidence/aws/keda-$agent.txt}
K=(kubectl --context "${KCTX:?đặt KCTX=<kube-context>}")
[[ -n "${AGW_AGENT_KEY:-}" ]] || eval "$(scripts/setup/vault_env.sh agentgateway)"
export AGW_AGENT_KEY

"${K[@]}" -n agentgateway port-forward svc/agentgateway "$port:3000" >/dev/null 2>&1 &
pf=$!
trap 'kill "$pf" "${load_pid:-}" 2>/dev/null || true' EXIT
# Route theo header x-kagent-host: ScaledObject của cả ba Agent đều đếm route này.
card() { curl -fsS -o /dev/null -H "Authorization: Bearer $AGW_AGENT_KEY" -H "x-kagent-host: $agent.kagent" \
  "http://localhost:$port/.well-known/agent-card.json"; }
for _ in $(seq 1 30); do card 2>/dev/null && break; sleep 1; done
card || { echo "Gateway chưa trả agent card của $agent" >&2; exit 1; }

hpa_get() { "${K[@]}" -n "$ns" get hpa "$hpa" -o "jsonpath=$1"; }
min=$(hpa_get '{.spec.minReplicas}')
thr=$(hpa_get '{.spec.metrics[0].external.target.averageValue}')
running() { "${K[@]}" -n "$ns" get pods -l "kagent=$agent" --no-headers | awk '$3=="Running"{print $1}'; }
pods() { running | wc -l; }

{
  echo "# KEDA autoscale $agent ($KCTX, $(date -u +%Y-%m-%dT%H:%M:%SZ))"
  echo "# Tải: $rps request/giây vào route a2a-$agent (header x-kagent-host) lấy agent card, ${load_s} s; ngưỡng $thr/pod; pods = Running thật."
} | tee "$out"

load() {
  local end=$((SECONDS + load_s))
  while ((SECONDS < end)); do
    for _ in $(seq "$rps"); do card 2>/dev/null & done
    sleep 1
  done
  wait
}
load &
load_pid=$!
t0=$SECONDS
peak=0
while :; do
  t=$((SECONDS - t0))
  p=$(pods)
  cur=$(hpa_get '{.status.currentMetrics[0].external.current.averageValue}' || true)
  # Tên đuôi từng pod ⇒ thấy pod nào mới sinh / pod nào bị bỏ khi scale.
  names=$(running | sed 's/.*-//' | paste -sd, -)
  echo "t=${t}s target=${cur:-0}/$thr pods=$p [$names]" | tee -a "$out"
  ((p > peak)) && peak=$p
  if ! kill -0 "$load_pid" 2>/dev/null; then
    ((peak > min && p <= min)) && break
    ((peak <= min && t > load_s + 180)) && { echo "# Không scale lên quá mức tối thiểu $min: kiểm Prometheus/KEDA." | tee -a "$out"; break; }
    ((t > load_s + 900)) && { echo "# Quá 900 s sau tải mà chưa về $min pod." | tee -a "$out"; break; }
  fi
  sleep "$interval"
done
"${K[@]}" -n "$ns" describe hpa "$hpa" | sed -n '/^Events:/,$p' | tee -a "$out"
