#!/usr/bin/env bash
# Measure KEDA scaling of one Agent on a real cluster: generate A2A load into agentgateway, record target/pods over time.
# Run: KCTX=<kube-context> scripts/evidence/keda_scale_test.sh <coordinator|model-agent|data-agent> [rps=3] [load seconds=150]
# Needs VAULT_ADDR + VAULT_TOKEN (fetches the gateway API key like eval_agent_ci.sh) or a preset AGW_AGENT_KEY.
# Writes docs/evidence/aws/keda-<agent>.txt, same format as keda-data-agent-20261030.txt.
# No xtrace: do not print secrets to the log.
set -euo pipefail
cd "$(dirname "$0")/../.."
agent=${1:?usage: keda_scale_test.sh <coordinator|model-agent|data-agent> [rps] [load seconds]}
rps=${2:-3}
load_s=${3:-150}
interval=${INTERVAL:-20}
ns=kagent
hpa="keda-hpa-$agent"
port=${EVAL_GATEWAY_PORT:-18300}
out=${OUT:-docs/evidence/aws/keda-$agent.txt}
K=(kubectl --context "${KCTX:?set KCTX=<kube-context>}")
[[ -n "${AGW_AGENT_KEY:-}" ]] || eval "$(scripts/setup/vault_env.sh agentgateway)"
export AGW_AGENT_KEY

"${K[@]}" -n agentgateway port-forward svc/agentgateway "$port:3000" >/dev/null 2>&1 &
pf=$!
trap 'kill "$pf" "${load_pid:-}" 2>/dev/null || true' EXIT
# Route by the x-kagent-host header: the ScaledObjects of all three Agents count this route.
card() { curl -fsS -o /dev/null -H "Authorization: Bearer $AGW_AGENT_KEY" -H "x-kagent-host: $agent.kagent" \
  "http://localhost:$port/.well-known/agent-card.json"; }
for _ in $(seq 1 30); do card 2>/dev/null && break; sleep 1; done
card || { echo "Gateway has not returned the agent card for $agent" >&2; exit 1; }

hpa_get() { "${K[@]}" -n "$ns" get hpa "$hpa" -o "jsonpath=$1"; }
min=$(hpa_get '{.spec.minReplicas}')
thr=$(hpa_get '{.spec.metrics[0].external.target.averageValue}')
running() { "${K[@]}" -n "$ns" get pods -l "kagent=$agent" --no-headers | awk '$3=="Running"{print $1}'; }
pods() { running | wc -l; }

{
  echo "# KEDA autoscale $agent ($KCTX, $(date -u +%Y-%m-%dT%H:%M:%SZ))"
  echo "# Load: $rps requests/second into route a2a-$agent (x-kagent-host header) fetching the agent card, ${load_s} s; threshold $thr/pod; pods = actually Running."
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
  # Suffix of each pod name ⇒ shows which pods are new / which were removed while scaling.
  names=$(running | sed 's/.*-//' | paste -sd, -)
  echo "t=${t}s target=${cur:-0}/$thr pods=$p [$names]" | tee -a "$out"
  ((p > peak)) && peak=$p
  if ! kill -0 "$load_pid" 2>/dev/null; then
    ((peak > min && p <= min)) && break
    ((peak <= min && t > load_s + 180)) && { echo "# Did not scale above the minimum $min: check Prometheus/KEDA." | tee -a "$out"; break; }
    ((t > load_s + 900)) && { echo "# Over 900 s after load and not back to $min pods." | tee -a "$out"; break; }
  fi
  sleep "$interval"
done
"${K[@]}" -n "$ns" describe hpa "$hpa" | sed -n '/^Events:/,$p' | tee -a "$out"
