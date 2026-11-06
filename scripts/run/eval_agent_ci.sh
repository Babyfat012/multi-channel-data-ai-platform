#!/usr/bin/env bash
# Cổng CI: hỏi A2A và chấm theo Feature API; không đổi/flush dữ liệu Redis.
set -euo pipefail
cd "$(dirname "$0")/../.."
mkdir -p out
EVAL_AGENT=${EVAL_AGENT:-data-agent}
EVAL_TASK=${EVAL_TASK:-features}  # features | predict | policy (hai tool của model-agent)
# Mỗi (agent, task) một file riêng: Jenkins chạy nhiều lần liên tiếp và archive out/agent-eval*.json*.
tag="$EVAL_AGENT-$EVAL_TASK"
rm -f "out/agent-eval-$tag.jsonl" "out/agent-eval-$tag-summary.json"
export AGW_AGENT_KEY
# Không bật xtrace: không in secret vào log Jenkins.
eval "$(scripts/setup/vault_env.sh agentgateway)"
feature_port=${EVAL_FEATURE_PORT:-18000}
gateway_port=${EVAL_GATEWAY_PORT:-18300}
K=(kubectl --context "${KCTX:-kind-platform}")
"${K[@]}" -n feature-store port-forward svc/feature-api "$feature_port:8000" >out/eval-feature-forward.log 2>&1 &
feature_pid=$!
"${K[@]}" -n agentgateway port-forward svc/agentgateway "$gateway_port:3000" >out/eval-gateway-forward.log 2>&1 &
gateway_pid=$!
trap 'kill "$feature_pid" "$gateway_pid" 2>/dev/null || true' EXIT
ready=0
for _ in $(seq 1 30); do
  kill -0 "$feature_pid" "$gateway_pid" 2>/dev/null || { echo "Port-forward eval đã dừng; xem out/eval-*-forward.log" >&2; exit 1; }
  if curl -fsS "http://localhost:$feature_port/healthz" >/dev/null && \
     curl -fsS -H "Authorization: Bearer $AGW_AGENT_KEY" \
       "http://localhost:$gateway_port/a2a/$EVAL_AGENT/.well-known/agent-card.json" >/dev/null; then
    ready=1
    break
  fi
  sleep 1
done
[[ "$ready" == 1 ]] || { echo "Port-forward eval chưa sẵn sàng" >&2; exit 1; }
# Danh sách SKU tường minh, không dùng dấu … trong lệnh CI. AWS (pipeline A): EVAL_SKUS=APP-…,…
python3 -m ml.agent_eval --agent "$EVAL_AGENT" --task "$EVAL_TASK" \
  --skus "${EVAL_SKUS:-sku-00001,sku-00002,sku-00003,sku-00004,sku-00005,sku-00006,sku-00007,sku-00008,sku-00009,sku-00010,sku-00011,sku-00012}" \
  --gateway "http://localhost:$gateway_port" --feature-api "http://localhost:$feature_port" \
  --min-accuracy 0.95 --timeout "${EVAL_TIMEOUT:-180}" --out "out/agent-eval-$tag.jsonl" | tee "out/agent-eval-$tag-summary.json"
