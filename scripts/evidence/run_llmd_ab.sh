#!/usr/bin/env bash
# llm-d-benchmark baseline vs optimized trên cùng endpoint qwen.llm, cùng workload, chỉ đổi args llama.cpp.
#   baseline  = cấu hình A ban đầu: threads 4, parallel 2, ctx 8192, KV f16, không flash-attn
#   optimized = infra/k8s/agents/llm.yaml hiện hành: threads 3, parallel 4, ctx 16384, flash-attn, KV q8_0
# Chạy: KCTX=<kube-context> LLMD_BENCH=... LLMD_SRC=... scripts/evidence/run_llmd_ab.sh
# Kết quả: out/llmd-benchmark/{baseline,optimized}/ và bảng so sánh in ra cuối.
set -euo pipefail
cd "$(dirname "$0")/../.."
k=(kubectl --context "${KCTX:?đặt KCTX=<kube-context>}" -n llm)
restore() { "${k[@]}" apply -f infra/k8s/agents/llm.yaml >/dev/null && "${k[@]}" rollout status deploy/qwen --timeout=15m; }
trap restore EXIT  # lỗi giữa chừng vẫn trả về cấu hình optimized

"${k[@]}" patch deploy/qwen --type=json -p '[{"op":"replace","path":"/spec/template/spec/containers/0/args","value":[
  "-hf","Qwen/Qwen2.5-1.5B-Instruct-GGUF:Q4_K_M","--alias","qwen2.5-1.5b-instruct","--host","0.0.0.0","--port","8080",
  "--ctx-size","8192","--threads","4","--parallel","2","--flash-attn","off","--jinja","--metrics"]}]'
"${k[@]}" rollout status deploy/qwen --timeout=15m
LABEL=baseline scripts/evidence/run_llmd_benchmark.sh

restore
LABEL=optimized scripts/evidence/run_llmd_benchmark.sh
trap - EXIT

python3 scripts/evidence/compare_llmd_runs.py out/llmd-benchmark/baseline out/llmd-benchmark/optimized
