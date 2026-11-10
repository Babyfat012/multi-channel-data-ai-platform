#!/usr/bin/env bash
# llm-d-benchmark baseline vs optimized on the same qwen.llm endpoint and workload, changing only the llama.cpp args.
#   baseline  = original configuration A: threads 4, parallel 2, ctx 8192, KV f16, no flash-attn
#   optimized = current infra/k8s/agents/llm.yaml: threads 3, parallel 4, ctx 16384, flash-attn, KV q8_0
# Run: KCTX=<kube-context> LLMD_BENCH=... LLMD_SRC=... scripts/evidence/run_llmd_ab.sh
# Output: out/llmd-benchmark/{baseline,optimized}/ and a comparison table printed at the end.
set -euo pipefail
cd "$(dirname "$0")/../.."
k=(kubectl --context "${KCTX:?set KCTX=<kube-context>}" -n llm)
restore() { "${k[@]}" apply -f infra/k8s/agents/llm.yaml >/dev/null && "${k[@]}" rollout status deploy/qwen --timeout=15m; }
trap restore EXIT  # a failure midway still restores the optimized configuration

"${k[@]}" patch deploy/qwen --type=json -p '[{"op":"replace","path":"/spec/template/spec/containers/0/args","value":[
  "-hf","Qwen/Qwen2.5-1.5B-Instruct-GGUF:Q4_K_M","--alias","qwen2.5-1.5b-instruct","--host","0.0.0.0","--port","8080",
  "--ctx-size","8192","--threads","4","--parallel","2","--flash-attn","off","--jinja","--metrics"]}]'
"${k[@]}" rollout status deploy/qwen --timeout=15m
LABEL=baseline scripts/evidence/run_llmd_benchmark.sh

restore
LABEL=optimized scripts/evidence/run_llmd_benchmark.sh
trap - EXIT

python3 scripts/evidence/compare_llmd_runs.py out/llmd-benchmark/baseline out/llmd-benchmark/optimized
