#!/usr/bin/env bash
# Benchmark LLM inference platform (llama.cpp trên EKS) bằng llm-d-benchmark, chế độ run-only (endpoint có sẵn).
# Chuẩn bị một lần (cần Python 3.14, kubectl; harness chạy thành pod trong namespace BENCH_NAMESPACE):
#   git clone https://github.com/llm-d/llm-d-benchmark && git -C llm-d-benchmark checkout dce3622d3af357ce528eebd5ae575c999cb8146b
#   uv venv --python 3.14 .venv-llmd && uv pip install --python .venv-llmd/bin/python -e llm-d-benchmark
# Chạy:   KCTX=<kube-context> LLMD_BENCH=.venv-llmd/bin/llmdbenchmark LLMD_SRC=llm-d-benchmark scripts/evidence/run_llmd_benchmark.sh
# Thử khô (không đụng cluster): thêm DRY_RUN=1. Kết quả: out/llmd-benchmark/ (chép vào docs/evidence/aws/ khi chụp).
set -euo pipefail
cd "$(dirname "$0")/../.."
bench=${LLMD_BENCH:-llmdbenchmark}
src=$(realpath "${LLMD_SRC:-llm-d-benchmark}")
ns=${BENCH_NAMESPACE:-llm-bench}
endpoint=${LLM_ENDPOINT:-http://qwen.llm.svc.cluster.local:8080}
model=${LLM_MODEL:-qwen2.5-1.5b-instruct}
kubeconfig=$(mktemp)
trap 'rm -f "$kubeconfig"' EXIT
# Kubeconfig riêng cho context đã chọn: không đổi current-context của máy.
kubectl config view --raw --minify --context "${KCTX:?đặt KCTX=<kube-context>}" >"$kubeconfig"
extra=()
[[ -z "${DRY_RUN:-}" ]] || extra+=(--dry-run)
"$bench" "${extra[@]}" --base-dir "$src" --spec "${LLMD_SPEC:-cpu}" --workspace out/llmd-benchmark run \
  --kubeconfig "$kubeconfig" --endpoint-url "$endpoint" --model "$model" --namespace "$ns" \
  --harness inference-perf --workload-file-path infra/k8s/benchmark/llmd-inference-perf-qwen-cpu.yaml \
  --wait-timeout 1800
