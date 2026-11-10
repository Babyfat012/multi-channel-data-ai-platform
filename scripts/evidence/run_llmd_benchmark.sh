#!/usr/bin/env bash
# Benchmark the LLM inference platform (llama.cpp on EKS) with llm-d-benchmark, run-only mode (existing endpoint).
# One-time setup (needs Python 3.14, kubectl; the harness runs as a pod in namespace BENCH_NAMESPACE):
#   git clone https://github.com/llm-d/llm-d-benchmark && git -C llm-d-benchmark checkout dce3622d3af357ce528eebd5ae575c999cb8146b
#   uv venv --python 3.14 .venv-llmd && uv pip install --python .venv-llmd/bin/python -e llm-d-benchmark
# Run:    KCTX=<kube-context> LLMD_BENCH=.venv-llmd/bin/llmdbenchmark LLMD_SRC=llm-d-benchmark scripts/evidence/run_llmd_benchmark.sh
# Dry run (does not touch the cluster): add DRY_RUN=1. Output: out/llmd-benchmark/ (copy into docs/evidence/aws/ when capturing).
set -euo pipefail
cd "$(dirname "$0")/../.."
bench=${LLMD_BENCH:-llmdbenchmark}
src=$(realpath "${LLMD_SRC:-llm-d-benchmark}")
ns=${BENCH_NAMESPACE:-llm-bench}
endpoint=${LLM_ENDPOINT:-http://qwen.llm.svc.cluster.local:8080}
model=${LLM_MODEL:-qwen2.5-1.5b-instruct}
# By default the harness pod requests 16 CPU / 32 GiB (GPU cluster): the EKS nodes here have only 4 vCPU ⇒ pod stuck Pending "Insufficient cpu/memory" (04/11).
# Patch the llm-d-benchmark scenario (only inside LLMD_SRC, not this repo) down to what a 0.4 request/second load needs.
python3 - "$src/config/scenarios/examples/cpu.yaml" <<'PY'
import pathlib, sys
f = pathlib.Path(sys.argv[1]); t = f.read_text()
old = "    harness:\n      name: inference-perf\n      experimentProfile: sanity_random.yaml\n"
new = old + "      resources:\n        cpu: \"1\"\n        memory: 2Gi\n        memoryLimit: 4Gi\n"
if old in t and "memoryLimit: 4Gi" not in t:
    f.write_text(t.replace(old, new, 1)); print("patched harness pod resources: 1 CPU / 2 GiB")
PY
kubeconfig=$(mktemp)
trap 'rm -f "$kubeconfig"' EXIT
# Separate kubeconfig for the selected context: does not change the machine's current-context.
kubectl config view --raw --minify --context "${KCTX:?set KCTX=<kube-context>}" >"$kubeconfig"
extra=()
[[ -z "${DRY_RUN:-}" ]] || extra+=(--dry-run)
# llm-d-benchmark creates a ReadWriteMany PVC; EKS here only has EBS gp3 (RWO only) ⇒ "Volume capabilities not supported" (04/11).
# Runs with --no-pvc by default: results are copied straight from the harness pod. With a shared PVC (EFS), set BENCH_PVC=1.
pvc=(--no-pvc)
[[ -z "${BENCH_PVC:-}" ]] || pvc=()
# LABEL separates the output directory per configuration (baseline / optimized), see run_llmd_ab.sh.
"$bench" "${extra[@]}" --base-dir "$src" --spec "${LLMD_SPEC:-cpu}" --workspace "out/llmd-benchmark/${LABEL:-optimized}" run \
  --kubeconfig "$kubeconfig" --endpoint-url "$endpoint" --model "$model" --namespace "$ns" \
  --harness inference-perf --workload-file-path infra/k8s/benchmark/llmd-inference-perf-qwen-cpu.yaml \
  --wait-timeout 1800 "${pvc[@]}"
