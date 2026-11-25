#!/usr/bin/env bash
# llm-d POC benchmark (step 6, local kind): same shared-prefix workload twice, through the plain Service (kube-proxy
# spreads requests) and through agentgateway + EPP (prefix-aware). vLLM replicas restart before each run so both start
# with an empty cache. Output: out/llmd-poc/<label>/summary.json + prefix-cache counters per replica.
#   kubectl --context kind-fsds-ml apply -f infra/k8s/agents/llm-d-poc.yaml && scripts/llm/llmd_poc_bench.sh
set -euo pipefail
cd "$(dirname "$0")/../.."
K=(kubectl --context "${KCTX:-kind-fsds-ml}" -n llm-d-poc)
IMAGE=quay.io/inference-perf/inference-perf:v0.7.0

counters() {  # "<pod> <prefix queries> <prefix hits>" per vLLM replica
  for pod in $("${K[@]}" get pod -l app=vllm-a -o jsonpath='{.items[*].metadata.name}'); do
    "${K[@]}" exec "$pod" -- python3 -c "
import urllib.request
m = urllib.request.urlopen('http://localhost:8000/metrics').read().decode()
v = lambda n: sum(float(l.split()[-1]) for l in m.splitlines() if l.startswith(n + '{'))
print('$pod', int(v('vllm:prefix_cache_queries_total')), int(v('vllm:prefix_cache_hits_total')), int(v('vllm:request_success_total')))"
  done
}

run() {  # run <label> <base url>
  local label=$1 url=$2 dir=out/llmd-poc/$1
  mkdir -p "$dir"
  "${K[@]}" rollout restart deploy/vllm-a >/dev/null
  "${K[@]}" rollout status deploy/vllm-a --timeout=15m >/dev/null
  sed "s|@@BASE_URL@@|$url|" infra/k8s/benchmark/llmd-poc-prefix.yaml >"$dir/workload.yaml"
  "${K[@]}" create configmap "bench-$label" --from-file=config.yml="$dir/workload.yaml" --dry-run=client -o yaml | "${K[@]}" apply -f - >/dev/null
  "${K[@]}" delete job "bench-$label" --ignore-not-found >/dev/null
  "${K[@]}" apply -f - >/dev/null <<YAML
apiVersion: batch/v1
kind: Job
metadata: {name: bench-$label}
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: inference-perf
        image: $IMAGE
        command: [sh, -c, "inference-perf --config_file /cfg/config.yml && echo '===SUMMARY===' && cat \$(find /out -name summary_lifecycle_metrics.json | head -1)"]
        volumeMounts: [{name: cfg, mountPath: /cfg}]
        resources: {requests: {cpu: 500m, memory: 512Mi}, limits: {memory: 1536Mi}}
      volumes: [{name: cfg, configMap: {name: bench-$label}}]
YAML
  "${K[@]}" wait --for=condition=complete "job/bench-$label" --timeout=20m >/dev/null
  "${K[@]}" logs "job/bench-$label" | sed -n '/===SUMMARY===/,$p' | tail -n +2 >"$dir/summary.json"
  counters >"$dir/prefix_cache.txt"
  echo "== $label"; cat "$dir/prefix_cache.txt"
}

run round-robin http://vllm-a.llm-d-poc.svc.cluster.local:8000
run epp http://agentgateway.llm-d-poc.svc.cluster.local:3000
python3 - <<'PY'
import json, pathlib
for label in ("round-robin", "epp"):
    d = pathlib.Path("out/llmd-poc") / label
    s = json.loads((d / "summary.json").read_text())
    ok, ttft = s["successes"], s["successes"]["latency"]["time_to_first_token"]
    rows = [l.split() for l in (d / "prefix_cache.txt").read_text().splitlines()]
    q, h = sum(int(r[1]) for r in rows), sum(int(r[2]) for r in rows)
    per_pod = " / ".join(r[3] for r in rows)
    print(f"| {label} | {ok['count']} / {s['failures']['count']} | {ttft['median']:.2f} / {ttft['p90']:.2f} "
          f"| {h / q:.0%} ({h:,}/{q:,} tokens) | {per_pod} |")
PY
