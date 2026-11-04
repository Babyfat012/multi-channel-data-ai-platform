#!/usr/bin/env bash
# In một lần bằng chứng runtime trên EKS để chụp màn hình: probe/health Web API, replica và autoscale TỪNG pod, Sandbox/Substrate.
# Chạy: KCTX=<kube-context> scripts/evidence/eks_runtime_proof.sh   (ghi docs/evidence/aws/eks-runtime-proof.txt)
# MCP tools/list + deny nằm ở scripts/evidence/gateway_smoke.py. Lệnh lỗi không dừng script (mục nào thiếu thì ghi rõ).
set -uo pipefail
cd "$(dirname "$0")/../.."
K=(kubectl --context "${KCTX:?đặt KCTX=<kube-context>}")
out=${OUT:-docs/evidence/aws/eks-runtime-proof.txt}
section() { printf '\n## %s\n' "$*"; }
{
  echo "# Bằng chứng runtime EKS ($KCTX, $(date -u +%Y-%m-%dT%H:%M:%SZ))"
  section "Feature API: Deployment, từng pod, cấu hình probe"
  "${K[@]}" -n feature-store get deploy,pods -l app=feature-api -o wide
  "${K[@]}" -n feature-store describe deploy feature-api | grep -E "Replicas:|StrategyType|RollingUpdateStrategy|Liveness|Readiness|Startup"
  section "Feature API: /healthz (liveness) và /readyz (readiness) gọi trong từng pod"
  for pod in $("${K[@]}" -n feature-store get pods -l app=feature-api -o name); do
    printf '%s: ' "$pod"
    "${K[@]}" -n feature-store exec "$pod" -- python -c \
      "import urllib.request as u;print({p: u.urlopen('http://localhost:8000'+p).status for p in ('/healthz','/readyz')})"
  done
  section "Agent: số replica và autoscale (KEDA → HPA), từng pod chạy ở node nào"
  "${K[@]}" -n kagent get scaledobject,hpa
  "${K[@]}" -n kagent get pods -l kagent -o wide
  section "Sandbox / Agent Substrate (gVisor)"
  "${K[@]}" -n kagent get workerpool,sandboxagent
  # Substrate không dùng RuntimeClass của Kubernetes: gVisor nằm ở image ateom và sandboxClass của WorkerPool.
  "${K[@]}" -n kagent get workerpool kagent-default -o yaml | grep -E "ateomImage|sandboxClass|replicas"
  "${K[@]}" -n ate-system get pods
} 2>&1 | tee "$out"
