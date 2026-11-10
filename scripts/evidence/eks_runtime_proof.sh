#!/usr/bin/env bash
# Print EKS runtime evidence once for screenshots: Web API and Agent pod probes/health, replicas and autoscaling of EACH pod, Sandbox/Substrate.
# Run: KCTX=<kube-context> scripts/evidence/eks_runtime_proof.sh   (writes docs/evidence/aws/eks-runtime-proof.txt)
# MCP tools/list + deny live in scripts/evidence/gateway_smoke.py. A failing command does not stop the script (missing sections are reported).
set -uo pipefail
cd "$(dirname "$0")/../.."
K=(kubectl --context "${KCTX:?set KCTX=<kube-context>}")
out=${OUT:-docs/evidence/aws/eks-runtime-proof.txt}
section() { printf '\n## %s\n' "$*"; }
{
  echo "# EKS runtime evidence ($KCTX, $(date -u +%Y-%m-%dT%H:%M:%SZ))"
  section "Feature API: Deployment, each pod, probe configuration"
  "${K[@]}" -n feature-store get deploy,pods -l app=feature-api -o wide
  "${K[@]}" -n feature-store describe deploy feature-api | grep -E "Replicas:|StrategyType|RollingUpdateStrategy|Liveness|Readiness|Startup"
  section "Feature API: /healthz (liveness) and /readyz (readiness) called inside each pod"
  for pod in $("${K[@]}" -n feature-store get pods -l app=feature-api -o name); do
    printf '%s: ' "$pod"
    "${K[@]}" -n feature-store exec "$pod" -- python -c \
      "import urllib.request as u;print({p: u.urlopen('http://localhost:8000'+p).status for p in ('/healthz','/readyz')})"
  done
  section "Agent: replica count and autoscaling (KEDA → HPA), which node each pod runs on"
  "${K[@]}" -n kagent get scaledobject,hpa
  "${K[@]}" -n kagent get pods -l kagent -o wide
  section "Agent: probes and replicas of each Deployment (Data Agent, Model Agent, Coordinator)"
  for agent in data-agent model-agent coordinator; do
    echo "--- $agent"
    "${K[@]}" -n kagent describe deploy "$agent" | grep -E "Replicas:|Liveness|Readiness|Startup" || echo "(Deployment declares no probes or does not exist)"
  done
  section "Sandbox / Agent Substrate (gVisor)"
  "${K[@]}" -n kagent get workerpool,sandboxagent
  # Substrate does not use a Kubernetes RuntimeClass: gVisor lives in the ateom image and the WorkerPool's sandboxClass.
  "${K[@]}" -n kagent get workerpool kagent-default -o yaml | grep -E "ateomImage|sandboxClass|replicas"
  "${K[@]}" -n ate-system get pods
} 2>&1 | tee "$out"
