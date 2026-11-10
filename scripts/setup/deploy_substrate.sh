#!/usr/bin/env bash
# Agent Substrate for kagent 0.10.3: Agents run in a gVisor sandbox (Substrate's node agent sets the gVisor runtime itself,
# without modifying the node's containerd) on a WorkerPool with few workers; idle actors snapshot to object storage, then go "Suspended"
# ⇒ many Agents share a few workers (Warm Up / Agent Fleet). Per kagent.dev/docs/kagent/examples/agent-substrate.
# Requires kagent installed (scripts/setup/deploy_kind_agents.sh). Safe to rerun many times.
set -euo pipefail
cd "$(dirname "$0")/../.."
CTX=${KCTX:-kind-platform}
SUBSTRATE_VERSION=0.0.9
KAGENT_VERSION=0.10.3   # matches deploy_kind_agents.sh; versions < 0.9.7 silently ignore the substrate values
H=(helm --kube-context "$CTX")
# SA token issuer taken from the cluster itself: kind = kubernetes.default.svc…, EKS = oidc.eks.<region>…; the chart default only fits kind.
ISSUER=$(kubectl --context "$CTX" get --raw /.well-known/openid-configuration | python3 -c 'import json,sys;print(json.load(sys.stdin)["issuer"])')
"${H[@]}" upgrade --install substrate-crds oci://ghcr.io/kagent-dev/substrate/helm/substrate-crds \
  --version "$SUBSTRATE_VERSION" -n ate-system --create-namespace --wait
"${H[@]}" upgrade --install substrate oci://ghcr.io/kagent-dev/substrate/helm/substrate \
  --version "$SUBSTRATE_VERSION" -n ate-system --set-string auth.jwt.issuer="$ISSUER" --wait --timeout 10m
# Keep existing values (model via agentgateway, A2A/MCP proxy) and only enable the Substrate wiring; 1 worker for the whole fleet.
"${H[@]}" upgrade kagent oci://ghcr.io/kagent-dev/kagent/helm/kagent --version "$KAGENT_VERSION" -n kagent \
  --reuse-values \
  --set controller.substrate.enabled=true \
  --set controller.substrate.ateApiEndpoint=dns:///api.ate-system.svc:443 \
  --set controller.substrate.ateApiInsecure=true \
  --set substrateWorkerPool.create=true \
  --set substrateWorkerPool.replicas="${SUBSTRATE_WORKERS:-1}" \
  --set substrateWorkerPool.ateomImage=ghcr.io/kagent-dev/substrate/ateom-gvisor:v"$SUBSTRATE_VERSION" \
  --wait --timeout 10m
kubectl --context "$CTX" apply -f infra/k8s/agents/sandbox/
# The first run creates the golden snapshot (~60–90 s).
kubectl --context "$CTX" -n kagent wait sandboxagent --all --for=condition=Ready --timeout=10m
kubectl --context "$CTX" -n kagent get workerpool,sandboxagent
kubectl --context "$CTX" -n ate-system get pods
