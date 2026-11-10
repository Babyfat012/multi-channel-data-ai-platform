#!/usr/bin/env bash
# Deploy the Agent stack in kind platform: LLM (llama.cpp), agentgateway, minimal kagent, Data Agent.
# Safe to rerun many times: the gateway API key is kept if the Secret already exists.
set -euo pipefail
cd "$(dirname "$0")/../.."
KC="kubectl --context ${KCTX:-kind-platform}"
KAGENT_VERSION=0.10.3

$KC apply -f infra/k8s/agents/llm.yaml
$KC apply -f infra/k8s/agents/agentgateway.yaml

# One API key for every Agent calling the gateway. Generated once, then read back from the Secret.
KEY=$($KC -n agentgateway get secret agentgateway-keys -o jsonpath='{.data.AGW_AGENT_KEY}' 2>/dev/null \
  | base64 -d || true)
[ -n "$KEY" ] || KEY="agw_$(openssl rand -hex 16)"
$KC -n agentgateway create secret generic agentgateway-keys --dry-run=client -o yaml \
  --from-literal=AGW_AGENT_KEY="$KEY" | $KC apply -f -
$KC -n agentgateway rollout restart deploy/agentgateway   # reload ConfigMap/Secret

helm --kube-context "${KCTX:-kind-platform}" upgrade --install kagent-crds \
  oci://ghcr.io/kagent-dev/kagent/helm/kagent-crds --version "$KAGENT_VERSION" \
  -n kagent --create-namespace --wait
# Same key on the kagent side: ModelConfig reads AGW_AGENT_KEY, RemoteMCPServer sends AUTH_HEADER.
$KC -n kagent create secret generic kagent-agw-key --dry-run=client -o yaml \
  --from-literal=AGW_AGENT_KEY="$KEY" --from-literal=AUTH_HEADER="Bearer $KEY" | $KC apply -f -
helm --kube-context "${KCTX:-kind-platform}" upgrade --install kagent \
  oci://ghcr.io/kagent-dev/kagent/helm/kagent --version "$KAGENT_VERSION" \
  -n kagent -f infra/k8s/agents/kagent-values.yaml --wait --timeout 10m

$KC apply -f infra/k8s/agents/data-agent.yaml
$KC -n kagent get agents,remotemcpservers,modelconfigs
