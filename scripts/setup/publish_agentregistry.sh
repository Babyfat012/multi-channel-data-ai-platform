#!/usr/bin/env bash
# Publish the MCP server + 3 Agents to agentregistry (governance): temporary port-forward, then `arctl apply`.
# Requires arctl v0.4.0 (matching the server). Rerunning = updating under the same names.
set -euo pipefail
cd "$(dirname "$0")/../.."
kubectl --context ${KCTX:-kind-platform} -n agentregistry port-forward svc/agentregistry 12121:12121 >/dev/null 2>&1 &
PF=$!; trap 'kill $PF' EXIT
for _ in $(seq 1 20); do curl -fsS localhost:12121/openapi.json >/dev/null 2>&1 && break; sleep 1; done
export ARCTL_API_BASE_URL=http://localhost:12121
arctl apply -f infra/k8s/agents/registry/feature-tools.yaml   # MCP server first; the Agents reference it
for f in data-agent model-agent coordinator; do arctl apply -f "infra/k8s/agents/registry/$f.yaml"; done
arctl get mcpservers
arctl get agents
