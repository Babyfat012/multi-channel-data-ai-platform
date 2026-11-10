#!/usr/bin/env bash
# Install agentregistry into kind platform (ns agentregistry), DB = database `agentregistry` on pgvector (ns rag).
set -euo pipefail
cd "$(dirname "$0")/../.."
KC="kubectl --context ${KCTX:-kind-platform}"
# DB account = pgvector's own account (Secret rag-pg), never .env/shell variables: any mismatch locks the app out of the DB.
pg() { $KC -n rag get secret rag-pg -o jsonpath="{.data.$1}" | base64 -d; }
POSTGRES_USER=$(pg POSTGRES_USER); POSTGRES_PASSWORD=$(pg POSTGRES_PASSWORD)
$KC -n rag exec deploy/pgvector -- sh -c \
  "psql -U \"\$POSTGRES_USER\" -d rag -tc \"SELECT 1 FROM pg_database WHERE datname='agentregistry'\" | grep -q 1 \
   || psql -U \"\$POSTGRES_USER\" -d rag -c 'CREATE DATABASE agentregistry'"
$KC create namespace agentregistry --dry-run=client -o yaml | $KC apply -f -
$KC -n agentregistry create secret generic agentregistry-db --dry-run=client -o yaml \
  --from-literal=AGENT_REGISTRY_DATABASE_URL="postgres://$POSTGRES_USER:$POSTGRES_PASSWORD@pgvector.rag.svc.cluster.local:5432/agentregistry?sslmode=disable" \
  | $KC apply -f -
helm --kube-context "${KCTX:-kind-platform}" -n agentregistry upgrade --install agentregistry \
  oci://ghcr.io/agentregistry-dev/agentregistry/charts/agentregistry --version 0.4.0 \
  -f infra/k8s/agents/agentregistry-values.yaml --wait --timeout 5m
$KC -n agentregistry get pods,svc
