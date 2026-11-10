#!/usr/bin/env bash
# Deploy Langfuse v2 in kind (ns langfuse), DB = database `langfuse` on pgvector, then load the 3 Agent system prompts
# (only if Langfuse does not have them yet) and sync them to the ConfigMap. API keys are generated once and kept in a Secret.
set -euo pipefail
cd "$(dirname "$0")/../.."
KC="kubectl --context ${KCTX:-kind-platform}"
# DB account = pgvector's own account (Secret rag-pg), never .env/shell variables: any mismatch locks the app out of the DB.
pg() { $KC -n rag get secret rag-pg -o jsonpath="{.data.$1}" | base64 -d; }
POSTGRES_USER=$(pg POSTGRES_USER); POSTGRES_PASSWORD=$(pg POSTGRES_PASSWORD)
$KC -n rag exec deploy/pgvector -- sh -c \
  "psql -U \"\$POSTGRES_USER\" -d rag -tc \"SELECT 1 FROM pg_database WHERE datname='langfuse'\" | grep -q 1 \
   || psql -U \"\$POSTGRES_USER\" -d rag -c 'CREATE DATABASE langfuse'"
$KC apply -f infra/k8s/agents/langfuse.yaml
get() { $KC -n langfuse get secret langfuse-env -o jsonpath="{.data.$1}" 2>/dev/null | base64 -d || true; }
PK=$(get LANGFUSE_INIT_PROJECT_PUBLIC_KEY); SK=$(get LANGFUSE_INIT_PROJECT_SECRET_KEY)
NA=$(get NEXTAUTH_SECRET); SALT=$(get SALT); PW=$(get LANGFUSE_INIT_USER_PASSWORD)
[ -n "$PK" ] || PK="pk-lf-$(openssl rand -hex 12)"; [ -n "$SK" ] || SK="sk-lf-$(openssl rand -hex 16)"
[ -n "$NA" ] || NA=$(openssl rand -hex 32); [ -n "$SALT" ] || SALT=$(openssl rand -hex 16); [ -n "$PW" ] || PW=$(openssl rand -hex 12)
$KC -n langfuse create secret generic langfuse-env --dry-run=client -o yaml \
  --from-literal=DATABASE_URL="postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@pgvector.rag.svc.cluster.local:5432/langfuse" \
  --from-literal=NEXTAUTH_SECRET="$NA" --from-literal=SALT="$SALT" \
  --from-literal=LANGFUSE_INIT_PROJECT_PUBLIC_KEY="$PK" --from-literal=LANGFUSE_INIT_PROJECT_SECRET_KEY="$SK" \
  --from-literal=LANGFUSE_INIT_USER_EMAIL=admin@platform.local --from-literal=LANGFUSE_INIT_USER_PASSWORD="$PW" \
  | $KC apply -f -
# Keys for the sync script / Jenkins (ns kagent), same values.
$KC -n kagent create secret generic langfuse-api --dry-run=client -o yaml \
  --from-literal=LANGFUSE_PUBLIC_KEY="$PK" --from-literal=LANGFUSE_SECRET_KEY="$SK" | $KC apply -f -
$KC -n langfuse rollout restart deploy/langfuse >/dev/null
$KC -n langfuse rollout status deploy/langfuse --timeout=600s
python3 scripts/setup/sync_agent_prompts.py --seed
