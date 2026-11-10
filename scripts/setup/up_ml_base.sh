#!/usr/bin/env bash
# Bring back the shared ML Platform / LLM Platform base after a machine restart or an accidental `docker compose stop`:
# MinIO + PostgreSQL + Lakekeeper, Vault (unseal), Jenkins, then reload Redis if empty (Redis
# in kind does not persist to disk). Safe to rerun many times; does not start LLM/agents (RAM-hungry).
set -euo pipefail
cd "$(dirname "$0")/../.."
docker compose --profile catalog up -d --wait minio postgres lakekeeper >/dev/null
scripts/setup/setup_kind_registry.sh
scripts/setup/setup_vault.sh | tail -1
scripts/setup/setup_jenkins.sh | tail -1
keys=$(kubectl --context ${KCTX:-kind-platform} -n feature-store exec deploy/redis -- redis-cli dbsize)
if [[ "$keys" == 0 ]]; then
  cutoff=${FEAST_ONLINE_END:-2026-10-13T00:00:00}  # online snapshot point = Gold Cutoff
  docker compose --profile ml-stream run --rm --no-deps feast-pusher \
    feast materialize 2025-12-01T00:00:00 "$cutoff" --views sku_return_daily >/dev/null 2>&1
  keys=$(kubectl --context ${KCTX:-kind-platform} -n feature-store exec deploy/redis -- redis-cli dbsize)
fi
printf 'ML Platform base ready: Redis %s keys, Vault unsealed, Jenkins :8090\n' "$keys"
