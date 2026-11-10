#!/usr/bin/env bash
# Submit the Flink stream feature job (flink_jobs/ml/sku_return_feature.sql), running in the background.
# PostgreSQL credentials: environment variables if set, otherwise read from Vault (VAULT_ADDR + VAULT_TOKEN,
# as CI does), and only as a last resort .env on a manual trigger. A running job with the same name is cancelled first ⇒ redeploys never create two parallel jobs.
set -euo pipefail
cd "$(dirname "$0")/../.."
if [[ -z "${POSTGRES_PASSWORD:-}" && -n "${VAULT_TOKEN:-}" ]]; then
  eval "$(scripts/setup/vault_env.sh postgres)"
elif [[ -z "${POSTGRES_PASSWORD:-}" && -f .env ]]; then
  set -a; . ./.env; set +a
fi
# Variant A (sku_return_feature_a.sql) does not query PostgreSQL ⇒ credentials are only required when the SQL has placeholders.
sql_file=${FEATURE_SQL:-flink_jobs/ml/sku_return_feature.sql}
if grep -q '@@PG_' "$sql_file"; then : "${POSTGRES_DB:?}" "${POSTGRES_USER:?}" "${POSTGRES_PASSWORD:?}"; fi
rest="http://localhost:${FLINK_UI_PORT:-8082}"
name=ml-sku-return-24h
for id in $(curl -fsS "$rest/jobs/overview" | python3 -c "import json,sys
for j in json.load(sys.stdin)['jobs']:
    if j['name'] == '$name' and j['state'] == 'RUNNING': print(j['jid'])"); do
  curl -fsS -X PATCH "$rest/jobs/$id?mode=cancel" >/dev/null && echo "cancelled old job $id"
done
sql=$(sed -e "s|@@PG_DB@@|${POSTGRES_DB:-}|" -e "s|@@PG_USER@@|${POSTGRES_USER:-}|" \
  -e "s|@@PG_PASSWORD@@|${POSTGRES_PASSWORD:-}|" "$sql_file")
docker compose --profile flink exec -T flink-jobmanager bash -c \
  'f=$(mktemp); cat > "$f"; ./bin/sql-client.sh -f "$f"; rm -f "$f"' <<<"$sql"
