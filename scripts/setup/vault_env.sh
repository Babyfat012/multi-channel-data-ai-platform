#!/usr/bin/env bash
# Print export commands for credentials read from Vault, so other scripts can use them instead of .env:
#   eval "$(scripts/setup/vault_env.sh postgres)"   → POSTGRES_USER/PASSWORD/DB
#   eval "$(scripts/setup/vault_env.sh minio)"      → MINIO_ROOT_USER/PASSWORD
#   eval "$(scripts/setup/vault_env.sh agentgateway)" → AGW_AGENT_KEY
# Requires VAULT_ADDR + VAULT_TOKEN (read-only token, policy platform-read).
set -euo pipefail
: "${VAULT_ADDR:?}" "${VAULT_TOKEN:?}" "${1:?postgres|minio|agentgateway|duckdns}"
curl -fsS -H "X-Vault-Token: $VAULT_TOKEN" "$VAULT_ADDR/v1/secret/data/platform/$1" | python3 -c '
import json, shlex, sys
kind, data = sys.argv[1], json.load(sys.stdin)["data"]["data"]
names = {"postgres": {"user": "POSTGRES_USER", "password": "POSTGRES_PASSWORD", "db": "POSTGRES_DB"},
         "minio": {"user": "MINIO_ROOT_USER", "password": "MINIO_ROOT_PASSWORD"},
         "agentgateway": {"AGW_AGENT_KEY": "AGW_AGENT_KEY"},
         "duckdns": {"token": "DUCKDNS_TOKEN"}}[kind]
for key, env in names.items():
    print(f"export {env}={shlex.quote(data[key])}")' "$1"
