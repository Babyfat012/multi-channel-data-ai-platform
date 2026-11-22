#!/usr/bin/env bash
# Init (first time) + unseal Vault, enable KV v2, load PLATFORM secrets from .env, create a read-only policy and token
# for CI. Unseal keys + root token are stored in out/vault/ (gitignored, mode 600) — local use only;
# on GKE, replace with Cloud KMS auto-unseal. Safe to rerun many times.
set -euo pipefail
cd "$(dirname "$0")/../.."
set -a; . ./.env; set +a
export VAULT_ADDR="http://localhost:${VAULT_PORT:-8200}"
docker compose --profile security up -d vault >/dev/null
for _ in $(seq 1 30); do curl -s -o /dev/null "$VAULT_ADDR/v1/sys/health" && break; sleep 1; done
mkdir -p out/vault && chmod 700 out/vault
api() { curl -fsS -H "X-Vault-Token: ${ROOT:-}" "$@"; }
if [[ "$(curl -s "$VAULT_ADDR/v1/sys/init" | python3 -c 'import json,sys;print(json.load(sys.stdin)["initialized"])')" == False ]]; then
  api -X PUT "$VAULT_ADDR/v1/sys/init" -d '{"secret_shares":1,"secret_threshold":1}' > out/vault/init.json
  chmod 600 out/vault/init.json
fi
KEY=$(python3 -c 'import json;print(json.load(open("out/vault/init.json"))["keys"][0])')
ROOT=$(python3 -c 'import json;print(json.load(open("out/vault/init.json"))["root_token"])')
api -X PUT "$VAULT_ADDR/v1/sys/unseal" -d "{\"key\":\"$KEY\"}" >/dev/null
api "$VAULT_ADDR/v1/sys/mounts" | grep -q '"secret/"' || \
  api -X POST "$VAULT_ADDR/v1/sys/mounts/secret" -d '{"type":"kv","options":{"version":"2"}}'
put() { api -X POST "$VAULT_ADDR/v1/secret/data/platform/$1" -d "{\"data\":$2}" >/dev/null; }
put minio "{\"user\":\"$MINIO_ROOT_USER\",\"password\":\"$MINIO_ROOT_PASSWORD\"}"
put postgres "{\"user\":\"$POSTGRES_USER\",\"password\":\"$POSTGRES_PASSWORD\",\"db\":\"$POSTGRES_DB\"}"
# agentgateway's agent key: sourced from the k8s Secret created by the helm chart; skipped if the cluster does not have it yet.
# EC2 (s3) has no kubectl/kubeconfig on the host ⇒ read via the Jenkins container (EKS kubeconfig, platform-ci group permissions);
# rerun this script AFTER setup_jenkins.sh.
if [[ "${PLATFORM_STORAGE:-minio}" == s3 ]]; then
  KC=(docker compose exec -T jenkins kubectl --kubeconfig /run/secrets/kubeconfig --context "${EKS_CLUSTER:-platform-apps}")
else
  KC=(kubectl --context "${KCTX:-kind-platform}")
fi
if agw=$("${KC[@]}" -n agentgateway get secret agentgateway-keys \
    -o jsonpath='{.data.AGW_AGENT_KEY}' 2>/dev/null | base64 -d) && [[ -n "$agw" ]]; then
  put agentgateway "{\"AGW_AGENT_KEY\":\"$agw\"}"
fi
api -X PUT "$VAULT_ADDR/v1/sys/policies/acl/platform-read" \
  -d '{"policy":"path \"secret/data/platform/*\" { capabilities = [\"read\"] }"}'
if [[ ! -s out/vault/ci-token ]]; then
  api -X POST "$VAULT_ADDR/v1/auth/token/create" -d '{"policies":["platform-read"],"period":"768h","display_name":"platform-ci"}' \
    | python3 -c 'import json,sys;print(json.load(sys.stdin)["auth"]["client_token"])' > out/vault/ci-token
  chmod 600 out/vault/ci-token
fi
printf 'Vault ready: %s, secrets in secret/platform/{minio,postgres}; read-only token: out/vault/ci-token\n' "$VAULT_ADDR"
