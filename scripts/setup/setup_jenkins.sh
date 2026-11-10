#!/usr/bin/env bash
# Start Jenkins (profile ci): the admin password is generated once and stored in Vault (secret/platform/jenkins),
# a read-only Vault token for the `vault-ci-token` credential, a kubeconfig (local kind / EKS on EC2) for `kind-kubeconfig`.
# Requires Vault to be set up (scripts/setup/setup_vault.sh). UI: http://localhost:8090 (user admin).
set -euo pipefail
cd "$(dirname "$0")/../.."
# EC2: .env (Ansible) carries PLATFORM_STORAGE=s3 + AWS_REGION ⇒ selects the EKS kubeconfig branch below.
if [[ -f .env ]]; then set -a; . ./.env; set +a; fi
export VAULT_ADDR="http://localhost:${VAULT_PORT:-8200}"
ROOT=$(python3 -c 'import json;print(json.load(open("out/vault/init.json"))["root_token"])')
api() { curl -fsS -H "X-Vault-Token: $ROOT" "$@"; }
if ! api "$VAULT_ADDR/v1/secret/data/platform/jenkins" >/dev/null 2>&1; then
  pw=$(python3 -c 'import secrets;print(secrets.token_urlsafe(18))')
  api -X POST "$VAULT_ADDR/v1/secret/data/platform/jenkins" -d "{\"data\":{\"admin_password\":\"$pw\"}}" >/dev/null
fi
export JENKINS_ADMIN_PASSWORD=$(api "$VAULT_ADDR/v1/secret/data/platform/jenkins" | python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["data"]["admin_password"])')
export VAULT_CI_TOKEN=$(cat out/vault/ci-token)
export DOCKER_GID=$(getent group docker | cut -d: -f3) PWD
mkdir -p out/jenkins out/jenkins_home
# AWS (PLATFORM_STORAGE=s3, running on EC2): the EKS kubeconfig uses `aws eks get-token` with the EC2 IAM role (group
# platform-ci, infra/k8s/ci/rbac.yaml); local: kind. The Jenkins image has the aws CLI, so the token is created in the container.
if [[ "${PLATFORM_STORAGE:-minio}" == s3 ]]; then
  docker run --rm --network host -v "$PWD/out/jenkins:/out" amazon/aws-cli:2.31.0 eks update-kubeconfig \
    --region "$AWS_REGION" --name "${EKS_CLUSTER:-platform-apps}" --alias "${EKS_CLUSTER:-platform-apps}" \
    --kubeconfig /out/kubeconfig >/dev/null
  sudo chown "$(id -u):$(id -g)" out/jenkins/kubeconfig
else
  kind get kubeconfig --name platform > out/jenkins/kubeconfig
fi
chmod 600 out/jenkins/kubeconfig
docker compose --profile ci up -d --build jenkins
for _ in $(seq 1 90); do curl -s -o /dev/null -w '%{http_code}' localhost:8090/login | grep -q 200 && break; sleep 2; done
printf 'Jenkins: http://localhost:8090  user admin, password in Vault secret/platform/jenkins\n'
