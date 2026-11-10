#!/usr/bin/env bash
# Set up routes via NGINX Ingress in kind: internal CA, HTTPS Ingress for the Feature API and agent UI
# (basic auth + rate limit). The agent UI password is generated once and stored in Vault secret/platform/agent-ui.
# Requires: ingress controller (helm nginx-ingress), cert-manager, Vault already set up.
set -euo pipefail
cd "$(dirname "$0")/../.."
K="kubectl --context ${KCTX:-kind-platform}"
if [[ -n "${INGRESS_DOMAIN:-}" ]]; then
  [[ "$INGRESS_DOMAIN" =~ ^[a-zA-Z0-9.-]+$ ]] || { echo 'Invalid INGRESS_DOMAIN' >&2; exit 1; }
  host_suffix=$INGRESS_DOMAIN
  https_port=${INGRESS_HTTPS_PORT:-443}
elif [[ "${KCTX:-kind-platform}" == kind-* ]]; then
  node_ip=$(docker inspect platform-control-plane --format '{{(index .NetworkSettings.Networks "kind").IPAddress}}')
  host_suffix="${node_ip//./-}.sslip.io"
  https_port=30443
else
  echo 'Set INGRESS_DOMAIN and point DNS at the cloud ingress before creating routes.' >&2
  exit 1
fi
$K apply -f infra/k8s/ingress/issuer.yaml >/dev/null
$K -n cert-manager wait --for=condition=Ready certificate/platform-local-ca --timeout=120s >/dev/null
# Cloud: INGRESS_ISSUER=platform-letsencrypt (real Let's Encrypt cert, HTTP-01); default is the internal CA.
issuer=${INGRESS_ISSUER:-platform-local-ca}
if [[ "$issuer" == platform-letsencrypt ]]; then
  : "${LE_EMAIL:?Set LE_EMAIL for the ACME account}"
  [[ "$LE_EMAIL" =~ ^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+$ ]] || { echo 'Invalid LE_EMAIL' >&2; exit 1; }
  sed "s/@@LE_EMAIL@@/$LE_EMAIL/" infra/k8s/ingress/issuer-letsencrypt-http01.yaml | $K apply -f - >/dev/null
elif [[ "$issuer" != platform-local-ca ]]; then
  echo "INGRESS_ISSUER only accepts platform-local-ca or platform-letsencrypt" >&2; exit 1
fi
# First Let's Encrypt run (Certificate not Ready; the secret may exist but be empty): the HTTPS redirect (no cert yet) and basic auth block HTTP-01
# ⇒ apply the open version, wait for the cert to be Ready, then apply the full version. Limitation: the day-60 renewal is still blocked by basic auth ⇒
# rerun the script (gap #24); prod uses a mergeable ingress or DNS-01.
apply_ing() {  # $1 namespace, $2 TLS secret; stdin = rendered Ingress
  local y; y=$(cat)
  if [[ "$issuer" == platform-letsencrypt ]] && [[ "$($K -n "$1" get certificate "$2" \
      -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)" != True ]]; then
    sed -e '/nginx.org\/basic-auth/d' \
      -e 's|^\(    acme.cert-manager.io/http01-edit-in-place: .*\)$|\1\n    ingress.kubernetes.io/ssl-redirect: "false"|' \
      <<<"$y" | $K apply -f - >/dev/null
    for _ in $(seq 1 30); do $K -n "$1" get certificate "$2" >/dev/null 2>&1 && break; sleep 2; done
    $K -n "$1" wait --for=condition=Ready "certificate/$2" --timeout=300s >/dev/null || {
      echo "cert $1/$2 not Ready: kubectl --context ${KCTX:-kind-platform} -n $1 describe challenges" >&2; exit 1; }
  fi
  $K apply -f - <<<"$y" >/dev/null
}
export VAULT_ADDR="${VAULT_ADDR:-http://localhost:${VAULT_PORT:-8200}}"
# Vault on EC2: pass VAULT_TOKEN via the environment (tunnel 8200); local reads init.json as before.
ROOT=${VAULT_TOKEN:-$(python3 -c 'import json;print(json.load(open("out/vault/init.json"))["root_token"])')}
api() { curl -fsS -H "X-Vault-Token: $ROOT" "$@"; }
if ! api "$VAULT_ADDR/v1/secret/data/platform/agent-ui" >/dev/null 2>&1; then
  pw=$(python3 -c 'import secrets;print(secrets.token_urlsafe(15))')
  api -X POST "$VAULT_ADDR/v1/secret/data/platform/agent-ui" -d "{\"data\":{\"user\":\"seller\",\"password\":\"$pw\"}}" >/dev/null
fi
pw=$(api "$VAULT_ADDR/v1/secret/data/platform/agent-ui" | python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["data"]["password"])')
$K -n kagent create secret generic agent-ui-basic-auth --type=nginx.org/htpasswd \
  --from-literal=htpasswd="seller:$(openssl passwd -apr1 "$pw")" --dry-run=client -o yaml | $K apply -f - >/dev/null
for f in feature-store/feature-api kagent/agent-ui; do
  sed -e "s/@@NODE_IP_DASH@@.sslip.io/$host_suffix/g" -e "s/cluster-issuer: platform-local-ca/cluster-issuer: $issuer/" \
    "infra/k8s/ingress/${f#*/}.yaml" | apply_ing "${f%/*}" "${f#*/}-tls"
done
# Ops UIs: shared "ops" password in Vault secret/platform/ops-ui; a route is created only if its service exists.
if ! api "$VAULT_ADDR/v1/secret/data/platform/ops-ui" >/dev/null 2>&1; then
  pw=$(python3 -c 'import secrets;print(secrets.token_urlsafe(15))')
  api -X POST "$VAULT_ADDR/v1/secret/data/platform/ops-ui" -d "{\"data\":{\"user\":\"ops\",\"password\":\"$pw\"}}" >/dev/null
fi
ops_pw=$(api "$VAULT_ADDR/v1/secret/data/platform/ops-ui" | python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["data"]["password"])')
# route-name  namespace  service  port  host
while read -r name ns svc port host; do
  $K -n "$ns" get svc "$svc" >/dev/null 2>&1 || { echo "skipping $name: $ns/$svc does not exist yet"; continue; }
  $K -n "$ns" create secret generic ops-basic-auth --type=nginx.org/htpasswd \
    --from-literal=htpasswd="ops:$(openssl passwd -apr1 "$ops_pw")" --dry-run=client -o yaml | $K apply -f - >/dev/null
  sed -e "s/@@NAME@@/$name/g" -e "s/@@NS@@/$ns/g" -e "s/@@SVC@@/$svc/g" -e "s/@@PORT@@/$port/g" \
    -e "s/@@HOST@@/$host.$host_suffix/g" -e "s/cluster-issuer: platform-local-ca/cluster-issuer: $issuer/" \
    infra/k8s/ingress/ops-ui.yaml | apply_ing "$ns" "$name-tls"
  echo "route https://$host.$host_suffix:$https_port → $ns/$svc:$port"
done <<'ROUTES'
grafana observability grafana 80 grafana
prometheus observability prometheus-server 80 metrics
jaeger observability jaeger 16686 traces
agent-registry agentregistry agentregistry 12121 registry
ROUTES
printf 'https://api.%s:%s  https://agent.%s:%s (user seller, password in Vault secret/platform/agent-ui)\n' "$host_suffix" "$https_port" "$host_suffix" "$https_port"
