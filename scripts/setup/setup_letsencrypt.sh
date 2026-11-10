#!/usr/bin/env bash
# Real-domain HTTPS is opt-in; without a domain, no Docker/kubectl/Vault/network calls are made.
# Before enabling: check the version/schema of the DuckDNS webhook chart.
set -euo pipefail
if [[ -z "${DUCKDNS_DOMAIN:-}" ]]; then
  echo 'Keeping the internal CA. To enable ACME: set DUCKDNS_DOMAIN=<name>.duckdns.org, LE_EMAIL, DUCKDNS_WEBHOOK_CHART (checked local chart), VAULT_ADDR/VAULT_TOKEN.'
  exit 0
fi
cd "$(dirname "$0")/../.."
[[ "$DUCKDNS_DOMAIN" =~ ^[a-z0-9]([a-z0-9-]*[a-z0-9])?\.duckdns\.org$ ]] || { echo 'Invalid DuckDNS domain' >&2; exit 1; }
: "${LE_EMAIL:?set the ACME email}" "${DUCKDNS_WEBHOOK_CHART:?path to the checked local chart}"
[[ "$LE_EMAIL" =~ ^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+$ ]] || exit 1
LE_ENV=${LE_ENV:-staging}
case "$LE_ENV" in
  staging) ACME_SERVER=https://acme-staging-v02.api.letsencrypt.org/directory ;;
  production) ACME_SERVER=https://acme-v02.api.letsencrypt.org/directory ;;
  *) echo 'LE_ENV only accepts staging or production' >&2; exit 1 ;;
esac
# Pinned local chart: this script never downloads charts from the network.
[[ -d "$DUCKDNS_WEBHOOK_CHART" ]] || { echo 'A local chart directory is required' >&2; exit 1; }
chart_version=$(helm show chart "$DUCKDNS_WEBHOOK_CHART" | sed -n 's/^version: *["\x27]*\([^"\x27 ]*\).*/\1/p')
[[ "$chart_version" == 1.0.0 ]] || { echo 'Chart must match the 1.0.0 pin' >&2; exit 1; }
eval "$(scripts/setup/vault_env.sh duckdns)"
: "${DUCKDNS_TOKEN:?missing token in Vault secret/platform/duckdns}"
K=(kubectl --context "${KCTX:-kind-platform}")
# Secrets go via stdin, never via argv/logs/repo files.
printf '%s' "$DUCKDNS_TOKEN" | "${K[@]}" -n cert-manager create secret generic duckdns-token \
  --from-file=token=/dev/stdin --dry-run=client -o yaml | "${K[@]}" apply -f - >/dev/null
unset DUCKDNS_TOKEN
helm --kube-context "${KCTX:-kind-platform}" upgrade --install duckdns-webhook "$DUCKDNS_WEBHOOK_CHART" \
  -n cert-manager --version 1.0.0 --set groupName=acme.platform.duckdns.org --wait --timeout 180s
sed -e "s|@@LE_ENV@@|$LE_ENV|g" -e "s|@@ACME_SERVER@@|$ACME_SERVER|g" \
  -e "s|@@LE_EMAIL@@|$LE_EMAIL|g" -e "s|@@DUCKDNS_DOMAIN@@|$DUCKDNS_DOMAIN|g" \
  infra/k8s/ingress/issuer-letsencrypt.yaml | "${K[@]}" apply -f -
# Separate Ingress: does not overwrite the host/internal CA certificate in use.
cat <<YAML | "${K[@]}" apply -f -
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: feature-api-duckdns
  namespace: feature-store
  annotations:
    cert-manager.io/cluster-issuer: platform-letsencrypt-$LE_ENV
    nginx.org/redirect-to-https: "true"
    nginx.org/limit-req-rate: "50r/s"
    nginx.org/limit-req-burst: "100"
spec:
  ingressClassName: nginx
  tls:
    - hosts: ["$DUCKDNS_DOMAIN"]
      secretName: feature-api-duckdns-$LE_ENV-tls
  rules:
    - host: $DUCKDNS_DOMAIN
      http:
        paths:
          - path: /
            pathType: Prefix
            backend: {service: {name: feature-api, port: {number: 8000}}}
YAML
printf 'Requested %s cert for %s. Check the Certificate is Ready before switching traffic.\n' "$LE_ENV" "$DUCKDNS_DOMAIN"
