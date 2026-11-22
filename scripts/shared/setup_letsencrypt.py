"""Opt-in real-domain HTTPS (Let's Encrypt via the DuckDNS DNS-01 webhook) for the Feature API.

Without DUCKDNS_DOMAIN nothing is called (no Docker/kubectl/Vault/network). To enable: DUCKDNS_DOMAIN=<name>.duckdns.org,
LE_EMAIL, DUCKDNS_WEBHOOK_CHART (a checked local chart, version 1.0.0), VAULT_ADDR + VAULT_TOKEN (secret/platform/duckdns),
LE_ENV=staging (default) | production.

    python3 -m scripts.shared.setup_letsencrypt
"""

import os
import re
import sys
from pathlib import Path

from scripts.common import ROOT, apply, env, kubectl, run, vault_secret

ACME_SERVERS = {
    "staging": "https://acme-staging-v02.api.letsencrypt.org/directory",
    "production": "https://acme-v02.api.letsencrypt.org/directory",
}


def duckdns_ingress(domain: str, le_env: str) -> dict:
    """Separate Ingress on the DuckDNS host: leaves the existing host and its internal-CA certificate alone.

    Only the Feature API is routed here; the full API host (all services, mergeable Ingress) is api.<suffix>.
    """
    return {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "Ingress",
        "metadata": {
            "name": "feature-api-duckdns",
            "namespace": "serving-api",
            "annotations": {
                "cert-manager.io/cluster-issuer": f"platform-letsencrypt-{le_env}",
                "nginx.org/redirect-to-https": "true",
                "nginx.org/limit-req-rate": "50r/s",
                "nginx.org/limit-req-burst": "100",
            },
        },
        "spec": {
            "ingressClassName": "nginx",
            "tls": [{"hosts": [domain], "secretName": f"feature-api-duckdns-{le_env}-tls"}],
            "rules": [
                {
                    "host": domain,
                    "http": {
                        "paths": [
                            {
                                "path": "/",
                                "pathType": "Prefix",
                                "backend": {
                                    "service": {"name": "feature-api", "port": {"number": 8000}}
                                },
                            }
                        ]
                    },
                }
            ],
        },
    }


def main() -> None:
    """Validate inputs, store the DuckDNS token, install the webhook and request the certificate."""
    domain = os.environ.get("DUCKDNS_DOMAIN")
    if not domain:
        print(
            "Keeping the internal CA. To enable ACME: set DUCKDNS_DOMAIN=<name>.duckdns.org, LE_EMAIL, "
            "DUCKDNS_WEBHOOK_CHART (checked local chart), VAULT_ADDR/VAULT_TOKEN."
        )
        return
    os.chdir(ROOT)
    if not re.fullmatch(r"[a-z0-9]([a-z0-9-]*[a-z0-9])?\.duckdns\.org", domain):
        sys.exit("Invalid DuckDNS domain")
    email, chart = env("LE_EMAIL"), env("DUCKDNS_WEBHOOK_CHART")
    if not re.fullmatch(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+", email):
        sys.exit("Invalid LE_EMAIL")
    le_env = os.environ.get("LE_ENV", "staging")
    if le_env not in ACME_SERVERS:
        sys.exit("LE_ENV only accepts staging or production")
    # Pinned local chart: this script never downloads charts from the network.
    if not Path(chart).is_dir():
        sys.exit("A local chart directory is required")
    chart_yaml = run(["helm", "show", "chart", chart], capture=True)
    version = re.search(r"^version:\s*[\"']?([^\"'\s]+)", chart_yaml, re.M)
    if not version or version[1] != "1.0.0":
        sys.exit("Chart must match the 1.0.0 pin")
    env("VAULT_ADDR")
    token = (vault_secret("duckdns", env("VAULT_TOKEN")) or {}).get("token")
    if not token:
        sys.exit("missing token in Vault secret/platform/duckdns")

    kctx = os.environ.get("KCTX", "kind-platform")
    kc = kubectl(kctx)
    apply(
        kc,
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "duckdns-token", "namespace": "cert-manager"},
            "stringData": {"token": token},
        },
    )
    run(
        [
            "helm",
            "--kube-context",
            kctx,
            "upgrade",
            "--install",
            "duckdns-webhook",
            chart,
            "-n",
            "cert-manager",
            "--version",
            "1.0.0",
            "--set",
            "groupName=acme.platform.duckdns.org",
            "--wait",
            "--timeout",
            "180s",
        ]
    )
    issuer = (ROOT / "infra/k8s/ingress/issuer-letsencrypt.yaml").read_text()
    for key, value in {
        "LE_ENV": le_env,
        "ACME_SERVER": ACME_SERVERS[le_env],
        "LE_EMAIL": email,
        "DUCKDNS_DOMAIN": domain,
    }.items():
        issuer = issuer.replace(f"@@{key}@@", value)
    apply(kc, issuer)
    apply(kc, duckdns_ingress(domain, le_env))
    print(
        f"Requested {le_env} cert for {domain}. Check the Certificate is Ready before switching traffic."
    )


if __name__ == "__main__":
    main()
