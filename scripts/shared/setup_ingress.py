"""Create the HTTPS routes behind NGINX Ingress: Feature API, agent UI (basic auth + rate limit) and the ops UIs.

Hosts are <name>.<node-ip>.sslip.io on kind (port 30443) or <name>.$INGRESS_DOMAIN in the cloud. Certificates come from
the internal CA, or Let's Encrypt with INGRESS_ISSUER=platform-letsencrypt (+ LE_EMAIL). Passwords are generated once
and kept in Vault: secret/platform/agent-ui (user seller) and secret/platform/ops-ui (user ops).
Requires: the NGINX ingress controller, cert-manager and Vault (VAULT_TOKEN, or the local out/vault/init.json).

    python3 -m scripts.shared.setup_ingress
"""

import os
import re
import secrets
import sys
import time

from scripts.common import (
    ROOT,
    apply,
    kubectl,
    run,
    succeeds,
    vault_put_secret,
    vault_root_token,
    vault_secret,
)

INGRESS = ROOT / "infra/k8s/ingress"
ISSUERS = ("platform-local-ca", "platform-letsencrypt")
# route name, namespace, service, port, host prefix; a route is created only when its service exists.
OPS_ROUTES = [
    ("grafana", "observability", "grafana", 80, "grafana"),
    ("prometheus", "observability", "prometheus-server", 80, "metrics"),
    ("jaeger", "observability", "jaeger", 16686, "traces"),
    ("agent-registry", "agentregistry", "agentregistry", 12121, "registry"),
]


def host_suffix_and_port() -> tuple[str, int]:
    """INGRESS_DOMAIN when set; on kind derive <node-ip>.sslip.io from the control-plane container."""
    domain = os.environ.get("INGRESS_DOMAIN")
    if domain:
        if not re.fullmatch(r"[a-zA-Z0-9.-]+", domain):
            sys.exit("Invalid INGRESS_DOMAIN")
        return domain, int(os.environ.get("INGRESS_HTTPS_PORT", "443"))
    if os.environ.get("KCTX", "kind-platform").startswith("kind-"):
        node_ip = run(
            [
                "docker",
                "inspect",
                "platform-control-plane",
                "--format",
                '{{(index .NetworkSettings.Networks "kind").IPAddress}}',
            ],
            capture=True,
        )
        return f"{node_ip.replace('.', '-')}.sslip.io", 30443
    sys.exit("Set INGRESS_DOMAIN and point DNS at the cloud ingress before creating routes.")


def open_for_http01(manifest: str) -> str:
    """Version of an Ingress that lets the first HTTP-01 challenge through: no basic auth, no HTTPS redirect."""
    lines = []
    for line in manifest.splitlines():
        if "nginx.org/basic-auth" in line:
            continue
        lines.append(line)
        if re.match(r"^    acme\.cert-manager\.io/http01-edit-in-place: ", line):
            lines.append('    ingress.kubernetes.io/ssl-redirect: "false"')
    return "\n".join(lines) + "\n"


def apply_ingress(
    kc: list[str], issuer: str, namespace: str, tls_secret: str, manifest: str
) -> None:
    """Apply a rendered Ingress. First Let's Encrypt run: apply the open version, wait for the cert, then the full one.

    Limitation: the day-60 renewal is blocked by basic auth again ⇒ rerun this script (gap #24); prod would use a
    mergeable ingress or DNS-01.
    """
    ns = [*kc, "-n", namespace]
    if issuer == "platform-letsencrypt":
        ready = run(
            [
                *ns,
                "get",
                "certificate",
                tls_secret,
                "-o",
                'jsonpath={.status.conditions[?(@.type=="Ready")].status}',
            ],
            capture=True,
            check=False,
        )
        if ready != "True":
            apply(kc, open_for_http01(manifest))
            for _ in range(30):
                if succeeds([*ns, "get", "certificate", tls_secret]):
                    break
                time.sleep(2)
            if not succeeds(
                [
                    *ns,
                    "wait",
                    "--for=condition=Ready",
                    f"certificate/{tls_secret}",
                    "--timeout=300s",
                ]
            ):
                sys.exit(
                    f"cert {namespace}/{tls_secret} not Ready: {' '.join(ns)} describe challenges"
                )
    apply(kc, manifest)


def render(path, issuer: str, **placeholders: str) -> str:
    """Fill @@KEY@@ placeholders and switch the cluster issuer away from the internal CA when asked."""
    text = path.read_text().replace(
        "cluster-issuer: platform-local-ca", f"cluster-issuer: {issuer}"
    )
    for key, value in placeholders.items():
        text = text.replace(f"@@{key}@@", value)
    return text


def ui_password(name: str, user: str, token: str) -> str:
    """Password of an ops/agent UI, generated and stored in Vault on the first run."""
    if vault_secret(name, token) is None:
        vault_put_secret(name, {"user": user, "password": secrets.token_urlsafe(15)}, token)
    return vault_secret(name, token)["password"]


def htpasswd_secret(kc: list[str], namespace: str, name: str, user: str, password: str) -> None:
    """nginx.org/htpasswd Secret; the password goes to openssl via stdin, never on the command line."""
    hashed = run(["openssl", "passwd", "-apr1", "-stdin"], stdin=password + "\n", capture=True)
    apply(
        kc,
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "type": "nginx.org/htpasswd",
            "metadata": {"name": name, "namespace": namespace},
            "stringData": {"htpasswd": f"{user}:{hashed}"},
        },
    )


def main() -> None:
    """Issuer, then the app routes, then the ops routes."""
    os.chdir(ROOT)
    kc = kubectl()
    host_suffix, https_port = host_suffix_and_port()
    apply(kc, (INGRESS / "issuer.yaml").read_text())
    run(
        [
            *kc,
            "-n",
            "cert-manager",
            "wait",
            "--for=condition=Ready",
            "certificate/platform-local-ca",
            "--timeout=120s",
        ],
        capture=True,
    )
    issuer = os.environ.get("INGRESS_ISSUER", "platform-local-ca")
    if issuer not in ISSUERS:
        sys.exit("INGRESS_ISSUER only accepts platform-local-ca or platform-letsencrypt")
    if issuer == "platform-letsencrypt":
        email = os.environ.get("LE_EMAIL") or sys.exit("Set LE_EMAIL for the ACME account")
        if not re.fullmatch(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+", email):
            sys.exit("Invalid LE_EMAIL")
        apply(
            kc,
            (INGRESS / "issuer-letsencrypt-http01.yaml").read_text().replace("@@LE_EMAIL@@", email),
        )

    # Vault on EC2: pass VAULT_TOKEN (tunnel 8200); locally the root token from init.json is used.
    token = os.environ.get("VAULT_TOKEN") or vault_root_token()
    htpasswd_secret(
        kc, "kagent", "agent-ui-basic-auth", "seller", ui_password("agent-ui", "seller", token)
    )
    for namespace, name in (("feature-store", "feature-api"), ("kagent", "agent-ui")):
        manifest = render(INGRESS / f"{name}.yaml", issuer).replace(
            "@@NODE_IP_DASH@@.sslip.io", host_suffix
        )
        apply_ingress(kc, issuer, namespace, f"{name}-tls", manifest)

    ops_password = ui_password("ops-ui", "ops", token)
    for name, namespace, service, port, host in OPS_ROUTES:
        if not succeeds([*kc, "-n", namespace, "get", "svc", service]):
            print(f"skipping {name}: {namespace}/{service} does not exist yet")
            continue
        htpasswd_secret(kc, namespace, "ops-basic-auth", "ops", ops_password)
        manifest = render(
            INGRESS / "ops-ui.yaml",
            issuer,
            NAME=name,
            NS=namespace,
            SVC=service,
            PORT=str(port),
            HOST=f"{host}.{host_suffix}",
        )
        apply_ingress(kc, issuer, namespace, f"{name}-tls", manifest)
        print(f"route https://{host}.{host_suffix}:{https_port} → {namespace}/{service}:{port}")
    print(
        f"https://api.{host_suffix}:{https_port}  https://agent.{host_suffix}:{https_port} "
        "(user seller, password in Vault secret/platform/agent-ui)"
    )


if __name__ == "__main__":
    main()
