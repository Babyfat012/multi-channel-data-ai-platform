"""Init (first time) + unseal Vault, enable KV v2, load PLATFORM secrets from .env, create a read-only CI policy/token.

Unseal key + root token are stored in out/vault/ (gitignored, mode 600), local use only; a cloud setup would use
KMS auto-unseal instead. Safe to rerun.

    python3 -m scripts.shared.setup_vault
"""

import base64
import json
import os
import time
import urllib.error
import urllib.request

from scripts.common import ROOT, env, load_env, run, vault, vault_addr, vault_put_secret

OUT = ROOT / "out/vault"


def write_private(path, text: str) -> None:
    """Write a file readable only by the owner (mode 600 from the start, no world-readable window)."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(text)


def wait_until_reachable(seconds: int = 30) -> None:
    """Wait for the Vault HTTP port; any HTTP answer (even 5xx while sealed) counts as up."""
    for _ in range(seconds):
        try:
            urllib.request.urlopen(f"{vault_addr()}/v1/sys/health", timeout=2)
            return
        except urllib.error.HTTPError:
            return
        except OSError:
            time.sleep(1)


def agentgateway_kubectl() -> list[str]:
    """kubectl that can read the agentgateway Secret: EC2 (s3) has no host kubeconfig ⇒ go through Jenkins' EKS one."""
    if os.environ.get("PLATFORM_STORAGE", "minio") == "s3":
        return [
            "docker",
            "compose",
            "exec",
            "-T",
            "jenkins",
            "kubectl",
            "--kubeconfig",
            "/run/secrets/kubeconfig",
            "--context",
            os.environ.get("EKS_CLUSTER", "platform-apps"),
        ]
    return ["kubectl", "--context", os.environ.get("KCTX", "kind-platform")]


def main() -> None:
    """Bring Vault to a ready state and (re)load the platform secrets."""
    os.chdir(ROOT)
    load_env()
    os.environ["VAULT_ADDR"] = f"http://localhost:{os.environ.get('VAULT_PORT', '8200')}"
    run(["docker", "compose", "--profile", "security", "up", "-d", "vault"], capture=True)
    wait_until_reachable()
    OUT.mkdir(parents=True, exist_ok=True)
    OUT.chmod(0o700)
    if not vault("GET", "sys/init", "")["initialized"]:
        init = vault("PUT", "sys/init", "", {"secret_shares": 1, "secret_threshold": 1})
        write_private(OUT / "init.json", json.dumps(init))
    init = json.loads((OUT / "init.json").read_text())
    root = init["root_token"]
    vault("PUT", "sys/unseal", root, {"key": init["keys"][0]})
    mounts = vault("GET", "sys/mounts", root)
    if "secret/" not in (mounts.get("data") or mounts):
        vault("POST", "sys/mounts/secret", root, {"type": "kv", "options": {"version": "2"}})

    vault_put_secret(
        "minio", {"user": env("MINIO_ROOT_USER"), "password": env("MINIO_ROOT_PASSWORD")}, root
    )
    vault_put_secret(
        "postgres",
        {
            "user": env("POSTGRES_USER"),
            "password": env("POSTGRES_PASSWORD"),
            "db": env("POSTGRES_DB"),
        },
        root,
    )
    # agentgateway's agent key comes from the k8s Secret made by its Helm chart; skipped while the cluster lacks it.
    # On EC2, rerun this AFTER setup_jenkins (the Jenkins container holds the EKS kubeconfig).
    encoded = run(
        [
            *agentgateway_kubectl(),
            "-n",
            "agentgateway",
            "get",
            "secret",
            "agentgateway-keys",
            "-o",
            "jsonpath={.data.AGW_AGENT_KEY}",
        ],
        capture=True,
        check=False,
    )
    if encoded:
        vault_put_secret(
            "agentgateway", {"AGW_AGENT_KEY": base64.b64decode(encoded).decode()}, root
        )

    vault(
        "PUT",
        "sys/policies/acl/platform-read",
        root,
        {"policy": 'path "secret/data/platform/*" { capabilities = ["read"] }'},
    )
    ci_token = OUT / "ci-token"
    if not ci_token.exists() or not ci_token.stat().st_size:
        created = vault(
            "POST",
            "auth/token/create",
            root,
            {"policies": ["platform-read"], "period": "768h", "display_name": "platform-ci"},
        )
        write_private(ci_token, created["auth"]["client_token"] + "\n")
    print(
        f"Vault ready: {vault_addr()}, secrets in secret/platform/{{minio,postgres}}; "
        "read-only token: out/vault/ci-token"
    )


if __name__ == "__main__":
    main()
