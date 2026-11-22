"""Start Jenkins (compose profile ci) with its secrets from Vault. Requires setup_vault first. UI: http://localhost:8090.

- admin password: generated once, kept in Vault secret/platform/jenkins (user admin)
- `vault-ci-token` credential: the read-only token from out/vault/ci-token
- `kind-kubeconfig` credential: local kind, or EKS when PLATFORM_STORAGE=s3 (EC2, set by Ansible in .env)

    python3 -m scripts.shared.setup_jenkins
"""

import grp
import os
import secrets
import time
import urllib.error
import urllib.request

from scripts.common import ROOT, load_env, run, vault_put_secret, vault_root_token, vault_secret

KUBECONFIG = ROOT / "out/jenkins/kubeconfig"


def kubeconfig_eks() -> None:
    """EKS kubeconfig using `aws eks get-token` with the EC2 IAM role (group platform-ci, infra/k8s/ci/rbac.yaml)."""
    cluster = os.environ.get("EKS_CLUSTER", "platform-apps")
    run(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            "host",
            "-v",
            f"{KUBECONFIG.parent}:/out",
            "amazon/aws-cli:2.31.0",
            "eks",
            "update-kubeconfig",
            "--region",
            os.environ["AWS_REGION"],
            "--name",
            cluster,
            "--alias",
            cluster,
            "--kubeconfig",
            "/out/kubeconfig",
        ],
        capture=True,
    )
    run(
        ["sudo", "chown", f"{os.getuid()}:{os.getgid()}", str(KUBECONFIG)]
    )  # written by root inside the container


def kubeconfig_kind() -> None:
    """Local kind kubeconfig."""
    KUBECONFIG.write_text(
        run(["kind", "get", "kubeconfig", "--name", "platform"], capture=True) + "\n"
    )


def wait_for_login_page(tries: int = 90) -> None:
    """Poll the login page until Jenkins answers 200 (up to ~3 minutes)."""
    for _ in range(tries):
        try:
            with urllib.request.urlopen("http://localhost:8090/login", timeout=2) as response:
                if response.status == 200:
                    return
        except (urllib.error.URLError, OSError):
            pass
        time.sleep(2)


def main() -> None:
    """Prepare password, CI token and kubeconfig, then start the container."""
    os.chdir(ROOT)
    load_env(required=False)
    os.environ["VAULT_ADDR"] = f"http://localhost:{os.environ.get('VAULT_PORT', '8200')}"
    root = vault_root_token()
    if vault_secret("jenkins", root) is None:
        vault_put_secret("jenkins", {"admin_password": secrets.token_urlsafe(18)}, root)
    compose_env = {
        **os.environ,
        "JENKINS_ADMIN_PASSWORD": vault_secret("jenkins", root)["admin_password"],
        "VAULT_CI_TOKEN": (ROOT / "out/vault/ci-token").read_text().strip(),
        "DOCKER_GID": str(grp.getgrnam("docker").gr_gid),
        "PWD": str(ROOT),
    }
    (ROOT / "out/jenkins_home").mkdir(parents=True, exist_ok=True)
    KUBECONFIG.parent.mkdir(parents=True, exist_ok=True)
    if os.environ.get("PLATFORM_STORAGE", "minio") == "s3":
        kubeconfig_eks()
    else:
        kubeconfig_kind()
    KUBECONFIG.chmod(0o600)
    os.environ.update(compose_env)
    run(["docker", "compose", "--profile", "ci", "up", "-d", "--build", "jenkins"])
    wait_for_login_page()
    print("Jenkins: http://localhost:8090  user admin, password in Vault secret/platform/jenkins")


if __name__ == "__main__":
    main()
