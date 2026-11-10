"""Vault: the agentgateway key is copied from the EKS Secret even when running on EC2 (host has no kubeconfig)."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
SCRIPT = (ROOT / "scripts/setup/setup_vault.sh").read_text()


def test_agentgateway_key_is_read_through_jenkins_kubeconfig_on_ec2():
    """Without the s3 branch ⇒ host kubectl points at kind, the error is swallowed ⇒ Vault has no key, CI eval 404s."""
    s3 = SCRIPT[SCRIPT.index('if [[ "${PLATFORM_STORAGE:-minio}" == s3 ]]; then') :].split("else")[
        0
    ]
    assert "docker compose exec -T jenkins kubectl --kubeconfig /run/secrets/kubeconfig" in s3
    assert 'if agw=$("${KC[@]}" -n agentgateway get secret agentgateway-keys' in SCRIPT
