"""Vault: the agentgateway key is copied from the EKS Secret even when running on EC2 (host has no kubeconfig)."""

from scripts.shared.setup_vault import agentgateway_kubectl


def test_agentgateway_key_is_read_through_jenkins_kubeconfig_on_ec2(monkeypatch):
    """Without the s3 branch ⇒ host kubectl points at kind, the error is swallowed ⇒ Vault has no key, CI eval 404s."""
    monkeypatch.setenv("PLATFORM_STORAGE", "s3")
    assert agentgateway_kubectl()[:8] == [
        "docker",
        "compose",
        "exec",
        "-T",
        "jenkins",
        "kubectl",
        "--kubeconfig",
        "/run/secrets/kubeconfig",
    ]
    monkeypatch.setenv("PLATFORM_STORAGE", "minio")
    monkeypatch.delenv("KCTX", raising=False)
    assert agentgateway_kubectl() == ["kubectl", "--context", "kind-platform"]
