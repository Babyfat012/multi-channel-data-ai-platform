"""Exercise cloud deployment commands using fake kubectl/Helm, without changing a cluster."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts/setup/deploy_feature_api.sh"


@pytest.fixture
def cloud(tmp_path):
    """Log fake deployment calls; never read real kubeconfig, AWS credentials or local .env."""
    fake = tmp_path / "fake.py"
    fake.write_text(
        f"#!{sys.executable}\n"
        "import json,os,sys,pathlib\n"
        "args=sys.argv[1:]; tool=pathlib.Path(sys.argv[0]).name\n"
        "body=sys.stdin.read() if ('/dev/stdin' in ' '.join(args) or '-f' in args and args[-1]=='-') else ''\n"
        "with open(os.environ['CALL_LOG'],'a') as f: f.write(json.dumps([tool,args,body])+'\\n')\n"
        "if '--dry-run=client' in args: print('apiVersion: v1\\nkind: Secret\\nmetadata: {name: fake}')\n"
    )
    fake.chmod(0o755)
    for name in ["kubectl", "helm"]:
        (tmp_path / name).symlink_to(fake)
    log = tmp_path / "calls.jsonl"
    return {
        **os.environ,
        "PATH": f"{tmp_path}:{os.environ['PATH']}",
        "CALL_LOG": str(log),
        "PLATFORM_STORAGE": "s3",
        "KCTX": "platform-apps",
        "REGISTRY": "123456789012.dkr.ecr.ap-southeast-1.amazonaws.com/platform",
        "AWS_REGION": "ap-southeast-1",
        "LAKEHOUSE_BUCKET": "test-lakehouse",
        "POSTGRES_USER": "test-user",
        "POSTGRES_PASSWORD": "test-secret",
        "SKIP_BUILD_PUSH": "1",
        "MINIO_ROOT_USER": "old-access",
        "MINIO_ROOT_PASSWORD": "old-secret",
        "AWS_ACCESS_KEY_ID": "old-inherited-key",
    }


def test_cloud_uses_selected_context_and_pod_identity_without_static_keys(cloud):
    """S3 registry, ClusterIP values and service account replace local networking and keys."""
    subprocess.run(
        ["bash", str(SCRIPT), "revision-123"], env=cloud, check=True, capture_output=True
    )
    calls = [json.loads(line) for line in Path(cloud["CALL_LOG"]).read_text().splitlines()]
    assert all("platform-apps" in args for _, args, _ in calls)
    secret = next(body for _, args, body in calls if "--from-env-file=/dev/stdin" in args)
    assert "FEAST_REGISTRY=s3://test-lakehouse/feature-store/registry.db" in secret
    assert "AWS_REGION=ap-southeast-1" in secret
    assert "AWS_ACCESS_KEY_ID" not in secret and "FEAST_S3_ENDPOINT_URL" not in secret
    assert (
        "old-access" not in secret
        and "old-secret" not in secret
        and "old-inherited-key" not in secret
    )
    deploy = next(args for tool, args, _ in calls if tool == "helm" and "upgrade" in args)
    assert "infra/helm/feature-api/values-aws.yaml" in deploy
    assert "image.tag=revision-123" in deploy


@pytest.mark.parametrize("bad_value", ["kind-platform", ""])
def test_cloud_target_is_required_before_any_mutation(cloud, bad_value):
    """A missing/local kubecontext fails before touching namespaces or secrets."""
    cloud["KCTX"] = bad_value
    result = subprocess.run(["bash", str(SCRIPT), "revision-123"], env=cloud, capture_output=True)
    assert result.returncode != 0
    assert not Path(cloud["CALL_LOG"]).exists()


def test_multiline_secret_fails_before_any_mutation(cloud):
    """Invalid env serialization cannot overwrite an existing Secret with partial data."""
    cloud["POSTGRES_PASSWORD"] = "line1\nAWS_ACCESS_KEY_ID=injected"
    result = subprocess.run(["bash", str(SCRIPT), "revision-123"], env=cloud, capture_output=True)
    assert result.returncode != 0
    assert not Path(cloud["CALL_LOG"]).exists()


def test_cloud_chart_does_not_publish_a_node_port():
    """Rendered cloud service stays private and selects the Pod Identity service account."""
    result = subprocess.run(
        [
            "helm",
            "template",
            "feature-api",
            "infra/helm/feature-api",
            "-f",
            "infra/helm/feature-api/values-aws.yaml",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    docs = [x for x in yaml.safe_load_all(result.stdout) if x]
    service = next(x for x in docs if x["kind"] == "Service")
    assert service["spec"]["type"] == "ClusterIP"
    assert all("nodePort" not in port for port in service["spec"]["ports"])
    deployment = next(x for x in docs if x["kind"] == "Deployment")
    assert deployment["spec"]["template"]["spec"]["serviceAccountName"] == "feature-api"
    assert any(
        x["kind"] == "ServiceAccount" and x["metadata"]["name"] == "feature-api" for x in docs
    )


@pytest.mark.parametrize(
    ("given", "expected"),
    [
        (None, None),
        (
            "s3://test-lakehouse/feature-store/rag-registry.db",
            "s3://test-lakehouse/feature-store/rag-registry.db",
        ),
    ],
)
def test_cloud_rag_is_opt_in(cloud, given, expected):
    """Missing RAG registry ⇒ Feast writes a new file itself while Pod Identity is read-only ⇒ enable only when passed explicitly."""
    cloud.pop("FEAST_RAG_REGISTRY", None)
    if given:
        cloud["FEAST_RAG_REGISTRY"] = given
    subprocess.run(
        ["bash", str(SCRIPT), "revision-123"], env=cloud, check=True, capture_output=True
    )
    calls = [json.loads(line) for line in Path(cloud["CALL_LOG"]).read_text().splitlines()]
    secret = next(body for _, args, body in calls if "--from-env-file=/dev/stdin" in args)
    rag = [line for line in secret.splitlines() if line.startswith("FEAST_RAG_REGISTRY=")]
    assert rag == ([f"FEAST_RAG_REGISTRY={expected}"] if expected else [])


def test_changed_secret_env_rolls_the_pods(cloud):
    """A Secret change alone does not replace the pod ⇒ env checksum in the pod template; unchanged env keeps the checksum unchanged."""

    def checksum(env):
        Path(env["CALL_LOG"]).unlink(missing_ok=True)
        subprocess.run(["bash", str(SCRIPT), "rev"], env=env, check=True, capture_output=True)
        calls = [json.loads(line) for line in Path(env["CALL_LOG"]).read_text().splitlines()]
        deploy = next(args for tool, args, _ in calls if tool == "helm" and "upgrade" in args)
        value = next(a for a in deploy if a.startswith("envChecksum="))
        assert "test-secret" not in value
        return value

    first = checksum(cloud)
    assert checksum(cloud) == first
    assert checksum({**cloud, "FEAST_RAG_REGISTRY": "s3://test-lakehouse/rag.db"}) != first
