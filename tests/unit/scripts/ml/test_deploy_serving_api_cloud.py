"""Exercise the serving-api deployment with fake kubectl/Helm, without changing a cluster."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[4]


def run_deploy(env, tag="revision-123", check=True):
    """Run the deploy module from the repo root with fake kubectl/helm on PATH."""
    return subprocess.run(
        [sys.executable, "-m", "scripts.ml.deploy_serving_api", tag],
        cwd=ROOT,
        env=env,
        check=check,
        capture_output=True,
    )


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


def calls_of(env):
    """Fake kubectl/helm calls as (tool, args, stdin)."""
    return [json.loads(line) for line in Path(env["CALL_LOG"]).read_text().splitlines()]


def secret_body(calls, release):
    """Env file piped into `kubectl create secret generic <release>-env`."""
    return next(body for _, args, body in calls if f"{release}-env" in args)


def helm_upgrade(calls, release):
    """Arguments of `helm upgrade --install <release>`."""
    return next(a for tool, a, _ in calls if tool == "helm" and "upgrade" in a and release in a)


def test_cloud_uses_selected_context_and_pod_identity_without_static_keys(cloud):
    """S3 registry, ClusterIP values and service account replace local networking and keys."""
    run_deploy(cloud)
    calls = calls_of(cloud)
    assert all("platform-apps" in args for _, args, _ in calls)
    assert all("serving-api" in args for _, args, _ in calls)
    features = secret_body(calls, "feature-api")
    assert "FEAST_REGISTRY=s3://test-lakehouse/feature-store/registry.db" in features
    assert "AWS_REGION=ap-southeast-1" in features
    for body in (features, secret_body(calls, "policy-api")):
        assert "AWS_ACCESS_KEY_ID" not in body and "FEAST_S3_ENDPOINT_URL" not in body
        assert not any(old in body for old in ("old-access", "old-secret", "old-inherited-key"))
    deploy = helm_upgrade(calls, "feature-api")
    assert "infra/helm/serving-api/values-features.yaml" in deploy
    assert "infra/helm/serving-api/values-aws.yaml" in deploy
    assert "image.tag=revision-123" in deploy
    assert any(a.endswith("/platform-serving-api") for a in deploy)


def test_each_service_gets_only_its_own_credentials(cloud):
    """Least privilege per pod: the MCP server holds no data credentials, only the URLs of the services it calls."""
    run_deploy(cloud)
    calls = calls_of(cloud)
    features, policy, mcp = (
        secret_body(calls, r) for r in ("feature-api", "policy-api", "mcp-server")
    )
    assert "RAG_PG_PASSWORD" not in features and "FEAST_REGISTRY" not in policy
    assert "RAG_PG_PASSWORD=test-secret" in policy
    assert mcp.splitlines() == [
        "FEATURE_API_URL=http://feature-api.serving-api.svc.cluster.local:8000",
        "POLICY_API_URL=http://policy-api.serving-api.svc.cluster.local:8000",
        "KSERVE_URL=http://return-risk-predictor.ml-serving.svc.cluster.local/v1/models/return-risk:predict",
    ]
    for release, values in (
        ("feature-api", "features"),
        ("policy-api", "policy"),
        ("mcp-server", "mcp"),
    ):
        assert f"infra/helm/serving-api/values-{values}.yaml" in helm_upgrade(calls, release)


@pytest.mark.parametrize("bad_value", ["kind-platform", ""])
def test_cloud_target_is_required_before_any_mutation(cloud, bad_value):
    """A missing/local kubecontext fails before touching namespaces or secrets."""
    cloud["KCTX"] = bad_value
    result = run_deploy(cloud, check=False)
    assert result.returncode != 0
    assert not Path(cloud["CALL_LOG"]).exists()


def test_multiline_secret_fails_before_any_mutation(cloud):
    """Invalid env serialization cannot overwrite an existing Secret with partial data."""
    cloud["POSTGRES_PASSWORD"] = "line1\nAWS_ACCESS_KEY_ID=injected"
    result = run_deploy(cloud, check=False)
    assert result.returncode != 0
    assert not Path(cloud["CALL_LOG"]).exists()


@pytest.mark.parametrize("service", ["features", "policy", "mcp"])
def test_chart_is_cluster_internal_with_one_service_account_per_release(service):
    """No NodePort; each release runs as its own ServiceAccount (Pod Identity binds by that name) and Secret."""
    result = subprocess.run(
        [
            "helm",
            "template",
            "some-api",
            "infra/helm/serving-api",
            "-f",
            f"infra/helm/serving-api/values-{service}.yaml",
            "-f",
            "infra/helm/serving-api/values-aws.yaml",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    docs = [x for x in yaml.safe_load_all(result.stdout) if x]
    svc = next(x for x in docs if x["kind"] == "Service")
    assert svc["spec"]["type"] == "ClusterIP"
    assert all("nodePort" not in port for port in svc["spec"]["ports"])
    pod = next(x for x in docs if x["kind"] == "Deployment")["spec"]["template"]["spec"]
    assert pod["serviceAccountName"] == "some-api"
    container = pod["containers"][0]
    assert container["envFrom"] == [{"secretRef": {"name": "some-api-env"}}]
    assert container["env"] == [{"name": "API_SURFACE", "value": service}]
    assert any(x["kind"] == "ServiceAccount" and x["metadata"]["name"] == "some-api" for x in docs)
    assert any(x["kind"] == "ScaledObject" for x in docs)


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
    run_deploy(cloud)
    secret = secret_body(calls_of(cloud), "policy-api")
    rag = [line for line in secret.splitlines() if line.startswith("FEAST_RAG_REGISTRY=")]
    assert rag == ([f"FEAST_RAG_REGISTRY={expected}"] if expected else [])


def test_changed_secret_env_rolls_only_that_service(cloud):
    """A Secret change alone does not replace pods ⇒ env checksum in the pod template, per release:
    changing the RAG registry rolls policy-api only; the checksum never contains the secret itself."""

    def checksums(env):
        Path(env["CALL_LOG"]).unlink(missing_ok=True)
        run_deploy(env, "rev")
        calls = calls_of(env)
        values = {}
        for release in ("feature-api", "policy-api", "mcp-server"):
            value = next(a for a in helm_upgrade(calls, release) if a.startswith("envChecksum="))
            assert "test-secret" not in value
            values[release] = value
        return values

    first = checksums(cloud)
    assert checksums(cloud) == first
    changed = checksums({**cloud, "FEAST_RAG_REGISTRY": "s3://test-lakehouse/rag.db"})
    assert changed["policy-api"] != first["policy-api"]
    assert (
        changed["feature-api"] == first["feature-api"]
        and changed["mcp-server"] == first["mcp-server"]
    )
