"""Check the image release contract and CI gates, fully offline."""

import os
import re
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]


def read(path):
    """Read a config file from the repo."""
    return (ROOT / path).read_text()


def test_no_direct_kind_image_loading():
    """Scripts/CI must release images, not load them directly into nodes."""
    for folder in ("scripts", "ci"):
        for path in (ROOT / folder).rglob("*"):
            if path.suffix not in (".sh", ".Jenkinsfile"):
                continue
            for line in path.read_text().splitlines():
                if not line.lstrip().startswith(("#", "//")):
                    assert not re.search(r"\bkind\s+load\b", line), path


def test_build_pipelines_publish_before_deploy():
    """Every build pipeline must Push before the first deploy."""
    for path in (ROOT / "ci/jenkins").glob("*.Jenkinsfile"):
        text = path.read_text()
        stages = re.findall(r"stage\('([^']+)'\)", text)
        if "Build" in stages:
            deploy = next(i for i, name in enumerate(stages) if name.startswith("Deploy"))
            assert stages.index("Build") < stages.index("Push") < deploy, path
            assert "scripts/run/push_images.sh" in text
    for name, deploy in (("feature-api-agents", "Deploy Agent"), ("coordinator", "Deploy")):
        text = read(f"ci/jenkins/{name}.Jenkinsfile")
        stages = re.findall(r"stage\('([^']+)'\)", text)
        assert stages.index("Eval agent") > stages.index(deploy)
        assert "eval_agent_ci.sh" in text
        assert "archiveArtifacts" in text and "out/agent-eval*.json*" in text


def test_registry_and_pull_configuration():
    """The registry binds loopback only, has a volume, and nodes pull via the canonical address."""
    compose = yaml.safe_load(read("docker-compose.yml"))
    registry = compose["services"]["registry"]
    assert registry["image"] == "registry:2.8.3"
    assert registry["ports"] == ["127.0.0.1:5001:5000"]
    assert "registry-data:/var/lib/registry" in registry["volumes"]
    assert compose["networks"]["kind"]["external"] is True
    values = yaml.safe_load(read("infra/helm/feature-api/values.yaml"))
    assert values["image"]["repository"] == "localhost:5001/platform-feature-api"
    assert values["image"]["pullPolicy"] == "IfNotPresent"
    assert "localhost:5001" in read("ml/training_pipeline.py")
    assert 'set_image_pull_policy(task, "IfNotPresent")' in read("ml/training_pipeline.py")
    assert "imagePullPolicy: IfNotPresent" in read("infra/k8s/ml/return-risk.yaml")
    setup = read("scripts/setup/setup_kind_registry.sh")
    assert "/etc/containerd/certs.d/localhost:5001/hosts.toml" in setup
    assert 'server = "http://registry:5000"' in setup
    assert "namespace: kube-public" in setup
    assert "kind create" not in setup and "kind delete" not in setup


@pytest.mark.parametrize(
    "script",
    [
        "scripts/setup/setup_kind_registry.sh",
        "scripts/setup/setup_letsencrypt.sh",
        "scripts/run/push_images.sh",
        "scripts/run/compose_release_images.sh",
        "scripts/run/eval_agent_ci.sh",
        "scripts/setup/deploy_kind_feature_api.sh",
        "scripts/run/deploy_return_risk.sh",
        "scripts/run/run_training_pipeline.sh",
        "scripts/setup/up_ml_base.sh",
        "scripts/setup/vault_env.sh",
    ],
)
def test_shell_syntax(script):
    """Parse bash without running infrastructure or network."""
    subprocess.run(["bash", "-n", str(ROOT / script)], check=True)


def test_letsencrypt_disabled_without_domain():
    """The default branch still runs with no infrastructure tools in PATH."""
    env = {**os.environ, "DUCKDNS_DOMAIN": "", "PATH": "/nonexistent"}
    result = subprocess.run(
        ["/bin/bash", str(ROOT / "scripts/setup/setup_letsencrypt.sh")],
        env=env,
        text=True,
        capture_output=True,
    )
    assert result.returncode == 0
    assert "Keeping the internal CA." in result.stdout
    assert not result.stderr


def test_push_retags_to_immutable_release(tmp_path):
    """Fake Docker records commands: inspect → tag release SHA → push to the same address."""
    docker = tmp_path / "docker"
    docker.write_text('#!/bin/bash\nprintf "%s\\n" "$*" >> "$DOCKER_LOG"\n')
    docker.chmod(0o755)
    log = tmp_path / "docker.log"
    env = {
        **os.environ,
        "PATH": f"{tmp_path}:{os.environ['PATH']}",
        "DOCKER_LOG": str(log),
        "TAG": "abc123",
        "REGISTRY": "localhost:5001",
    }
    subprocess.run(
        ["bash", str(ROOT / "scripts/run/push_images.sh"), "platform-feast:0.66.0"],
        env=env,
        check=True,
    )
    assert log.read_text().splitlines() == [
        "image inspect platform-feast:0.66.0",
        "tag platform-feast:0.66.0 localhost:5001/platform-feast:abc123",
        "push localhost:5001/platform-feast:abc123",
    ]
    log.unlink()
    env["TAG"] = "latest"
    result = subprocess.run(
        ["bash", str(ROOT / "scripts/run/push_images.sh"), "platform-feast:0.66.0"],
        env=env,
        capture_output=True,
    )
    assert result.returncode != 0
    assert not log.exists()


def test_compose_release_matches_published_tag():
    """Services sharing an image get the same pushed address, taskmanager included."""
    result = subprocess.run(
        [
            "bash",
            "scripts/run/compose_release_images.sh",
            "flink-jobmanager=platform-flink",
            "flink-taskmanager=platform-flink",
        ],
        cwd=ROOT,
        env={**os.environ, "REGISTRY": "localhost:5001", "TAG": "abc123"},
        text=True,
        capture_output=True,
        check=True,
    )
    services = yaml.safe_load(result.stdout)["services"]
    assert {v["image"] for v in services.values()} == {"localhost:5001/platform-flink:abc123"}


def test_eval_gate_propagates_failure_and_archives_summary_contract():
    """pipefail keeps the harness exit 1 through tee; the CI threshold is stated explicitly."""
    text = read("scripts/run/eval_agent_ci.sh")
    assert "set -euo pipefail" in text
    assert "--min-accuracy 0.95" in text
    assert 'tee "out/agent-eval-$tag-summary.json"' in text
    assert "flushall" not in text


def test_registry_setup_repeated_on_existing_nodes(tmp_path):
    """Two setup passes write the same hosts/ConfigMap on both nodes without recreating the cluster."""
    import json
    import sys

    fake = tmp_path / "fake.py"
    fake.write_text(
        f"#!{sys.executable}\n"
        "import json, os, pathlib, sys\n"
        "tool = pathlib.Path(sys.argv[0]).name\n"
        "args = sys.argv[1:]\n"
        'body = ""\n'
        'if tool == "kind":\n'
        '    assert args == ["get", "nodes", "--name", "platform"]\n'
        '    print("platform-control-plane\\nplatform-worker")\n'
        'elif "containerd" in args:\n'
        '    print("config_path = \\"/etc/containerd/certs.d\\"")\n'
        'elif tool == "kubectl" or "cp" in args:\n'
        "    body = sys.stdin.read()\n"
        'with open(os.environ["CALL_LOG"], "a") as f:\n'
        '    f.write(json.dumps([tool, args, body]) + "\\n")\n'
    )
    fake.chmod(0o755)
    for tool in ("docker", "kind", "kubectl"):
        (tmp_path / tool).symlink_to(fake)
    log = tmp_path / "calls.jsonl"
    env = {
        **os.environ,
        "PATH": f"{tmp_path}:{os.environ['PATH']}",
        "CALL_LOG": str(log),
        "KIND_CLUSTER": "platform",
    }
    for _ in range(2):
        subprocess.run(
            ["bash", str(ROOT / "scripts/setup/setup_kind_registry.sh")],
            env=env,
            check=True,
            capture_output=True,
        )
    calls = [json.loads(line) for line in log.read_text().splitlines()]
    hosts = [call for call in calls if call[0] == "docker" and "cp" in call[1]]
    assert len(hosts) == 4
    assert len({call[2] for call in hosts}) == 1
    assert {call[1][2] for call in hosts} == {"platform-control-plane", "platform-worker"}
    assert 'server = "http://registry:5000"' in hosts[0][2]
    configs = [yaml.safe_load(call[2]) for call in calls if call[0] == "kubectl"]
    assert configs[0] == configs[1]
    assert configs[0]["metadata"] == {"name": "local-registry-hosting", "namespace": "kube-public"}


def test_push_logs_into_ecr_only_for_ecr_registries(tmp_path):
    """ECR registry ⇒ aws ecr get-login-password for the right region, then docker login; local registry ⇒ no aws calls."""
    for tool in ("docker", "aws"):
        fake = tmp_path / tool
        fake.write_text(
            f'#!/bin/bash\nprintf "{tool} %s\\n" "$*" >> "$TOOL_LOG"\ncat >/dev/null || true\n'
        )
        fake.chmod(0o755)
    log = tmp_path / "tools.log"
    env = {
        **os.environ,
        "PATH": f"{tmp_path}:{os.environ['PATH']}",
        "TOOL_LOG": str(log),
        "TAG": "abc123",
        "REGISTRY": "665116832975.dkr.ecr.ap-southeast-2.amazonaws.com/platform",
    }
    subprocess.run(
        ["bash", str(ROOT / "scripts/run/push_images.sh"), "platform-feast:0.66.0"],
        env=env,
        check=True,
        stdin=subprocess.DEVNULL,
    )
    calls = log.read_text().splitlines()
    # `aws … | docker login` run concurrently in a pipe ⇒ assume no order between the first two lines.
    assert set(calls[:2]) == {
        "aws ecr get-login-password --region ap-southeast-2",
        "docker login --username AWS --password-stdin 665116832975.dkr.ecr.ap-southeast-2.amazonaws.com",
    }
    assert (
        calls[-1]
        == "docker push 665116832975.dkr.ecr.ap-southeast-2.amazonaws.com/platform/platform-feast:abc123"
    )
    log.unlink()
    env["REGISTRY"] = "localhost:5001"
    subprocess.run(
        ["bash", str(ROOT / "scripts/run/push_images.sh"), "platform-feast:0.66.0"],
        env=env,
        check=True,
        stdin=subprocess.DEVNULL,
    )
    assert not any(line.startswith("aws ") for line in log.read_text().splitlines())


def test_jenkins_on_ec2_is_loopback_with_eks_kubeconfig_and_bundle_source():
    """Jenkins AWS: loopback only, jobs fetch a git bundle, EKS kubeconfig on s3, kubectl matches EKS 1.37, aws CLI present."""
    import yaml as _yaml

    from tests.unit.infra.aws.test_aws_provision import ComposeLoader

    jenkins = _yaml.load((ROOT / "docker-compose.aws.yml").read_text(), Loader=ComposeLoader)[
        "services"
    ]["jenkins"]
    assert "--httpListenAddress=127.0.0.1" in jenkins["environment"]["JENKINS_OPTS"]
    assert jenkins["environment"]["CI_GIT_REMOTE"] == "/repo/out/platform.bundle"
    assert (
        "url('${CI_GIT_REMOTE:-file:///repo}')" in (ROOT / "infra/jenkins/jenkins.yaml").read_text()
    )
    casc = (ROOT / "infra/jenkins/jenkins.yaml").read_text()
    assert "scriptPath('ci/jenkins/' + name + '.Jenkinsfile')" in casc and "${name}" not in casc
    assert (
        "-Dhudson.plugins.git.GitSCM.ALLOW_LOCAL_CHECKOUT=true"
        in (ROOT / "docker/Dockerfile.jenkins").read_text()
    )
    setup = (ROOT / "scripts/setup/setup_jenkins.sh").read_text()
    eks = setup[setup.index('if [[ "${PLATFORM_STORAGE:-minio}" == s3 ]]; then') :].split("else")[0]
    assert "eks update-kubeconfig" in eks and "kind get kubeconfig" not in eks
    dockerfile = (ROOT / "docker/Dockerfile.jenkins").read_text()
    assert "/release/v1.37.2/" in dockerfile and "awscli-exe-linux-x86_64-2.31.0.zip" in dockerfile


def _compose_args(tmp_path, env):
    """Run ci_compose.sh with fake docker and return the arguments it passes to docker."""
    fake = tmp_path / "docker"
    fake.write_text('#!/bin/bash\nprintf "%s\\n" "$*" > "$DOCKER_LOG"\n')
    fake.chmod(0o755)
    log = tmp_path / "docker.log"
    subprocess.run(
        ["bash", str(ROOT / "scripts/run/ci_compose.sh"), "ps"],
        cwd=tmp_path,
        check=True,
        env={
            **os.environ,
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "DOCKER_LOG": str(log),
            **env,
        },
    )
    return log.read_text().split()


def test_ci_compose_adds_aws_overlays_only_on_s3(tmp_path):
    """Jenkins on EC2 must carry the AWS overlay (otherwise it recreates Airflow/Flink with the local config)."""
    assert _compose_args(tmp_path, {}) == ["compose", "-f", "docker-compose.yml", "ps"]
    assert _compose_args(tmp_path, {"DATA_STORAGE": "s3"}) == [
        "compose",
        "-f",
        "docker-compose.yml",
        "-f",
        "docker-compose.aws.yml",
        "ps",
    ]
    (tmp_path / "out").mkdir()
    (tmp_path / "out/ci-compose-images.yaml").write_text("services: {}\n")
    assert _compose_args(tmp_path, {"DATA_STORAGE": "s3", "CI_STREAM": "1"}) == [
        "compose",
        "-f",
        "docker-compose.yml",
        "-f",
        "docker-compose.aws.yml",
        "-f",
        "docker-compose.aws-stream.yml",
        "-f",
        "out/ci-compose-images.yaml",
        "ps",
    ]


def test_compose_pipelines_use_machine_env_and_pipeline_a_flink_on_aws():
    """s3 branch: EC2's real .env, no MinIO keys, no setup_airflow, 2 PyFlink jobs; no more bare compose."""
    rag = (ROOT / "ci/jenkins/rag-pipeline.Jenkinsfile").read_text()
    stream = (ROOT / "ci/jenkins/stream-features.Jenkinsfile").read_text()
    for text in (rag, stream):
        assert "choice(name: 'DATA_STORAGE', choices: ['minio', 's3']" in text
        assert "install -m 600 /repo/.env .env" in text
        assert "-f docker-compose.yml -f out/ci-compose-images.yaml" not in text
        assert "always { sh 'rm -f .env' }" in text
    assert 'if [ "$DATA_STORAGE" != s3 ]; then bash scripts/setup/setup_airflow.sh' in rag
    # AWS: two PyFlink jobs on the Flink ML cluster, verify exactly 2 jobs RUNNING.
    assert 'COMPOSE="$COMPOSE" scripts/run/run_stream_feature_jobs.sh' in stream
    assert 'want="ml-sku-return-24h-online ml-sku-return-24h-offline"' in stream
    # Only the local branch fetches MinIO keys from Vault; AWS goes through the IAM role.
    local_branch = stream.split("else\n", 1)[1].split("fi", 1)[0]
    assert 'set +x; eval "$(scripts/setup/vault_env.sh minio)"' in local_branch


def test_agent_pipelines_pass_eval_skus_for_pipeline_a():
    """AWS SKUs (APP-…) differ from local ⇒ both agent pipelines get EVAL_SKUS; the script uses the local set when empty."""
    for name in ("feature-api-agents", "coordinator"):
        text = (ROOT / f"ci/jenkins/{name}.Jenkinsfile").read_text()
        assert "string(name: 'EVAL_SKUS'" in text and 'EVAL_SKUS = "${params.EVAL_SKUS}"' in text
    assert '--skus "${EVAL_SKUS:-sku-00001,' in (ROOT / "scripts/run/eval_agent_ci.sh").read_text()
