"""Check the image release contract and CI gates, fully offline."""

import importlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]


def read(path):
    """Read a config file from the repo."""
    return (ROOT / path).read_text()


def run_module(module, *args, env=None, **kwargs):
    """Run `python -m scripts.<module>` from the repo root, like Jenkins and the docs do."""
    return subprocess.run(
        [sys.executable, "-m", f"scripts.{module}", *args], cwd=ROOT, env=env, **kwargs
    )


def test_no_direct_kind_image_loading():
    """Scripts/CI must release images, not load them directly into nodes."""
    for folder in ("scripts", "infra/jenkins"):
        for path in (ROOT / folder).rglob("*"):
            if path.suffix not in (".sh", ".py", ".Jenkinsfile"):
                continue
            for line in path.read_text().splitlines():
                if not line.lstrip().startswith(("#", "//")):
                    assert not re.search(r"\bkind[\"',\s]+load\b", line), path


def test_build_pipelines_publish_before_deploy():
    """Every build pipeline must Push before the first deploy."""
    for path in (ROOT / "infra/jenkins").glob("*.Jenkinsfile"):
        text = path.read_text()
        stages = re.findall(r"stage\('([^']+)'\)", text)
        if "Build" in stages:
            deploy = next(i for i, name in enumerate(stages) if name.startswith("Deploy"))
            assert stages.index("Build") < stages.index("Push") < deploy, path
            assert "python3 -m scripts.shared.push_images" in text
    for name, deploy in (("feature-api-agents", "Deploy Agent"), ("coordinator", "Deploy")):
        text = read(f"infra/jenkins/{name}.Jenkinsfile")
        stages = re.findall(r"stage\('([^']+)'\)", text)
        assert stages.index("Eval agent") > stages.index(deploy)
        assert "python3 -m scripts.llm.eval_agent_ci" in text
        assert "archiveArtifacts" in text and "out/agent-eval*.json*" in text


def test_registry_and_pull_configuration():
    """The registry binds loopback only, has a volume, and nodes pull via the canonical address."""
    compose = yaml.safe_load(read("docker-compose.yml"))
    registry = compose["services"]["registry"]
    assert registry["image"] == "registry:2.8.3"
    assert registry["ports"] == ["127.0.0.1:5001:5000"]
    assert "registry-data:/var/lib/registry" in registry["volumes"]
    assert compose["networks"]["kind"]["external"] is True
    values = yaml.safe_load(read("infra/helm/serving-api/values.yaml"))
    assert values["image"]["repository"] == "localhost:5001/platform-serving-api"
    assert values["image"]["pullPolicy"] == "IfNotPresent"
    assert "localhost:5001" in read("ml/training_pipeline.py")
    assert 'set_image_pull_policy(task, "IfNotPresent")' in read("ml/training_pipeline.py")
    assert "imagePullPolicy: IfNotPresent" in read("infra/k8s/ml/return-risk.yaml")
    setup = read("scripts/shared/setup_kind_registry.py")
    assert "/etc/containerd/certs.d/localhost:5001/hosts.toml" in setup
    assert 'server = "http://registry:5000"' in setup
    assert "namespace: kube-public" in setup
    assert "kind create" not in setup and "kind delete" not in setup


@pytest.mark.parametrize(
    "script",
    # The bash scripts that remain (eval'd helpers, one-liners, historical evidence).
    sorted(str(path.relative_to(ROOT)) for path in (ROOT / "scripts").rglob("*.sh")),
)
def test_shell_syntax(script):
    """Parse bash without running infrastructure or network."""
    subprocess.run(["bash", "-n", str(ROOT / script)], check=True)


@pytest.mark.parametrize(
    "module",
    sorted(
        # Scripts converted from bash all build on scripts/common.py; older scripts import heavy deps
        # (airflow, psycopg, …) and are covered by their own tests.
        f"scripts.{path.parent.name}.{path.stem}"
        for path in (ROOT / "scripts").glob("*/*.py")
        if "from scripts.common import" in path.read_text()
    ),
)
def test_script_modules_import(module):
    """Importing a script must not run it: all work happens in main()."""
    assert callable(importlib.import_module(module).main)


def test_letsencrypt_disabled_without_domain():
    """The default branch still runs with no infrastructure tools in PATH."""
    env = {**os.environ, "DUCKDNS_DOMAIN": "", "PATH": "/nonexistent"}
    result = run_module("shared.setup_letsencrypt", env=env, text=True, capture_output=True)
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
    run_module(
        "shared.push_images",
        "platform-feast:0.66.0",
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
    result = run_module(
        "shared.push_images",
        "platform-feast:0.66.0",
        env=env,
        capture_output=True,
    )
    assert result.returncode != 0
    assert not log.exists()


def test_compose_release_matches_published_tag():
    """Services sharing an image get the same pushed address, taskmanager included."""
    result = run_module(
        "shared.compose_release_images",
        "flink-jobmanager=platform-flink",
        "flink-taskmanager=platform-flink",
        env={**os.environ, "REGISTRY": "localhost:5001", "TAG": "abc123"},
        text=True,
        capture_output=True,
        check=True,
    )
    services = yaml.safe_load(result.stdout)["services"]
    assert {v["image"] for v in services.values()} == {"localhost:5001/platform-flink:abc123"}


def test_eval_gate_propagates_failure_and_archives_summary_contract():
    """pipefail keeps the harness exit 1 through tee; the CI threshold is stated explicitly."""
    from scripts.llm import eval_agent_ci

    text = read("scripts/llm/eval_agent_ci.py")
    flat = " ".join(text.split())
    assert eval_agent_ci.MIN_ACCURACY == "0.95" and '"--min-accuracy", MIN_ACCURACY' in flat
    # tee() exits non-zero when ml.agent_eval fails; the summary keeps stdout only (valid JSON).
    assert 'out/agent-eval-{tag}-summary.json"' in text and "merge_stderr=False" in text
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
        run_module(
            "shared.setup_kind_registry",
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
    run_module(
        "shared.push_images",
        "platform-feast:0.66.0",
        env=env,
        check=True,
        stdin=subprocess.DEVNULL,
    )
    calls = log.read_text().splitlines()
    assert calls[:2] == [
        "aws ecr get-login-password --region ap-southeast-2",
        "docker login --username AWS --password-stdin 665116832975.dkr.ecr.ap-southeast-2.amazonaws.com",
    ]
    assert (
        calls[-1]
        == "docker push 665116832975.dkr.ecr.ap-southeast-2.amazonaws.com/platform/platform-feast:abc123"
    )
    log.unlink()
    env["REGISTRY"] = "localhost:5001"
    run_module(
        "shared.push_images",
        "platform-feast:0.66.0",
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
    assert "scriptPath('infra/jenkins/' + name + '.Jenkinsfile')" in casc and "${name}" not in casc
    assert (
        "-Dhudson.plugins.git.GitSCM.ALLOW_LOCAL_CHECKOUT=true"
        in (ROOT / "infra/docker/Dockerfile.jenkins").read_text()
    )
    import inspect

    from scripts.shared import setup_jenkins

    eks = inspect.getsource(setup_jenkins.kubeconfig_eks)
    assert '"update-kubeconfig"' in eks and '"kind"' not in eks
    dockerfile = (ROOT / "infra/docker/Dockerfile.jenkins").read_text()
    assert "/release/v1.37.2/" in dockerfile and "awscli-exe-linux-x86_64-2.31.0.zip" in dockerfile


def _compose_args(tmp_path, env):
    """Run ci_compose.sh with fake docker and return the arguments it passes to docker."""
    fake = tmp_path / "docker"
    fake.write_text('#!/bin/bash\nprintf "%s\\n" "$*" > "$DOCKER_LOG"\n')
    fake.chmod(0o755)
    log = tmp_path / "docker.log"
    subprocess.run(
        ["bash", str(ROOT / "scripts/shared/ci_compose.sh"), "ps"],
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
    rag = (ROOT / "infra/jenkins/rag-pipeline.Jenkinsfile").read_text()
    stream = (ROOT / "infra/jenkins/stream-features.Jenkinsfile").read_text()
    for text in (rag, stream):
        assert "choice(name: 'DATA_STORAGE', choices: ['minio', 's3']" in text
        assert "install -m 600 /repo/.env .env" in text
        assert "-f docker-compose.yml -f out/ci-compose-images.yaml" not in text
        assert "cleanup { sh 'rm -f .env' }" in text  # always removed, but after the rollback
    assert 'if [ "$DATA_STORAGE" != s3 ]; then python3 -m scripts.data.setup_airflow' in rag
    # AWS: two PyFlink jobs on the Flink ML cluster, verify exactly 2 jobs RUNNING.
    assert 'COMPOSE="$COMPOSE" python3 -m scripts.ml.run_stream_feature_jobs' in stream
    assert 'want="ml-sku-return-24h-online ml-sku-return-24h-offline"' in stream
    # Only the local branch fetches MinIO keys from Vault; AWS goes through the IAM role.
    local_branch = stream.split("else\n", 1)[1].split("fi", 1)[0]
    assert 'set +x; eval "$(scripts/shared/vault_env.sh minio)"' in local_branch


def test_agent_pipelines_pass_eval_skus_for_pipeline_a():
    """AWS SKUs (APP-…) differ from local ⇒ both agent pipelines get EVAL_SKUS; the script uses the local set when empty."""
    for name in ("feature-api-agents", "coordinator"):
        text = (ROOT / f"infra/jenkins/{name}.Jenkinsfile").read_text()
        assert "string(name: 'EVAL_SKUS'" in text and 'EVAL_SKUS = "${params.EVAL_SKUS}"' in text
    from scripts.llm.eval_agent_ci import DEFAULT_SKUS

    assert DEFAULT_SKUS.startswith("sku-00001,") and DEFAULT_SKUS.endswith(",sku-00012")


def test_only_the_approval_pipeline_sets_production():
    """Training only nominates a challenger; production moves after a human input, before the deploy,
    and the wait holds no executor (jenkins.yaml has one)."""
    pipeline = read("ml/training_pipeline.py")
    assert '"challenger", version' in pipeline and '"production", version' not in pipeline
    text = read("infra/jenkins/return-risk.Jenkinsfile")
    stages = re.findall(r"stage\('([^']+)'\)", text)
    assert stages == ["Show challenger", "Approve", "Promote", "Deploy model"]
    approve = text.split("stage('Approve')")[1].split("stage('Promote')")[0]
    assert "input(" in approve and "agent any" not in approve
    assert re.search(r"pipeline \{\s*(//[^\n]*\n\s*)*agent none", text)
    assert '--version "$CHALLENGER"' in text and "disableConcurrentBuilds()" in text
    assert "'return-risk'" in read("infra/jenkins/jenkins.yaml")


def test_monitor_trains_only_on_drift_and_schedules_do_not_pile_up():
    """Diagram M5: drift daily → train on drift; training weekly. Training steps sit only under the drift
    condition; schedules never overlap or replay missed days; the drift image keeps pandas 2 for Evidently."""
    pipeline = read("ml/training_pipeline.py")
    monitor = pipeline.split("def return_risk_monitor(")[1]
    condition = 'with dsl.If(drift.outputs["Output"] == "train"'
    assert condition in monitor
    assert monitor.index(condition) < monitor.index("train_and_register(")
    assert "check_drift(" in monitor and "@dsl.component(base_image=DRIFT_IMAGE)" in pipeline
    submit = read("scripts/ml/run_training_pipeline.py")
    assert '"return-risk-monitor-daily", return_risk_monitor, "0 0 2 * * *"' in submit
    assert '"return-risk-training-weekly", return_risk_training, "0 0 3 * * 0"' in submit
    assert "max_concurrency=1" in submit and "no_catchup=True" in submit
    assert "delete_recurring_run" in submit
    assert '"pandas==2.3.3"' in read("infra/docker/Dockerfile.drift")


@pytest.mark.parametrize(
    ("name", "checks"),
    [("rag-pipeline", ["DAG import", "Run DAG"]), ("stream-features", ["Verify"])],
)
def test_compose_deploys_roll_back_to_the_last_good_release(name, checks):
    """Diagram "Deploy to EC2 · rolls back if a check fails": only a release that passed every check becomes
    last-good; a failure after Deploy started redeploys it with the SAME deploy + check scripts; .env survives
    until the rollback is done (post `always` would run before `failure`)."""
    text = read(f"infra/jenkins/{name}.Jenkinsfile")
    stages = re.findall(r"stage\('([^']+)'\)", text)
    assert stages[-1] == "Keep as last good"
    assert stages.index("Deploy") < min(stages.index(c) for c in checks)
    deploy = text.split("stage('Deploy')")[1].split("stage(")[0]
    assert deploy.index("env.DEPLOY_STARTED = 'true'") < deploy.index("sh DEPLOY_SH")
    check = "CHECK_SH" if name == "rag-pipeline" else "VERIFY_SH"
    failure = text.split("failure {")[1].split("cleanup {")[0]
    assert "env.DEPLOY_STARTED != 'true'" in failure
    assert "fileExists('out/ci-compose-images.good.yaml')" in failure
    assert "cp out/ci-compose-images.good.yaml out/ci-compose-images.yaml" in failure
    assert failure.index("sh DEPLOY_SH") < failure.index(f"sh {check}")
    assert "cleanup { sh 'rm -f .env' }" in text and "always { sh 'rm -f .env' }" not in text


def run_verify(tmp_path, storage, jobs, pusher="running", flink_up=True):
    """Run the stream VERIFY_SH exactly as written in the Jenkinsfile, with fake curl/sleep/compose on PATH."""
    script = re.search(
        r"VERIFY_SH = '''(.*?)'''", read("infra/jenkins/stream-features.Jenkinsfile"), re.S
    ).group(1)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    overview = json.dumps({"jobs": [{"name": n, "state": "RUNNING"} for n in jobs]})
    fakes = {
        "curl": f"echo '{overview}'" if flink_up else "exit 7",
        "sleep": "exit 0",
        "compose": f"echo {pusher}",
    }
    for tool, body in fakes.items():
        (bin_dir / tool).write_text(f"#!/bin/sh\n{body}\n")
        (bin_dir / tool).chmod(0o755)
    env = {
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "DATA_STORAGE": storage,
        "COMPOSE": "compose",
    }
    return subprocess.run(["bash", "-c", script], env=env, capture_output=True, text=True)


@pytest.mark.parametrize(
    ("storage", "jobs", "pusher", "flink_up", "ok"),
    [
        ("s3", ["ml-sku-return-24h-online", "ml-sku-return-24h-offline"], "running", True, True),
        (
            "s3",
            ["ml-sku-return-24h-online"] * 2 + ["ml-sku-return-24h-offline"],
            "running",
            True,
            False,
        ),
        ("s3", ["ml-sku-return-24h-online"], "running", True, False),
        ("s3", [], "running", False, False),  # Flink UI down
        ("minio", ["ml-sku-return-24h"], "running", True, True),
        (
            "minio",
            ["ml-sku-return-24h"],
            "exited",
            True,
            False,
        ),  # jobs fine, pusher down: still red
    ],
)
def test_stream_verify_script(tmp_path, storage, jobs, pusher, flink_up, ok):
    """Each expected job RUNNING exactly once, and locally the pusher too; the job loop must not skip the pusher."""
    result = run_verify(tmp_path, storage, jobs, pusher, flink_up)
    assert (result.returncode == 0) is ok, result.stdout + result.stderr
