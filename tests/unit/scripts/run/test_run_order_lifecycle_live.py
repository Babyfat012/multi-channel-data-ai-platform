"""Check the runbook and v2 routes fully offline, without Docker/Chrome."""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from urllib.parse import unquote

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[4]
RUNBOOK = ROOT / "scripts/run/run_order_lifecycle_live.sh"


def test_bash_syntax():
    """Check the runbook's Bash syntax."""
    subprocess.run(["bash", "-n", str(RUNBOOK)], check=True)


@pytest.mark.parametrize(
    "args,code,token",
    [
        (["--help"], 0, "--phase"),
        (["--phase"], 2, "--phase"),
        (["--phase", "wrong"], 2, "Invalid phase"),
        (["--unknown"], 2, "Invalid flag"),
    ],
)
def test_cli_flags_without_docker(args, code, token):
    """Check help and CLI argument errors without starting Docker."""
    result = subprocess.run(["bash", str(RUNBOOK), *args], capture_output=True, text=True)
    assert result.returncode == code
    assert token in result.stdout + result.stderr


def test_compose_mounts_v2_only_expected_services():
    """Check the v2 source mount appears only on the expected services."""
    services = yaml.safe_load((ROOT / "docker-compose.yml").read_text())["services"]
    fixture = "${PLATFORM_ORDER_LIFECYCLE_DIR:-./out/order_lifecycle_live}:/data/order_lifecycle:ro"
    mounted = {name for name, service in services.items() if fixture in service.get("volumes", [])}
    assert mounted == {"airflow", "spark-master", "spark-worker-1", "spark-worker-2"}
    assert "./out/receipts:/opt/airflow/receipts" in services["airflow"]["volumes"]


def test_capture_routes_without_browser():
    """Check DataHub URLs and screenshot paths without opening a browser."""
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/evidence/capture_order_lifecycle_datahub_evidence.py"),
            "--print-urls",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    targets = json.loads(result.stdout)
    expected = {
        "lakehouse.gold.lifecycle_fact_order_lines",
        "lakehouse.silver.lifecycle_stg_line_lifecycle",
    }
    assert len(targets) == 4
    for name in expected:
        urn = f"urn:li:dataset:(urn:li:dataPlatform:iceberg,{name},PROD)"
        routes = [t for t in targets if t["urn"] == urn]
        assert {t["route"] for t in routes} == {"Lineage", "Quality"}
        for target in routes:
            assert (
                unquote(target["url"]) == f"http://127.0.0.1:9002/dataset/{urn}/{target['route']}"
            )
            assert "/docs/images/datahub/v2/dp-order-lifecycle" in target["path"]
    assert {Path(t["path"]).name for t in targets} == {
        "dp-order-lifecycle-gold-lineage.png",
        "dp-order-lifecycle-gold-validation.png",
        "dp-order-lifecycle-silver-lineage.png",
        "dp-order-lifecycle-silver-validation.png",
    }


def test_quality_accepts_multiple_checks_and_rejects_failure():
    """Check multiple assertions passing together and detection of failing assertions."""
    spec = importlib.util.spec_from_file_location(
        "capture_v2", ROOT / "scripts/evidence/capture_order_lifecycle_datahub_evidence.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    class Page:
        """Fake Playwright page returning fixed content."""

        def __init__(self, body):
            """Store the page content to return."""
            self.body = body

        async def evaluate(self, expression):
            """Ignore the expression, return the stored content."""
            return self.body

    import asyncio

    assert asyncio.run(module.wait_for_quality(Page("Quality 19 Passing 0 Failing")))
    with pytest.raises(RuntimeError, match="failing assertions"):
        asyncio.run(module.wait_for_quality(Page("Quality 19 Passing 1 Failing")))


@pytest.fixture
def fake_runtime(tmp_path):
    """Fake Docker records argv, returns state/receipts; no connections or Spark runs."""
    executable = tmp_path / "docker"
    executable.write_text("""#!/usr/bin/env python3
import json, os, pathlib, sys
args = sys.argv[1:]
with open(os.environ["FAKE_DOCKER_LOG"], "a") as log:
    log.write(json.dumps(args) + "\\n")
if args[0] == "ps":
    pass
elif "python" in args and "-" in args:
    sys.stdin.read()
    print(json.dumps({"state": os.environ.get("FAKE_RUN_STATE", "success"), "directory": "20261019T010203"}))
elif "cp" in args:
    dest = pathlib.Path(args[-1])
    dest.mkdir(parents=True, exist_ok=True)
    for task in ("check_contracts", "ingest_bronze", "build_silver", "build_gold", "validate_gold"):
        (dest / (task + ".json")).write_text(json.dumps({"run_id": os.environ["PLATFORM_V2_RUN_ID"], "status": "passed"}))
""")
    executable.chmod(0o755)
    log = tmp_path / "docker.jsonl"
    source = tmp_path / "fixture"
    source.mkdir()
    (source / "manifest.json").write_text("{}")
    run_id = "test_live_v2_" + tmp_path.name
    env = {
        **os.environ,
        "PATH": f"{tmp_path}:{os.environ['PATH']}",
        "FAKE_DOCKER_LOG": str(log),
        "PLATFORM_ORDER_LIFECYCLE_DIR": str(source),
        "PLATFORM_V2_RUN_ID": run_id,
        "PLATFORM_PYTHON": sys.executable,
    }
    project = tmp_path / "project"
    (project / "scripts").mkdir(parents=True)
    (project / "configs").mkdir()
    for script in (
        "run/run_order_lifecycle_live.sh",
        "run/run_aws_measurements.sh",
        "setup/setup_airflow.sh",
        "setup/setup_dp1_file_channels.sh",
        "setup/setup_dp1_cdc.sh",
        "setup/setup_dp2_silver.sh",
        "setup/setup_dp2_file_channels.sh",
        "setup/setup_dp3_features.sh",
    ):
        (project / "scripts" / script).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(ROOT / "scripts" / script, project / "scripts" / script)
    for config in ("iceberg_maintenance.json", "iceberg_maintenance_aws.json"):
        shutil.copy(ROOT / "configs" / config, project / "configs")
    env["FAKE_PROJECT"] = str(project)
    yield env, log, run_id


@pytest.mark.parametrize("phase", ["source", "airflow", "datahub", "capture", "all"])
def test_all_phase_flags_parse(fake_runtime, phase):
    # source needs no Docker; the other phases are checked in depth separately.
    """Check every phase name is recognized by the CLI."""
    env, _, _ = fake_runtime
    if phase != "source":
        env["PLATFORM_V2_TIMEOUT"] = "invalid"
    result = subprocess.run(
        [
            "bash",
            str(Path(env["FAKE_PROJECT"]) / "scripts/run/run_order_lifecycle_live.sh"),
            "--phase",
            phase,
        ],
        env=env,
        capture_output=True,
        text=True,
    )
    assert "Invalid phase" not in result.stderr
    assert result.returncode == (0 if phase == "source" else 2)


@pytest.mark.parametrize("state", ["success", "failed"])
def test_airflow_copies_run_receipts_before_stop(fake_runtime, state):
    """Check Airflow copies the run's receipts before stopping services."""
    env, log, run_id = fake_runtime
    env["FAKE_RUN_STATE"] = state
    result = subprocess.run(
        [
            "bash",
            str(Path(env["FAKE_PROJECT"]) / "scripts/run/run_order_lifecycle_live.sh"),
            "--phase",
            "airflow",
        ],
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == (0 if state == "success" else 1), result.stderr
    calls = [json.loads(line) for line in log.read_text().splitlines()]
    trigger = next(c for c in calls if "trigger" in c)
    assert trigger[trigger.index("--run-id") + 1] == run_id
    assert json.loads(trigger[trigger.index("--conf") + 1]) == {
        "source_root": "/data/order_lifecycle",
        "cutoff": "2026-06-01T00:00:00.000000Z",
    }
    up = next(c for c in calls if "up" in c)
    assert "generator-batch" not in up
    copy = next(c for c in calls if "cp" in c)
    assert copy[-2] == "airflow:/opt/airflow/receipts/order_lifecycle/20261019T010203/."
    assert copy[-1] == f"out/receipts-v2-run/{run_id}/"
    assert "stop" in calls[-1]
    assert (Path(env["FAKE_PROJECT"]) / copy[-1] / "build_gold.json").is_file()
    if state == "failed":
        assert "not publishing" in result.stderr


def test_source_refuses_partial_directory(fake_runtime):
    """Check a half-written source directory is refused to avoid overwriting data."""
    env, _, _ = fake_runtime
    source = Path(env["PLATFORM_ORDER_LIFECYCLE_DIR"])
    (source / "manifest.json").unlink()
    (source / "partial.jsonl").write_text("keep")
    result = subprocess.run(
        ["bash", str(RUNBOOK), "--phase", "source"], env=env, capture_output=True, text=True
    )
    assert result.returncode == 1
    assert (source / "partial.jsonl").read_text() == "keep"


@pytest.mark.parametrize("invalid", [False, True])
@pytest.mark.parametrize("storage", ["minio", "s3"])
def test_datahub_phase_stops_spark_then_publishes_same_arguments(fake_runtime, invalid, storage):
    """Check Spark is stopped before publishing to DataHub with the right run arguments."""
    env, log, run_id = fake_runtime
    project = Path(env["FAKE_PROJECT"])
    receipts = project / "out/receipts-v2-run" / run_id
    receipts.mkdir(parents=True)
    for task in ("check_contracts", "ingest_bronze", "build_silver", "build_gold", "validate_gold"):
        (receipts / f"{task}.json").write_text(
            json.dumps({"run_id": "wrong" if invalid else run_id, "checks": {"missing": 0}})
        )
    publisher = project.parent / "publisher"
    publisher.write_text("""#!/usr/bin/env python3
import json, os, sys
with open(os.environ["FAKE_DOCKER_LOG"], "a") as log:
    log.write(json.dumps(sys.argv[1:]) + "\\n")
print("[]")
""")
    publisher.chmod(0o755)
    curl = project.parent / "curl"
    curl.write_text("#!/bin/sh\nexit 0\n")
    curl.chmod(0o755)
    env["DATAHUB_PYTHON"] = str(publisher)
    env.update(PLATFORM_STORAGE=storage, RAW_BUCKET="test-raw", AWS_REGION="ap-southeast-1")
    result = subprocess.run(
        ["bash", str(project / "scripts/run/run_order_lifecycle_live.sh"), "--phase", "datahub"],
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == (1 if invalid else 0), result.stderr
    if invalid:
        assert "Wrong run_id" in result.stderr
        assert not log.exists()
        return
    calls = [json.loads(line) for line in log.read_text().splitlines()]
    assert "stop" in calls[0] and "spark-master" in calls[0]
    assert "up" in calls[1] and "datahub" in calls[1]
    dry, publish = calls[-2:]
    assert dry[1] == "--dry-run" and publish[1] == "--publish"
    assert dry[2:] == publish[2:]
    assert dry[dry.index("--source-root") + 1] == (
        "s3a://test-raw/order_lifecycle/fixture" if storage == "s3" else "/data/order_lifecycle"
    )
    if storage == "s3":
        assert not any("minio" in arg for call in calls for arg in call)
    assert dry[dry.index("--receipt-dir") + 1] == f"out/receipts-v2-run/{run_id}"
    assert (receipts / "datahub-mcps.json").read_text().strip() == "[]"


@pytest.mark.parametrize("from_dotenv", [False, True])
def test_s3_airflow_uses_remote_source_without_minio(fake_runtime, from_dotenv):
    """Run real Bash with fake Docker/aws: S3 conf, receipts and environment precedence."""
    env, log, run_id = fake_runtime
    project = Path(env["FAKE_PROJECT"])
    aws = project.parent / "aws"
    aws.write_text('#!/bin/sh\nprintf \'%s\\n\' "$*" >> "$FAKE_AWS_LOG"\n')
    aws.chmod(0o755)
    env["FAKE_AWS_LOG"] = str(project.parent / "aws.log")
    settings = dict(
        PLATFORM_STORAGE="s3",
        RAW_BUCKET="test-raw",
        AWS_REGION="ap-southeast-1",
        PLATFORM_DATASET_NAME="scale90",
        PLATFORM_SPARK_PACKAGES_AWS="test:aws:1",
    )
    if from_dotenv:
        for key in settings:
            env.pop(key, None)
        (project / ".env").write_text("\n".join(f"{k}={v}" for k, v in settings.items()) + "\n")
    else:
        env.update(settings)
        (project / ".env").write_text("PLATFORM_STORAGE=minio\nRAW_BUCKET=wrong\n")
    # No host fixture: the AWS branch must check the S3 manifest, not generate locally.
    shutil.rmtree(env["PLATFORM_ORDER_LIFECYCLE_DIR"])
    result = subprocess.run(
        ["bash", str(project / "scripts/run/run_order_lifecycle_live.sh"), "--phase", "airflow"],
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    calls = [json.loads(line) for line in log.read_text().splitlines()]
    assert not any("minio" in arg for call in calls for arg in call)
    trigger = next(c for c in calls if "trigger" in c)
    assert json.loads(trigger[trigger.index("--conf") + 1])["source_root"] == (
        "s3a://test-raw/order_lifecycle/scale90"
    )
    assert (
        "s3://test-raw/order_lifecycle/scale90/manifest.json"
        in Path(env["FAKE_AWS_LOG"]).read_text()
    )
    assert (project / "out/receipts-v2-run" / run_id / "build_gold.json").is_file()


@pytest.mark.parametrize("what", ["skew", "maintenance", "warehouse"])
def test_aws_measurement_invocations_use_cluster_and_collect_evidence(fake_runtime, what):
    """Check wrapper commands on fake Docker: AWS packages, resources, JSON and log copy."""
    env, log, _ = fake_runtime
    env.update(
        PLATFORM_STORAGE="s3",
        RAW_BUCKET="test-raw",
        AWS_REGION="ap-southeast-1",
        PLATFORM_SPARK_PACKAGES_AWS="test:aws:1",
        PLATFORM_DATASET_NAME="scale90",
    )
    project = Path(env["FAKE_PROJECT"])
    result = subprocess.run(
        ["bash", str(project / "scripts/run/run_aws_measurements.sh"), "--what", what],
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    calls = [json.loads(line) for line in log.read_text().splitlines()]
    submits = [c for c in calls if "spark-submit" in c]
    assert submits
    for call in submits:
        assert call[call.index("--master") + 1] == "spark://spark-master:7077"
        assert call[call.index("--packages") + 1] == "test:aws:1"
        assert call[call.index("--executor-memory") + 1] == "2g"
        assert "spark.cores.max=2" in call
        assert "spark.executor.cores=1" in call
    if what == "skew":
        assert "s3a://test-raw/order_lifecycle/scale90" in submits[0]
    if what == "maintenance":
        assert "--execute" in submits[0]
        assert (
            "lakehouse.gold.lifecycle_fact_order_lines"
            in submits[0][submits[0].index("--config-json") + 1]
        )
        assert "--validate-receipt" in submits[1]
    assert "cp" in calls[-1]
    assert calls[-1][-1].startswith("out/aws-evidence/")
