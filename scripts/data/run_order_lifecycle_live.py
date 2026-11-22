"""Host-side planner for the order-lifecycle v2 run: fixture → real DAG → DataHub → UI screenshots.

RAM is split across two phases: phase A (Airflow + Spark) is stopped before phase B (DataHub) starts.

    python3 -m scripts.data.run_order_lifecycle_live [--phase source|airflow|datahub|capture|all]

Environment:
  PLATFORM_ORDER_LIFECYCLE_DIR     host fixture (default ./out/order_lifecycle_live)
  PLATFORM_ORDER_LIFECYCLE_CUTOFF  default 2026-06-01T00:00:00.000000Z
  PLATFORM_V2_RUN_ID               the new run on trigger; the run to publish on datahub (default: latest)
  PLATFORM_V2_TIMEOUT              DAG timeout in seconds (1800); PLATFORM_V2_START_TIMEOUT (600)
  PLATFORM_V2_EXECUTOR_MEMORY      default 2g, fits the default worker for the small fixture
  PLATFORM_STORAGE                 minio (default) or s3; from the environment or .env
  PLATFORM_PYTHON                  Python with the repo deps: .venv/bin/python (S3: /mnt/platform-data/venv/bin/python)
  RAW_BUCKET, AWS_REGION, PLATFORM_DATASET_NAME   S3 source synced by Ansible
  DATAHUB_PYTHON                   Python with the acryl-datahub SDK (default: the uv tool environment)
  DATAHUB_GMS_URL                  default http://127.0.0.1:8089; DATAHUB_GMS_TOKEN optional, env only
  DATAHUB_QUICKSTART_DIR           default ~/.datahub/quickstart
Receipts: out/receipts-v2-run/<run_id>/; latest run id: out/receipts-v2-run/latest-run-id.
Requires Docker Compose, curl, Chrome, aiohttp and the DataHub quickstart/SDK (docs/data-platform/data_governance.md).
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

from scripts.common import ROOT, env, env_defaults, read_env_file, run, succeeds
from scripts.data import setup_airflow

COMPOSE = ["docker", "compose", "--profile", "airflow", "--profile", "spark"]
DAG = "dp_order_lifecycle"
RECEIPT_TASKS = ("check_contracts", "ingest_bronze", "build_silver", "build_gold", "validate_gold")
S3_KEYS = [
    "RAW_BUCKET",
    "PLATFORM_DATASET_NAME",
    "AWS_REGION",
    "PLATFORM_SPARK_PACKAGES_AWS",
    "COMPOSE_FILE",
    "PLATFORM_ORDER_LIFECYCLE_CUTOFF",
]

AIRFLOW_READY = """
import json, urllib.request
with urllib.request.urlopen("http://localhost:8080/health", timeout=5) as r:
    h = json.load(r)
assert h["metadatabase"]["status"] == h["scheduler"]["status"] == "healthy"
"""
SPARK_READY = """
import json, urllib.request
with urllib.request.urlopen("http://localhost:8080/json/", timeout=5) as r:
    assert json.load(r)["aliveworkers"] >= 2
"""
DAG_READY = f"""
from airflow.models.dag import DagModel
assert DagModel.get_current("{DAG}") is not None
"""
RUN_INFO = f"""
import json, sys
from airflow.models.dagrun import DagRun
runs = DagRun.find(dag_id="{DAG}", run_id=sys.argv[1])
if not runs:
    raise SystemExit("Triggered run not found")
r = runs[0]
print(json.dumps({{"state": r.state, "directory": r.execution_date.strftime("%Y%m%dT%H%M%S")}}))
"""


class Config:
    """Settings resolved once from the environment (and .env for the storage choice)."""

    def __init__(self):
        """Environment wins over .env; secrets from .env are never exported."""
        if "PLATFORM_STORAGE" not in os.environ:
            os.environ["PLATFORM_STORAGE"] = read_env_file().get("PLATFORM_STORAGE") or "minio"
        self.storage = os.environ["PLATFORM_STORAGE"] or "minio"
        if self.storage == "s3":
            env_defaults(S3_KEYS)
        self.cutoff = (
            os.environ.get("PLATFORM_ORDER_LIFECYCLE_CUTOFF") or "2026-06-01T00:00:00.000000Z"
        )
        self.timeout = positive_int("PLATFORM_V2_TIMEOUT", 1800)
        self.start_timeout = positive_int("PLATFORM_V2_START_TIMEOUT", 600)
        dataset = os.environ.get("PLATFORM_DATASET_NAME") or "fixture"
        if self.storage == "s3":
            self.source_root = f"s3a://{env('RAW_BUCKET')}/order_lifecycle/{dataset}"
            self.python = os.environ.get("PLATFORM_PYTHON") or "/mnt/platform-data/venv/bin/python"
            os.environ["PLATFORM_ORDER_LIFECYCLE_DIR"] = f"/mnt/platform-data/source/{dataset}"
            # Phase A services to stop afterwards; no MinIO on AWS. Volumes and other projects are untouched.
            self.services = [
                "airflow",
                "spark-worker-1",
                "spark-worker-2",
                "spark-master",
                "lakekeeper",
                "lakekeeper-migrate",
                "lakekeeper-db-init",
                "airflow-db-init",
                "postgres",
            ]
        else:
            self.source_root = "/data/order_lifecycle"
            self.python = os.environ.get("PLATFORM_PYTHON") or ".venv/bin/python"
            os.environ.setdefault("PLATFORM_ORDER_LIFECYCLE_DIR", "./out/order_lifecycle_live")
            self.services = [
                "airflow",
                "spark-worker-1",
                "spark-worker-2",
                "spark-master",
                "lakekeeper",
                "lakekeeper-migrate",
                "lakekeeper-db-init",
                "airflow-db-init",
                "minio-init",
                "minio",
                "postgres",
            ]
        self.fixture = Path(os.environ["PLATFORM_ORDER_LIFECYCLE_DIR"])
        quickstart = os.environ.get("DATAHUB_QUICKSTART_DIR") or str(
            Path.home() / ".datahub/quickstart"
        )
        os.environ.setdefault("DATAHUB_VERSION", "v1.7.0.1")
        os.environ.setdefault("UI_INGESTION_DEFAULT_CLI_VERSION", "1.7.0.14")
        os.environ.update(DATAHUB_MAPPED_GMS_PORT="8089", DATAHUB_MAPPED_FRONTEND_PORT="9002")
        self.hub = [
            "docker",
            "compose",
            "--env-file",
            f"{quickstart}/.local-secrets.env",
            "--profile",
            "quickstart",
            "-f",
            f"{quickstart}/docker-compose.yml",
            "-f",
            "infra/docker/datahub-compose.override.yml",
            "-p",
            "datahub",
        ]
        self.gms = os.environ.get("DATAHUB_GMS_URL") or "http://127.0.0.1:8089"


def positive_int(key: str, default: int) -> int:
    """A timeout in seconds; anything but a positive integer is a usage error (exit 2)."""
    value = os.environ.get(key) or str(default)
    if not re.fullmatch(r"[1-9][0-9]*", value):
        print("Timeout must be a positive integer number of seconds", file=sys.stderr)
        sys.exit(2)
    return int(value)


def wait_until(label: str, cmd: list[str], seconds: int) -> None:
    """Retry a command every 5 s until it succeeds or the start timeout runs out."""
    deadline = time.monotonic() + seconds
    while not succeeds(cmd):
        if time.monotonic() >= deadline:
            sys.exit(f"Timeout waiting for {label}")
        time.sleep(5)


def in_airflow(code: str) -> list[str]:
    """Run a Python snippet in the Airflow container."""
    return [*COMPOSE, "exec", "-T", "airflow", "python", "-c", code]


def datahub_running() -> bool:
    """True when any DataHub Compose container is up (it needs the RAM phase A uses)."""
    return bool(
        run(
            ["docker", "ps", "-q", "--filter", "label=com.docker.compose.project=datahub"],
            capture=True,
        )
    )


def validate_run_id(run_id: str) -> str:
    """The run id becomes a host directory name: no slashes or odd characters."""
    if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.:+-]*", run_id):
        sys.exit("run_id is not valid as a host directory name")
    return run_id


def source_phase(cfg: Config) -> None:
    """Make sure the source exists: the S3 manifest on AWS, else a local fixture (never overwrite a partial one)."""
    if cfg.storage == "s3":
        dataset = os.environ.get("PLATFORM_DATASET_NAME") or "fixture"
        run(
            [
                "aws",
                "s3",
                "ls",
                f"s3://{os.environ['RAW_BUCKET']}/order_lifecycle/{dataset}/manifest.json",
                "--region",
                env("AWS_REGION"),
            ]
        )
        return
    if (cfg.fixture / "manifest.json").is_file():
        print(f"Keeping existing fixture: {cfg.fixture}")
        return
    if cfg.fixture.is_dir() and any(cfg.fixture.iterdir()):
        sys.exit("Source directory is not empty but has no manifest; choose a new directory.")
    run(
        [
            cfg.python,
            "-m",
            "generator.generate_order_lifecycle_fixture",
            "--output",
            str(cfg.fixture),
        ]
    )


def airflow_phase(cfg: Config) -> None:
    """Phase A: start Airflow + Spark, trigger the DAG, wait, copy the receipts, stop phase A."""
    if cfg.storage != "s3" and not (cfg.fixture / "manifest.json").is_file():
        sys.exit("Run --phase source first")
    if datahub_running():
        sys.exit(
            "Stop the DataHub Compose stack before phase A to free enough RAM "
            "(docs/data-platform/data_governance.md)."
        )
    Path("out/receipts-v2-run").mkdir(parents=True, exist_ok=True)
    Path("out/receipts").mkdir(parents=True, exist_ok=True)
    Path("out/receipts").chmod(0o777)  # the Airflow user writes into this fresh bind mount
    workers = ["airflow", "spark-master", "spark-worker-1", "spark-worker-2"]
    wait_flags = ["up", "-d", "--wait", "--wait-timeout", str(cfg.start_timeout)]
    if cfg.storage == "s3":
        source_phase(cfg)
        run([*COMPOSE, *wait_flags, *workers])
    else:
        # minio-init runs once and exits; newer Compose counts that exit as a --wait failure ⇒ start it separately.
        run([*COMPOSE, "up", "-d", "minio-init"])
        run([*COMPOSE, *wait_flags, *workers, "minio"])
    wait_until("Airflow", in_airflow(AIRFLOW_READY), cfg.start_timeout)
    wait_until(
        "Spark",
        [*COMPOSE, "exec", "-T", "spark-master", "python3", "-c", SPARK_READY],
        cfg.start_timeout,
    )
    wait_until("DAG-v2", in_airflow(DAG_READY), cfg.start_timeout)
    setup_airflow.main([])
    airflow = [*COMPOSE, "exec", "-T", "airflow", "airflow"]
    run(
        [
            *airflow,
            "variables",
            "set",
            "platform_order_lifecycle_executor_memory",
            os.environ.get("PLATFORM_V2_EXECUTOR_MEMORY") or "2g",
        ]
    )
    run([*airflow, "dags", "list-import-errors", "--output", "json"])
    # Default run id has no ':' or '/' so the host directory stays easy to use.
    run_id = validate_run_id(
        os.environ.get("PLATFORM_V2_RUN_ID")
        or f"live_v2__{datetime.now(UTC):%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:8]}"
    )
    conf = json.dumps({"source_root": cfg.source_root, "cutoff": cfg.cutoff})
    run([*airflow, "dags", "unpause", DAG])
    run([*airflow, "dags", "trigger", DAG, "--run-id", run_id, "--conf", conf])
    print(f"run_id={run_id}")
    deadline = time.monotonic() + cfg.timeout
    while True:
        out = run(
            [*COMPOSE, "exec", "-T", "airflow", "python", "-", run_id], stdin=RUN_INFO, capture=True
        )
        info = json.loads(out.splitlines()[-1])
        if info["state"] in ("success", "failed"):
            break
        if time.monotonic() >= deadline:
            sys.exit(f"Timeout DAG: {run_id}; see Airflow logs/receipt")
        time.sleep(10)
    receipts = f"out/receipts-v2-run/{run_id}/"
    Path(receipts).mkdir(parents=True, exist_ok=True)
    run(
        [
            *COMPOSE,
            "cp",
            f"airflow:/opt/airflow/receipts/order_lifecycle/{info['directory']}/.",
            receipts,
        ]
    )
    run([*COMPOSE, "stop", *cfg.services])
    if info["state"] != "success":
        sys.exit(f"DAG failed: {run_id}; receipts copied, not publishing")
    Path("out/receipts-v2-run/latest-run-id").write_text(run_id + "\n")
    print(f"DAG success: {run_id}; receipts in {receipts}; phase A stopped.")


def check_receipts(receipts: Path, run_id: str) -> None:
    """Refuse to publish the wrong run or an incomplete set; a missing file is never a PASS."""
    for task in RECEIPT_TASKS:
        doc = json.loads((receipts / f"{task}.json").read_text())
        if doc["run_id"] != run_id:
            sys.exit(f"Wrong run_id: {task}")
        if doc.get("status", "passed") != "passed":
            sys.exit(f"Receipt failed: {task}")
        if any(v != 0 for v in doc.get("checks", {}).values()):
            sys.exit(f"Check failed: {task}")


def datahub_phase(cfg: Config) -> None:
    """Phase B: stop phase A, start DataHub, publish the run's lineage + assertions (dry run saved first)."""
    run_id = validate_run_id(
        os.environ.get("PLATFORM_V2_RUN_ID")
        or Path("out/receipts-v2-run/latest-run-id").read_text().strip()
    )
    receipts = f"out/receipts-v2-run/{run_id}"
    check_receipts(Path(receipts), run_id)
    run([*COMPOSE, "stop", *cfg.services])
    run([*cfg.hub, "up", "-d"])
    wait_until(
        "GMS",
        ["curl", "--fail", "--silent", "--show-error", "--max-time", "5", f"{cfg.gms}/health"],
        cfg.start_timeout,
    )
    publisher = os.environ.get("DATAHUB_PYTHON") or str(
        Path.home() / ".local/share/uv/tools/acryl-datahub/bin/python"
    )
    args = ["--receipt-dir", receipts, "--source-root", cfg.source_root, "--server", cfg.gms]
    script = "scripts/data/publish_order_lifecycle_datahub.py"
    mcps = run([publisher, script, "--dry-run", *args], capture=True)
    Path(receipts, "datahub-mcps.json").write_text(mcps + "\n")
    run([publisher, script, "--publish", *args])


def capture_phase(cfg: Config) -> None:
    """Screenshot the DataHub lineage/quality pages once the UI answers."""
    wait_until(
        "DataHub-UI",
        ["curl", "--fail", "--silent", "--show-error", "--max-time", "5", "http://127.0.0.1:9002"],
        cfg.start_timeout,
    )
    run([cfg.python, "scripts/evidence/capture_order_lifecycle_datahub_evidence.py"])


def cleanup_hint(cfg: Config) -> None:
    """After a failure: how to stop each phase without losing data."""
    minio = "" if cfg.storage == "s3" else " minio"
    print(
        "Clean up phase A: docker compose --profile airflow --profile spark stop airflow spark-worker-1 "
        f"spark-worker-2 spark-master lakekeeper postgres{minio}",
        file=sys.stderr,
    )
    print(
        "Clean up phase B: rerun the DataHub Compose command in docs/data-platform/data_governance.md, replacing "
        "up -d with stop.",
        file=sys.stderr,
    )
    print(
        "Do not use down -v; do not delete volumes. Check the logs, then use a new run_id if Gold needs to be rerun.",
        file=sys.stderr,
    )


def main() -> None:
    """Parse the phase and run it; on failure keep data/receipts and print how to clean up."""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--phase", default="all", choices=["source", "airflow", "datahub", "capture", "all"]
    )
    phase = parser.parse_args().phase
    os.chdir(ROOT)
    cfg = Config()
    try:
        if phase in ("source", "all"):
            source_phase(cfg)
        if phase in ("airflow", "all"):
            airflow_phase(cfg)
        if phase in ("datahub", "all"):
            datahub_phase(cfg)
        if phase == "capture" or (phase == "all" and cfg.storage != "s3"):
            capture_phase(cfg)
        elif phase == "all":
            print(
                "Run on the laptop over a tunnel: "
                ".venv/bin/python scripts/evidence/capture_order_lifecycle_datahub_evidence.py"
            )
    except (SystemExit, subprocess.CalledProcessError, OSError, ValueError) as exc:
        code = exc.code if isinstance(exc, SystemExit) else 1
        if code in (0, None):
            raise
        if not isinstance(code, int):
            print(code, file=sys.stderr)
            code = 1
        print(
            f"Phase {phase} failed (exit {code}). Data/receipts kept intact for log inspection.",
            file=sys.stderr,
        )
        cleanup_hint(cfg)
        sys.exit(code)


if __name__ == "__main__":
    main()
