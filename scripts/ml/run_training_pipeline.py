"""Compile ml/training_pipeline.py and submit a run to Kubeflow Pipelines, or (re)create its schedules.

Targets: kind (default; MinIO Secret from .env) or PIPELINE_TARGET=aws (EKS: ECR, S3 via Pod Identity, MLflow on EC2).
Requires KFP (infra/k8s/ml), MLflow (`docker compose --profile ml up -d mlflow`) and a registry configured for kind.

    python3 -m scripts.ml.run_training_pipeline [RUN_NAME]   # one training run, wait for it (30 min max)
    python3 -m scripts.ml.run_training_pipeline --schedule   # recurring runs: monitor daily, training weekly

A schedule pins the images compiled into it (TAG): rerun --schedule after changing ml/ code.
SKIP_BUILD_PUSH=1 when the image TAG is already in the registry (immutable ECR tags reject a re-push).
"""

import os
import subprocess
import sys
import time
import urllib.request

from scripts.common import ROOT, apply, env, load_env, run

# Runs in a throwaway uv environment that has the KFP SDK.
SUBMIT = """
import sys
import kfp
from kfp import compiler
sys.path.insert(0, ".")
from ml.training_pipeline import return_risk_training

package = "out/kfp/return_risk_training.yaml"
compiler.Compiler().compile(return_risk_training, package)
client = kfp.Client(host="http://localhost:8888")
run = client.create_run_from_pipeline_package(package, arguments={}, run_name=sys.argv[1] if len(sys.argv) > 1 else None,
                                              experiment_name="return-risk")
print("run_id", run.run_id)
result = client.wait_for_run_completion(run.run_id, timeout=1800)
print("state", result.state)
"""

# Diagram: drift "every day", training "on drift or every week". KFP cron has a seconds field; times are UTC.
SCHEDULE = """
import sys
import kfp
from kfp import compiler
sys.path.insert(0, ".")
from ml.training_pipeline import return_risk_monitor, return_risk_training

client = kfp.Client(host="http://localhost:8888")
experiment = client.create_experiment("return-risk")  # returns the existing one
existing = client.list_recurring_runs(experiment_id=experiment.experiment_id, page_size=100).recurring_runs or []
for name, pipeline, cron in (
    ("return-risk-monitor-daily", return_risk_monitor, "0 0 2 * * *"),
    ("return-risk-training-weekly", return_risk_training, "0 0 3 * * 0"),
):
    package = f"out/kfp/{name}.yaml"
    compiler.Compiler().compile(pipeline, package)
    for old in existing:  # replace, so an old image tag does not keep running next to the new one
        if old.display_name == name:
            client.delete_recurring_run(old.recurring_run_id)
    # max_concurrency=1 + no_catchup: a slow run is not overlapped, and a paused cluster does not replay missed days.
    client.create_recurring_run(experiment.experiment_id, name, cron_expression=cron,
                                pipeline_package_path=package, max_concurrency=1, no_catchup=True)
    print("scheduled", name, cron)
"""


def wait_for_kfp(seconds: int = 30) -> None:
    """Wait until the port-forward answers (EKS over the Internet is slower than kind)."""
    for _ in range(seconds):
        try:
            urllib.request.urlopen("http://localhost:8888/apis/v2beta1/healthz", timeout=2)
            return
        except OSError:
            time.sleep(1)


def main() -> None:
    """Build/push the train + drift images, prepare credentials, port-forward KFP, then submit or schedule."""
    os.chdir(ROOT)
    load_env()
    schedule = sys.argv[1:] == ["--schedule"]
    registry = os.environ.setdefault("REGISTRY", "localhost:5001")
    tag = os.environ.get("TAG") or run(["git", "rev-parse", "HEAD"], capture=True)
    if tag == "latest":
        sys.exit("TAG must be immutable, do not use latest")
    os.environ["TAG"] = tag
    target = os.environ.setdefault("PIPELINE_TARGET", "kind")
    if target == "aws":
        # EKS: S3 via pipeline-runner's Pod Identity, MLflow on EC2; no MinIO Secret.
        env("LAKEHOUSE_BUCKET")
        env("MLFLOW_TRACKING_URI")  # e.g. http://10.42.0.12:5050
        if ".amazonaws.com/" not in registry:
            sys.exit("aws: REGISTRY must be ECR")
    if os.environ.get("SKIP_BUILD_PUSH") != "1":
        run(
            [
                "docker",
                "build",
                "-f",
                "infra/docker/Dockerfile.train",
                "-t",
                f"platform-train:{tag}",
                ".",
            ]
        )
        run(
            [
                "docker",
                "build",
                "-f",
                "infra/docker/Dockerfile.drift",
                "-t",
                f"platform-drift:{tag}",
                ".",
            ]
        )
        run(
            [
                sys.executable,
                "-m",
                "scripts.shared.push_images",
                f"platform-train:{tag}",
                f"platform-drift:{tag}",
            ]
        )
    kc = ["kubectl", "--context", os.environ.get("KCTX") or "kind-platform", "-n", "kubeflow"]
    if target != "aws":
        apply(
            kc,
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {"name": "platform-minio", "namespace": "kubeflow"},
                "stringData": {
                    "AWS_ACCESS_KEY_ID": env("MINIO_ROOT_USER"),
                    "AWS_SECRET_ACCESS_KEY": env("MINIO_ROOT_PASSWORD"),
                },
            },
        )
    (ROOT / "out/kfp").mkdir(parents=True, exist_ok=True)
    forward = subprocess.Popen(
        [*kc, "port-forward", "svc/ml-pipeline-ui", "8888:80"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        wait_for_kfp()
        run(
            [
                "uv",
                "run",
                "--no-project",
                "--with",
                "kfp==2.17.0",
                "--with",
                "kfp-kubernetes==2.17.0",
                "python",
                "-",
                *([] if schedule else sys.argv[1:]),
            ],
            stdin=SCHEDULE if schedule else SUBMIT,
        )
    finally:
        forward.terminate()


if __name__ == "__main__":
    main()
