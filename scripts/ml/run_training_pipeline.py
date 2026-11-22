"""Compile ml/training_pipeline.py and submit a run to Kubeflow Pipelines, then wait for it (30 min max).

Targets: kind (default; MinIO Secret from .env) or PIPELINE_TARGET=aws (EKS: ECR, S3 via Pod Identity, MLflow on EC2).
Requires KFP (infra/k8s/ml), MLflow (`docker compose --profile ml up -d mlflow`) and a registry configured for kind.

    python3 -m scripts.ml.run_training_pipeline [RUN_NAME]

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


def wait_for_kfp(seconds: int = 30) -> None:
    """Wait until the port-forward answers (EKS over the Internet is slower than kind)."""
    for _ in range(seconds):
        try:
            urllib.request.urlopen("http://localhost:8888/apis/v2beta1/healthz", timeout=2)
            return
        except OSError:
            time.sleep(1)


def main() -> None:
    """Build/push the training image, prepare credentials, port-forward KFP and submit."""
    os.chdir(ROOT)
    load_env()
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
        run([sys.executable, "-m", "scripts.shared.push_images", f"platform-train:{tag}"])
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
                *sys.argv[1:],
            ],
            stdin=SUBMIT,
        )
    finally:
        forward.terminate()


if __name__ == "__main__":
    main()
