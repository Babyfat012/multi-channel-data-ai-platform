"""ML Platform: Gold → offline Parquet/training set → apply → load/reconcile a separate batch release, then publish.

`end` is a SIMULATED point in time (default = Gold Cutoff), not wall-clock time: on every run the
Publisher picks the latest snapshot per SKU up to end; the active release keeps serving while the new one loads.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from datetime import datetime, timezone

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from platform_spark_operator import PlatformSparkSubmitOperator

FEAST = "/home/airflow/feast-venv/bin/feast"
FEAST_PYTHON = "/home/airflow/feast-venv/bin/python"
ENSURE_24H = (
    "import os; from push_stream_features import ensure_offline_file; "
    "ensure_offline_file(os.environ['FEAST_SKU_RETURN_24H_PATH'])"
)
REPO = "/opt/feature_repo"
# Gold A Cutoff (end of the 180 days of data); locally, if the variable is unset, fall back to the old Cutoff.
FULL_RELOAD_START = "2000-01-01T00:00:00+00:00"  # before any simulated data
CUTOFF = "{{ var.value.get('platform_ml_cutoff', var.value.platform_order_lifecycle_cutoff) }}"
# Spark writes via s3a://, Feast reads the same location via s3://.
SPARK_ROOT = "{{ var.value.platform_feast_root | replace('s3://', 's3a://') }}"
SPARK_OUTPUT = SPARK_ROOT + "/sku_daily"
CONF = {
    "spark.driver.host": "airflow",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.jars.ivy": "/opt/airflow/.ivy2",
}


def run_feast(*args, **context):
    """Run the Feast CLI on a copy of the repo (mounted read-only), with credentials fetched at run time."""
    # Do not declare `params=` in the signature: Airflow forbids context names clashing with *args positions (ValueError reserved).
    params = context.get("params")
    # Redis lost its data but the registry still remembers the old watermark ⇒ incremental loads 0 rows
    # and still goes green. full_reload: reload everything (FULL_RELOAD_START, end], writing the latest value per SKU.
    if args[0] == "materialize-incremental" and (params or {}).get("full_reload"):
        print(
            "full_reload=true: deliberately reloading the batch, skipping the incremental watermark check."
        )
        args = ("materialize", *args[1:-1], FULL_RELOAD_START, args[-1])
    root = Variable.get("platform_feast_root")
    redis = Variable.get("platform_feast_redis")
    if not redis:
        raise ValueError("platform_feast_redis is empty: the online Redis address is not set")
    env = {**os.environ}
    # AWS: real S3 via the EC2 IAM role, no keys, no custom endpoint. Local: MinIO.
    if os.environ.get("PLATFORM_STORAGE", "minio") != "s3":
        minio = BaseHook.get_connection("platform_minio")
        env.update(
            AWS_ACCESS_KEY_ID=minio.login,
            AWS_SECRET_ACCESS_KEY=minio.password,
            FEAST_S3_ENDPOINT_URL=Variable.get("platform_minio_internal_endpoint"),
        )
    env |= {
        "FEAST_REGISTRY": f"{root}/registry.db",
        "FEAST_SKU_DAILY_PATH": f"{root}/sku_daily",
        "FEAST_SKU_RETURN_24H_PATH": f"{root}/sku_return_24h/",
        "FEAST_REDIS": redis,
        "FEAST_PUBLICATION_ROOT": f"{root}/publications",
    }
    with tempfile.TemporaryDirectory() as tmp:
        repo = shutil.copytree(REPO, f"{tmp}/repo")
        if args[0] == "publish-batch":
            command = [FEAST_PYTHON, "-m", "publish_batch", *args[1:]]
            if (params or {}).get("full_reload"):
                command.append("--full-reload")
            subprocess.run(command, cwd=repo, env=env, check=True, timeout=3600)
            return
        if args[0] == "materialize-incremental":
            subprocess.run(
                [FEAST_PYTHON, "-m", "check_online_store"],
                cwd=repo,
                env=env,
                check=True,
                timeout=120,
            )
        if args[0] == "apply":
            # The 24h stream view reads Parquet created by the pusher; where the pusher is not running (AWS), create an empty file.
            subprocess.run(
                [FEAST_PYTHON, "-c", ENSURE_24H], cwd=repo, env=env, check=True, timeout=120
            )
        result = subprocess.run(
            [FEAST, *args], cwd=repo, env=env, capture_output=True, text=True, check=False
        )
    print(result.stdout, result.stderr, sep="\n")
    result.check_returncode()


with DAG(
    dag_id="ml_feature_materialize",
    description="Daily SKU features: Parquet → separate batch → reconcile → publish Redis A/B",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    params={
        "end": Param("", type="string", description="Simulated point in time; empty = Gold Cutoff"),
        "full_reload": Param(
            False,
            type="boolean",
            description="Allow recovery when the online release being served has lost its data",
        ),
    },
    tags=["platform", "ml-platform", "feature-store"],
) as dag:
    export = PlatformSparkSubmitOperator(
        task_id="export_sku_daily",
        application="/opt/spark_jobs/gold/export_sku_daily_features.py",
        application_args=["--cutoff", CUTOFF, "--output", SPARK_OUTPUT],
        conn_id="spark_default",
        packages="{{ var.value.platform_spark_packages }},org.apache.hadoop:hadoop-aws:3.3.4",
        driver_memory="1g",
        conf=CONF,
    )
    # Training set (entity df for KFP): labeled lines before the Cutoff + order-time attributes, same label run as the features.
    export_training = PlatformSparkSubmitOperator(
        task_id="export_training_lines",
        application="/opt/spark_jobs/gold/export_training_lines.py",
        application_args=[
            "--cutoff",
            CUTOFF,
            "--output",
            SPARK_ROOT + "/training_lines",
            # 2‰ ≈ 360k lines: the offline Feast PIT join runs pandas in ONE KFP pod; 1% (1.8M) OOMs an 8 GB node.
            "--sample-per-mille",
            "2",
        ],
        conn_id="spark_default",
        packages="{{ var.value.platform_spark_packages }},org.apache.hadoop:hadoop-aws:3.3.4",
        driver_memory="1g",
        conf=CONF,
    )
    apply = PythonOperator(task_id="feast_apply", python_callable=run_feast, op_args=["apply"])
    materialize = PythonOperator(
        task_id="publish_batch",
        python_callable=run_feast,
        op_args=[
            "publish-batch",
            "--end",
            # Python 3.10 fromisoformat cannot parse the "Z" suffix.
            "{{ (params.end or var.value.get('platform_ml_cutoff', var.value.platform_order_lifecycle_cutoff))"
            " | replace('Z', '+00:00') }}",
        ],
    )
    export >> export_training >> apply >> materialize
