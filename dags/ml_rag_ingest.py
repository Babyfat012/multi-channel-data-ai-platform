"""LLM Platform RAG: text documents → chunk → embedding → Feast online store (pgvector) → validate → DataHub.

Each task calls one step of `rag/pipeline.py` inside feast-venv; credentials come from Airflow Connections at run time.
"""

from __future__ import annotations

import os
import subprocess
from datetime import datetime, timezone

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from platform_runtime import resolve_rag_env

PYTHON = "/home/airflow/feast-venv/bin/python"
RECEIPTS = "/opt/airflow/receipts/rag/{{ run_id }}"
MODEL_CACHE = "/opt/airflow/rag-models"  # airflow-data volume: download the model once


def run_step(step, receipt_dir=None, run_id=None, **_):
    """Run `python -m rag.pipeline <step>` with env built from Connection/Variable."""
    storage = os.environ.get("PLATFORM_STORAGE", "minio")
    # AWS (s3): no MinIO ⇒ do not look up that connection; S3 via the EC2 IAM role.
    names = ["platform_rag_pg"] + (["platform_minio"] if storage != "s3" else [])
    connections = {}
    for name in names:
        conn = BaseHook.get_connection(name)
        connections[name] = {
            "host": conn.host,
            "port": conn.port,
            "login": conn.login,
            "password": conn.password,
        }
    keys = ["platform_rag_root", "platform_feast_root"]
    keys += ["platform_minio_internal_endpoint"] if storage != "s3" else []
    variables = {k: Variable.get(k) for k in keys}
    env = {
        **os.environ,
        "PYTHONPATH": "/opt/rag",
        **resolve_rag_env(connections, variables, storage=storage),
    }
    args = [PYTHON, "-m", "rag.pipeline", step, "--model-cache", MODEL_CACHE]
    if receipt_dir:
        args += ["--receipt-dir", receipt_dir, "--run-id", run_id]
    if step == "publish":
        server = Variable.get("platform_datahub_gms_url", default_var="")
        if not server:
            raise AirflowSkipException(
                "Variable platform_datahub_gms_url is not set (DataHub is not running)"
            )
        args += ["--datahub", server]
    result = subprocess.run(args, env=env, capture_output=True, text=True, check=False)
    print(result.stdout, result.stderr, sep="\n")
    result.check_returncode()


with DAG(
    dag_id="ml_rag_ingest",
    description="RAG: text → chunk → embedding → Feast pgvector",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["platform", "ml-platform", "rag"],
) as dag:
    tasks = [
        PythonOperator(task_id=step, python_callable=run_step, op_kwargs={"step": step})
        for step in ("upload", "chunk", "embed", "load")
    ]
    receipt = {"receipt_dir": RECEIPTS, "run_id": "{{ run_id }}"}
    tasks += [
        PythonOperator(task_id=step, python_callable=run_step, op_kwargs={"step": step, **receipt})
        for step in ("validate", "publish")
    ]
    for upstream, downstream in zip(tasks, tasks[1:]):
        upstream >> downstream
