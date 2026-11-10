"""Manually triggered DAG: v2 source → contract-checked Silver → Gold, with label/feature checks.

Reads configuration only at runtime; Spark emits a Bronze receipt with the real schema.
The operator writes a failure receipt only when Spark fails.
"""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from platform_spark_operator import PlatformSparkSubmitOperator


class OrderLifecycleSparkSubmitOperator(PlatformSparkSubmitOperator):
    """Render v2 resources even when the provider does not template these fields."""

    template_fields = tuple(
        dict.fromkeys(
            (
                *PlatformSparkSubmitOperator.template_fields,
                "_driver_memory",
                "_executor_memory",
                "_executor_cores",
            )
        )
    )


DIRECTORY = "{{ var.value.platform_order_lifecycle_receipt_dir }}/{{ ts_nodash }}"
SOURCE_ROOT = "{{ params.source_root or var.value.platform_order_lifecycle_source_root }}"
CUTOFF = "{{ params.cutoff or var.value.platform_order_lifecycle_cutoff }}"
RUN_ID = "{{ run_id }}"
PACKAGES = "{{ var.value.platform_spark_packages }}"
CONF = {
    "spark.driver.host": "airflow",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.jars.ivy": "/opt/airflow/.ivy2",
}


def write_receipt(path, document):
    """Save the receipt with the real completion time into the run's directory."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    document["completed_at"] = datetime.now(timezone.utc).isoformat()
    target.write_text(json.dumps(document, ensure_ascii=False, indent=2) + "\n")


def check_registry(receipt, run_id):
    """Run the contract CLI with the bundled Python, save errors too, then pass on the exit code."""
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "spark_jobs.lib.data_contracts",
            "check-registry",
            "--contracts-dir",
            "/opt/contracts",
        ],
        cwd="/opt",
        capture_output=True,
        text=True,
        check=False,
    )
    write_receipt(
        receipt,
        {
            "run_id": run_id,
            "task_id": "check_contracts",
            "status": "passed" if result.returncode == 0 else "failed",
            "exit_code": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
        },
    )
    if result.returncode:
        raise RuntimeError(f"check-registry failed; see receipt {receipt}")


class OrderLifecycleBronzeOperator(OrderLifecycleSparkSubmitOperator):
    """Add optional JDBC and a failure receipt for job 20."""

    template_fields = (
        *OrderLifecycleSparkSubmitOperator.template_fields,
        "receipt_path",
        "walmart_jdbc",
    )

    def __init__(self, *, receipt_path, walmart_jdbc, **kwargs):
        """Take the receipt path and JDBC table name already templated by Airflow."""
        self.receipt_path = receipt_path
        self.walmart_jdbc = walmart_jdbc
        super().__init__(**kwargs)

    def execute(self, context):
        """Keep a successful Spark receipt; write a failure receipt, then re-raise the Spark error unchanged."""
        original_args = self.application_args
        document = {
            "run_id": context["run_id"],
            "task_id": "ingest_bronze",
            "status": "failed",
            "checks_available": False,
            "tables": {"raw_source_records": "lakehouse.bronze.lifecycle_raw_records"},
            "note": "Spark failed; see the job 20 log, a failure receipt does not confirm metrics/schema.",
        }
        try:
            self.application_args = list(original_args)
            if self.walmart_jdbc:
                self.application_args += ["--walmart-jdbc", self.walmart_jdbc]
            return super().execute(context)
        except Exception:
            write_receipt(self.receipt_path, document)
            raise
        finally:
            self.application_args = original_args


with DAG(
    dag_id="dp_order_lifecycle",
    description="Contracts → Bronze v2 → Silver v2 → Gold v2 → Validate",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    is_paused_upon_creation=True,
    params={
        "source_root": Param(
            None,
            type=["null", "string"],
            minLength=1,
            description="Fixture directory in the container; null uses the Variable",
        ),
        "cutoff": Param(
            None,
            type=["null", "string"],
            format="date-time",
            description="Cutoff UTC; null uses the Variable",
        ),
        "walmart_jdbc": Param(
            None,
            type=["null", "string"],
            pattern=r"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$",
            description="E.g. walmart_lifecycle.orders; null reads the bootstrap file",
        ),
    },
    tags=["platform", "v2", "contracts"],
) as dag:
    check_contracts = PythonOperator(
        task_id="check_contracts",
        python_callable=check_registry,
        op_kwargs={"receipt": DIRECTORY + "/check_contracts.json", "run_id": RUN_ID},
    )
    ingest_bronze = OrderLifecycleBronzeOperator(
        task_id="ingest_bronze",
        application="/opt/spark_jobs/bronze/ingest_order_lifecycle.py",
        application_args=[
            "--root",
            SOURCE_ROOT,
            "--run-id",
            RUN_ID,
            "--receipt",
            DIRECTORY + "/ingest_bronze.json",
        ],
        walmart_jdbc="{{ params.walmart_jdbc or '' }}",
        receipt_path=DIRECTORY + "/ingest_bronze.json",
        runtime_groups=("iceberg", "postgres"),
        conn_id="spark_default",
        deploy_mode="client",
        packages=PACKAGES,
        driver_memory="{{ var.value.platform_order_lifecycle_driver_memory }}",
        executor_memory="{{ var.value.platform_order_lifecycle_executor_memory }}",
        executor_cores="{{ var.value.platform_order_lifecycle_executor_cores }}",
        conf=CONF,
    )
    build_silver = OrderLifecycleSparkSubmitOperator(
        task_id="build_silver",
        application="/opt/spark_jobs/silver/build_order_lifecycle.py",
        application_args=[
            "--run-id",
            RUN_ID,
            "--as-of",
            CUTOFF,
            "--contracts-dir",
            "/opt/contracts",
            "--receipt",
            DIRECTORY + "/build_silver.json",
        ],
        conn_id="spark_default",
        deploy_mode="client",
        packages=PACKAGES,
        driver_memory="{{ var.value.platform_order_lifecycle_driver_memory }}",
        executor_memory="{{ var.value.platform_order_lifecycle_executor_memory }}",
        executor_cores="{{ var.value.platform_order_lifecycle_executor_cores }}",
        conf=CONF,
    )
    build_gold = OrderLifecycleSparkSubmitOperator(
        task_id="build_gold",
        application="/opt/spark_jobs/gold/build_order_lifecycle.py",
        application_args=[
            "--run-id",
            RUN_ID,
            "--cutoff",
            CUTOFF,
            "--receipt",
            DIRECTORY + "/build_gold.json",
        ],
        conn_id="spark_default",
        deploy_mode="client",
        packages=PACKAGES,
        driver_memory="{{ var.value.platform_order_lifecycle_driver_memory }}",
        executor_memory="{{ var.value.platform_order_lifecycle_executor_memory }}",
        executor_cores="{{ var.value.platform_order_lifecycle_executor_cores }}",
        conf=CONF,
    )
    validate_gold = OrderLifecycleSparkSubmitOperator(
        task_id="validate_gold",
        application="/opt/spark_jobs/gold/validate_order_lifecycle.py",
        application_args=[
            "--run-id",
            RUN_ID,
            "--cutoff",
            CUTOFF,
            "--receipt",
            DIRECTORY + "/validate_gold.json",
        ],
        conn_id="spark_default",
        deploy_mode="client",
        packages=PACKAGES,
        driver_memory="{{ var.value.platform_order_lifecycle_driver_memory }}",
        executor_memory="{{ var.value.platform_order_lifecycle_executor_memory }}",
        executor_cores="{{ var.value.platform_order_lifecycle_executor_cores }}",
        conf=CONF,
    )
    check_contracts >> ingest_bronze >> build_silver >> build_gold >> validate_gold
