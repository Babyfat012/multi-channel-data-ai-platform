"""Manually triggered Walmart v2 CDC DAG, paused by default; bounded ingest, then oracle check."""

from __future__ import annotations

from datetime import datetime, timezone

from airflow import DAG
from platform_spark_operator import PlatformSparkSubmitOperator


class OrderLifecycleCdcSparkSubmitOperator(PlatformSparkSubmitOperator):
    """Let Airflow render all three v2 Spark resource fields."""

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


DIRECTORY = "{{ var.value.platform_order_lifecycle_cdc_control_dir }}/{{ ts_nodash }}"
COMMON = ["--run-id", "{{ run_id }}", "--boundary-dir", DIRECTORY]
RESOURCES = {
    "conn_id": "spark_default",
    "packages": "{{ var.value.platform_order_lifecycle_cdc_spark_packages }}",
    "driver_memory": "{{ var.value.platform_order_lifecycle_driver_memory }}",
    "executor_memory": "{{ var.value.platform_order_lifecycle_executor_memory }}",
    "executor_cores": "{{ var.value.platform_order_lifecycle_executor_cores }}",
    "conf": {
        "spark.driver.host": "airflow",
        "spark.driver.bindAddress": "0.0.0.0",
        "spark.jars.ivy": "/opt/airflow/.ivy2",
    },
}

with DAG(
    dag_id="dp_order_lifecycle_cdc",
    description="Walmart v2 WAL → Kafka → Bronze CDC → oracle",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    is_paused_upon_creation=True,
    tags=["platform", "dp1", "cdc", "v2"],
) as dag:
    ingest_cdc = OrderLifecycleCdcSparkSubmitOperator(
        task_id="ingest_cdc",
        application="/opt/spark_jobs/cdc/ingest_order_lifecycle_changes.py",
        application_args=COMMON
        + [
            "--bootstrap",
            "{{ conn.platform_kafka.host }}:{{ conn.platform_kafka.port }}",
            "--receipt",
            DIRECTORY + "/ingest.json",
        ],
        runtime_groups=("iceberg",),
        **RESOURCES,
    )
    validate_cdc = OrderLifecycleCdcSparkSubmitOperator(
        task_id="validate_cdc",
        application="/opt/spark_jobs/cdc/validate_order_lifecycle_changes.py",
        application_args=COMMON
        + [
            "--oracle",
            "{{ var.value.platform_order_lifecycle_cdc_oracle }}",
            "--ingest-receipt",
            DIRECTORY + "/ingest.json",
            "--receipt",
            DIRECTORY + "/validate.json",
        ],
        runtime_groups=("iceberg",),
        **RESOURCES,
    )
    ingest_cdc >> validate_cdc
