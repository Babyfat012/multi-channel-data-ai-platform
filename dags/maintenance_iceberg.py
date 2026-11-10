"""Iceberg maintenance: daily Iceberg maintenance schedule, paused by default, enabled manually after review."""

from datetime import datetime, timezone

from airflow import DAG
from platform_spark_operator import PlatformSparkSubmitOperator

TABLES = "{{ var.value.platform_maintenance_tables }}"
DIRECTORY = "{{ var.value.platform_maintenance_receipt_dir }}/{{ ts_nodash }}"
APPLICATION = "/opt/spark_jobs/maintenance/maintain_iceberg_tables.py"
CONFIG = "{{ var.value.platform_maintenance_config }}"
CONF = {
    "spark.driver.host": "airflow",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.jars.ivy": "/opt/airflow/.ivy2",
}

with DAG(
    dag_id="maintenance_iceberg",
    description="Plan → maintain + check data → check receipt with a fresh session",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["platform", "iceberg-maintenance", "iceberg"],
) as dag:
    plan = PlatformSparkSubmitOperator(
        task_id="plan",
        application=APPLICATION,
        application_args=[
            "--plan-only",
            "--tables",
            TABLES,
            "--config-json",
            CONFIG,
            "--receipt",
            DIRECTORY + "/plan.json",
        ],
        conn_id="spark_default",
        packages="{{ var.value.platform_spark_packages }}",
        driver_memory="600m",
        conf=CONF,
    )
    execute = PlatformSparkSubmitOperator(
        task_id="execute",
        application=APPLICATION,
        application_args=[
            "--execute",
            "--tables",
            TABLES,
            "--config-json",
            CONFIG,
            "--receipt",
            DIRECTORY + "/execute.json",
        ],
        conn_id="spark_default",
        packages="{{ var.value.platform_spark_packages }}",
        driver_memory="600m",
        conf=CONF,
    )
    validate = PlatformSparkSubmitOperator(
        task_id="validate",
        application=APPLICATION,
        application_args=["--validate-receipt", DIRECTORY + "/execute.json"],
        conn_id="spark_default",
        packages="{{ var.value.platform_spark_packages }}",
        driver_memory="600m",
        conf=CONF,
    )
    plan >> execute >> validate
