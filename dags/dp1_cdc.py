"""DP1 CDC extension: PostgreSQL WAL → Kafka → Iceberg Bronze, then validate."""

from __future__ import annotations

from datetime import datetime, timezone

from airflow import DAG
from platform_spark_operator import PlatformSparkSubmitOperator


class Dp1CdcSparkSubmitOperator(PlatformSparkSubmitOperator):
    """CDC tasks share Iceberg and Kafka connection configuration."""

    def __init__(self, **kwargs):
        """Select the connection groups used by the CDC Spark jobs."""
        kwargs.setdefault("runtime_groups", ("iceberg", "kafka"))
        super().__init__(**kwargs)


SparkSubmitOperator = Dp1CdcSparkSubmitOperator

PACKAGES = "{{ var.value.platform_cdc_spark_packages }}"
TABLE = "{{ var.value.platform_dp1_cdc_table }}"
BOUNDARY_DIR = "/opt/airflow/platform-control/cdc"
SPARK_CONF = {
    "spark.driver.host": "airflow",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.jars.ivy": "/opt/airflow/.ivy2",
}

with DAG(
    dag_id="dp1_cdc",
    description="Walmart CDC source via Kafka → Bronze and check offset completeness",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["platform", "dp1", "cdc"],
) as dag:
    ingest = SparkSubmitOperator(
        task_id="ingest_walmart_cdc",
        application="/opt/spark_jobs/cdc/ingest_walmart_order_changes.py",
        application_args=[
            "--bootstrap",
            "{{ conn.platform_kafka.host }}:{{ conn.platform_kafka.port }}",
            "--topic",
            "{{ var.value.platform_dp1_cdc_topic }}",
            "--table",
            TABLE,
            "--run-id",
            "{{ run_id }}",
            "--boundary-dir",
            BOUNDARY_DIR,
        ],
        conn_id="spark_default",
        packages=PACKAGES,
        driver_memory="600m",
        conf=SPARK_CONF,
    )
    validate = SparkSubmitOperator(
        task_id="validate_walmart_cdc",
        application="/opt/spark_jobs/cdc/validate_walmart_order_changes.py",
        application_args=[
            "--bootstrap",
            "{{ conn.platform_kafka.host }}:{{ conn.platform_kafka.port }}",
            "--topic",
            "{{ var.value.platform_dp1_cdc_topic }}",
            "--table",
            TABLE,
            "--run-id",
            "{{ run_id }}",
            "--boundary-dir",
            BOUNDARY_DIR,
        ],
        conn_id="spark_default",
        packages=PACKAGES,
        driver_memory="600m",
        conf=SPARK_CONF,
    )
    ingest >> validate
