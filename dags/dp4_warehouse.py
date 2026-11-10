"""DP4: the last 30 days of the five-Channel Gold fact → PostgreSQL warehouse + seller_sku index."""

from __future__ import annotations

from datetime import datetime, timezone

from airflow import DAG
from platform_spark_operator import PlatformSparkSubmitOperator as SparkSubmitOperator

with DAG(
    dag_id="dp4_warehouse",
    description="Gold fact 30 days before Cutoff → PostgreSQL; reconcile per Channel, then index",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["platform", "dp4", "warehouse"],
) as dag:
    SparkSubmitOperator(
        task_id="export_and_index_warehouse",
        application="/opt/spark_jobs/warehouse/export_order_snapshot_lines.py",
        # JDBC write + reconciliation need Postgres credentials from Connection platform_postgres (the operator
        # grants only the declared groups).
        runtime_groups=("iceberg", "postgres"),
        application_args=[
            "--source-table",
            "{{ var.value.platform_dp3_gold_source_table }}",
            "--cutoff",
            "{{ var.value.get('platform_ml_cutoff', var.value.platform_order_lifecycle_cutoff) }}",
            "--days",
            "30",
        ],
        conn_id="spark_default",
        packages="{{ var.value.platform_spark_packages }}",
        driver_memory="1g",
        executor_memory="1536m",
        executor_cores=1,
        conf={
            "spark.driver.host": "airflow",
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.jars.ivy": "/opt/airflow/.ivy2",
        },
    )
