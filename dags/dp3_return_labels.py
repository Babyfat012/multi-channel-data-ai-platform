"""Separate five-Channel return label set for training, joined to Gold through the ground-truth key ledger."""

from __future__ import annotations

from datetime import datetime, timezone

from airflow import DAG
from platform_spark_operator import PlatformSparkSubmitOperator as SparkSubmitOperator

with DAG(
    dag_id="dp3_return_labels",
    description="Join five-Channel labels with the Gold fact into one label table",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["platform", "dp3", "labels"],
) as dag:
    SparkSubmitOperator(
        task_id="build_and_validate_return_labels",
        application="/opt/spark_jobs/gold/build_order_snapshot_return_labels.py",
        application_args=[
            "--fact-table",
            "{{ var.value.platform_dp3_gold_source_table }}",
            "--run-id",
            "{{ run_id }}",
            # Executors read these two paths directly: local is a bind mount, AWS is S3.
            "--ground-truth",
            "{{ var.value.get('platform_dp3_ground_truth', '/data/ground-truth') }}",
            "--keys-path",
            "{{ var.value.get('platform_dp3_walmart_keys', "
            "'/data/ground-truth-bridge/walmart-keys.parquet') }}",
        ],
        conn_id="spark_default",
        packages="{{ var.value.platform_spark_packages }}",
        driver_memory="600m",
        executor_memory="1536m",
        executor_cores=1,
        conf={
            "spark.driver.host": "airflow",
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.jars.ivy": "/opt/airflow/.ivy2",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "64m",
        },
    )
