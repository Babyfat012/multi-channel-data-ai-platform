"""DP1: load five Channels into Bronze, validating each source before moving on.

Manually triggered on the `_dp1` trial tables, one Spark task at a time to fit laptop RAM.
"""

from __future__ import annotations

from datetime import datetime, timezone

from airflow import DAG
from platform_spark_operator import PlatformSparkSubmitOperator


class Dp1SparkSubmitOperator(PlatformSparkSubmitOperator):
    """DP1 tasks share Iceberg and source-PostgreSQL credentials."""

    def __init__(self, **kwargs):
        """Select the two runtime connection groups required by DP1 jobs."""
        kwargs.setdefault("runtime_groups", ("iceberg", "postgres"))
        super().__init__(**kwargs)


SparkSubmitOperator = Dp1SparkSubmitOperator

PACKAGES = "{{ var.value.platform_spark_packages }}"
RUN_ID = "{{ run_id }}"
AMAZON_TABLE = "{{ var.value.platform_dp1_amazon_table }}"
WALMART_TABLE = "{{ var.value.platform_dp1_walmart_table }}"
SOURCE_ROOT = "{{ var.value.platform_dp1_extra_source_root }}"
WINDOW_DAYS = "{{ var.value.platform_dp1_amazon_window_days }}"
WINDOW_START = "{{ var.value.platform_dp1_amazon_window_start }}"
# Local reads the /data/raw mount; AWS sets the Variable to point at s3a:// (docker-compose.aws.yml).
AMAZON_SOURCE = (
    "{{ var.value.get('platform_dp1_amazon_source', "
    "'/data/raw/source-exports/channel=amazon/dataset=orders') }}"
)
SPARK_CONF = {
    "spark.driver.host": "airflow",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.jars.ivy": "/opt/airflow/.ivy2",
}

with DAG(
    dag_id="dp1_bronze",
    description="Raw 5 Channels -> Bronze; check row counts, payload and provenance",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["platform", "dp1", "bronze"],
) as dag:
    ingest_amazon = SparkSubmitOperator(
        task_id="ingest_amazon",
        application="/opt/spark_jobs/bronze/ingest_amazon_order_snapshots.py",
        application_args=[
            "--window-days",
            WINDOW_DAYS,
            "--window-start",
            WINDOW_START,
            "--source",
            AMAZON_SOURCE,
            "--table",
            AMAZON_TABLE,
            "--run-id",
            RUN_ID,
        ],
        conn_id="spark_default",
        packages=PACKAGES,
        driver_memory="600m",
        conf=SPARK_CONF,
    )
    validate_amazon = SparkSubmitOperator(
        task_id="validate_amazon",
        application="/opt/spark_jobs/bronze/validate_order_snapshots.py",
        application_args=[
            "--channel",
            "amazon",
            "--window-days",
            WINDOW_DAYS,
            "--window-start",
            WINDOW_START,
            "--source",
            AMAZON_SOURCE,
            "--table",
            AMAZON_TABLE,
            "--run-id",
            RUN_ID,
        ],
        conn_id="spark_default",
        packages=PACKAGES,
        driver_memory="600m",
        conf=SPARK_CONF,
    )
    ingest_walmart = SparkSubmitOperator(
        task_id="ingest_walmart",
        application="/opt/spark_jobs/bronze/ingest_walmart_order_snapshots.py",
        application_args=["--num-partitions", "16", "--table", WALMART_TABLE, "--run-id", RUN_ID],
        conn_id="spark_default",
        packages=PACKAGES,
        driver_memory="600m",
        conf=SPARK_CONF,
    )
    validate_walmart = SparkSubmitOperator(
        task_id="validate_walmart",
        application="/opt/spark_jobs/bronze/validate_order_snapshots.py",
        application_args=["--channel", "walmart", "--table", WALMART_TABLE, "--run-id", RUN_ID],
        conn_id="spark_default",
        packages=PACKAGES,
        driver_memory="600m",
        conf=SPARK_CONF,
    )

    previous = validate_walmart
    ingest_amazon >> validate_amazon >> ingest_walmart >> validate_walmart
    for channel in ("tiktok_shop", "shopify", "shopee"):
        table = "{{ var.value.platform_dp1_" + channel + "_table }}"
        ingest = SparkSubmitOperator(
            task_id=f"ingest_{channel}",
            application="/opt/spark_jobs/bronze/ingest_file_order_snapshots.py",
            application_args=[
                "--channel",
                channel,
                "--source-root",
                SOURCE_ROOT,
                "--table",
                table,
                "--run-id",
                RUN_ID,
            ],
            conn_id="spark_default",
            packages=PACKAGES,
            driver_memory="600m",
            conf=SPARK_CONF,
        )
        validate = SparkSubmitOperator(
            task_id=f"validate_{channel}",
            application="/opt/spark_jobs/bronze/validate_file_order_snapshots.py",
            application_args=[
                "--channel",
                channel,
                "--source-root",
                SOURCE_ROOT,
                "--table",
                table,
                "--run-id",
                RUN_ID,
            ],
            conn_id="spark_default",
            packages=PACKAGES,
            driver_memory="600m",
            conf=SPARK_CONF,
        )
        previous >> ingest >> validate
        previous = validate
