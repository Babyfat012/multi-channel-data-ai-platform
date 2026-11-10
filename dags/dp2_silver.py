"""DP2: Bronze -> five-Channel Silver -> Gold fact, validating each stage.

Spark tasks run sequentially to fit the dev machine's RAM.
"""

from __future__ import annotations

from datetime import datetime, timezone

from airflow import DAG
from platform_spark_operator import PlatformSparkSubmitOperator as SparkSubmitOperator

PACKAGES = "{{ var.value.platform_spark_packages }}"
RUN_ID = "{{ run_id }}"
AMAZON_BRONZE = "{{ var.value.platform_dp2_amazon_bronze_table }}"
WALMART_BRONZE = "{{ var.value.platform_dp2_walmart_bronze_table }}"
AMAZON_SILVER = "{{ var.value.platform_dp2_amazon_silver_table }}"
WALMART_SILVER = "{{ var.value.platform_dp2_walmart_silver_table }}"
GOLD_TABLE = "{{ var.value.platform_dp2_gold_table }}"
AMAZON_DIM = "{{ var.value.platform_dp2_amazon_dim_table }}"
SPARK_CONF = {
    "spark.driver.host": "airflow",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.jars.ivy": "/opt/airflow/.ivy2",
}

with DAG(
    dag_id="dp2_silver",
    description="Bronze 5 Channels -> Silver Order Line; check the output of each Channel",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["platform", "dp2", "silver"],
) as dag:
    ingest_amazon = SparkSubmitOperator(
        task_id="ingest_amazon_silver",
        application="/opt/spark_jobs/silver/build_amazon_order_snapshots.py",
        application_args=[
            "--source-table",
            AMAZON_BRONZE,
            "--table",
            AMAZON_SILVER,
            "--run-id",
            RUN_ID,
        ],
        conn_id="spark_default",
        packages=PACKAGES,
        driver_memory="600m",
        conf=SPARK_CONF,
    )
    validate_amazon = SparkSubmitOperator(
        task_id="validate_amazon_silver",
        application="/opt/spark_jobs/silver/validate_order_snapshots.py",
        application_args=[
            "--channel",
            "amazon",
            "--source-table",
            AMAZON_BRONZE,
            "--table",
            AMAZON_SILVER,
            "--run-id",
            RUN_ID,
        ],
        conn_id="spark_default",
        packages=PACKAGES,
        driver_memory="600m",
        conf=SPARK_CONF,
    )
    ingest_walmart = SparkSubmitOperator(
        task_id="ingest_walmart_silver",
        application="/opt/spark_jobs/silver/build_walmart_order_snapshots.py",
        application_args=[
            "--source-table",
            WALMART_BRONZE,
            "--table",
            WALMART_SILVER,
            "--run-id",
            RUN_ID,
        ],
        conn_id="spark_default",
        packages=PACKAGES,
        driver_memory="600m",
        executor_memory="1536m",
        executor_cores=1,
        conf={**SPARK_CONF, "spark.sql.adaptive.advisoryPartitionSizeInBytes": "64m"},
    )
    validate_walmart = SparkSubmitOperator(
        task_id="validate_walmart_silver",
        application="/opt/spark_jobs/silver/validate_order_snapshots.py",
        application_args=[
            "--channel",
            "walmart",
            "--source-table",
            WALMART_BRONZE,
            "--table",
            WALMART_SILVER,
            "--run-id",
            RUN_ID,
        ],
        conn_id="spark_default",
        packages=PACKAGES,
        driver_memory="600m",
        conf=SPARK_CONF,
    )

    previous = validate_walmart
    ingest_amazon >> validate_amazon >> ingest_walmart >> validate_walmart
    for channel in ("tiktok_shop", "shopify", "shopee"):
        bronze_table = "{{ var.value.platform_dp2_" + channel + "_bronze_table }}"
        silver_table = "{{ var.value.platform_dp2_" + channel + "_silver_table }}"
        ingest = SparkSubmitOperator(
            task_id=f"ingest_{channel}_silver",
            application="/opt/spark_jobs/silver/build_file_order_snapshots.py",
            application_args=[
                "--channel",
                channel,
                "--source-table",
                bronze_table,
                "--table",
                silver_table,
                "--run-id",
                RUN_ID,
            ],
            conn_id="spark_default",
            packages=PACKAGES,
            driver_memory="600m",
            conf=SPARK_CONF,
        )
        validate = SparkSubmitOperator(
            task_id=f"validate_{channel}_silver",
            application="/opt/spark_jobs/silver/validate_file_order_snapshots.py",
            application_args=[
                "--channel",
                channel,
                "--source-table",
                bronze_table,
                "--table",
                silver_table,
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

    gold_sources = [
        "--amazon-source",
        AMAZON_SILVER,
        "--walmart-source",
        WALMART_SILVER,
    ]
    for channel in ("tiktok_shop", "shopify", "shopee"):
        gold_sources += [
            f"--{channel.replace('_', '-')}-source",
            "{{ var.value.platform_dp2_" + channel + "_silver_table }}",
        ]
    gold_args = ["--table", GOLD_TABLE, "--run-id", RUN_ID, *gold_sources]
    build_gold = SparkSubmitOperator(
        task_id="build_gold_order_lines",
        application="/opt/spark_jobs/gold/build_order_snapshot_lines.py",
        application_args=gold_args,
        conn_id="spark_default",
        packages=PACKAGES,
        driver_memory="600m",
        conf=SPARK_CONF,
    )
    validate_gold = SparkSubmitOperator(
        task_id="validate_gold_order_lines",
        application="/opt/spark_jobs/gold/validate_order_snapshot_lines.py",
        application_args=gold_args,
        conn_id="spark_default",
        packages=PACKAGES,
        driver_memory="600m",
        conf=SPARK_CONF,
    )
    build_dim = SparkSubmitOperator(
        task_id="build_amazon_order_dim",
        application="/opt/spark_jobs/gold/build_amazon_order_status_scd2.py",
        application_args=[
            "--source-table",
            AMAZON_BRONZE,
            "--table",
            AMAZON_DIM,
            "--run-id",
            RUN_ID,
        ],
        conn_id="spark_default",
        packages=PACKAGES,
        driver_memory="600m",
        executor_memory="1536m",
        executor_cores=1,
        conf={**SPARK_CONF, "spark.sql.adaptive.advisoryPartitionSizeInBytes": "64m"},
    )
    validate_dim = SparkSubmitOperator(
        task_id="validate_amazon_order_dim",
        application="/opt/spark_jobs/gold/validate_amazon_order_status_scd2.py",
        application_args=["--table", AMAZON_DIM, "--fact-table", GOLD_TABLE, "--run-id", RUN_ID],
        conn_id="spark_default",
        packages=PACKAGES,
        driver_memory="600m",
        executor_memory="1536m",
        executor_cores=1,
        conf={**SPARK_CONF, "spark.sql.adaptive.advisoryPartitionSizeInBytes": "64m"},
    )
    previous >> build_gold >> build_dim >> validate_dim >> validate_gold
