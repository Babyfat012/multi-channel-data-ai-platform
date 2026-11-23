"""Small Airflow DAG that verifies shared PLATFORM connections and Variables."""

from __future__ import annotations

from datetime import datetime, timezone

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from platform_runtime import resolve_runtime_env
from platform_spark_operator import PlatformSparkSubmitOperator


def verify_runtime_configuration() -> None:
    """Resolve all configured groups from Airflow metadata without running Spark."""
    connection_ids = ("platform_minio", "platform_postgres", "platform_kafka")
    connections = {}
    for connection_id in connection_ids:
        connection = BaseHook.get_connection(connection_id)
        connections[connection_id] = {
            "host": connection.host,
            "port": connection.port,
            "schema": connection.schema,
            "login": connection.login,
            "password": connection.password,
        }

    variable_names = (
        "platform_minio_internal_endpoint",
        "platform_warehouse_name",
        "platform_lakekeeper_catalog_uri",
        "platform_dp1_cdc_topic",
    )
    variables = {name: Variable.get(name) for name in variable_names}
    resolved = resolve_runtime_env(connections, variables, ("iceberg", "postgres", "kafka"))

    required = {
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "MINIO_INTERNAL_ENDPOINT",
        "WAREHOUSE_NAME",
        "LAKEKEEPER_CATALOG_URI",
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "KAFKA_BOOTSTRAP_SERVERS",
        "KAFKA_TOPIC",
    }
    missing = sorted(key for key in required if not resolved.get(key))
    if missing:
        raise ValueError(f"Airflow runtime configuration is incomplete: {', '.join(missing)}")

    encrypted_connections = [
        connection_id
        for connection_id in connection_ids
        if BaseHook.get_connection(connection_id).is_encrypted
    ]
    if not {"platform_minio", "platform_postgres"}.issubset(encrypted_connections):
        raise ValueError("Expected MinIO and PostgreSQL passwords to be encrypted in Airflow")

    print(
        "PLATFORM runtime configuration resolved: "
        f"connections={','.join(connection_ids)}; "
        f"variables={','.join(variable_names)}; "
        f"encrypted_passwords={','.join(encrypted_connections)}; "
        f"resolved_keys={len(resolved)}"
    )


with DAG(
    dag_id="platform_runtime_config_check",
    description="Verify PLATFORM Airflow metadata and run a three-row Spark smoke query",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    is_paused_upon_creation=False,
    tags=["platform", "configuration", "validation"],
) as dag:
    verify = PythonOperator(
        task_id="verify_connections_and_variables",
        python_callable=verify_runtime_configuration,
    )
    smoke = PlatformSparkSubmitOperator(
        task_id="verify_spark_runtime",
        runtime_groups=("iceberg", "postgres", "kafka"),
        application="/opt/spark_jobs/measure/validate_spark_runtime.py",
        conn_id="spark_default",
        driver_memory="600m",
        executor_memory="512m",
        executor_cores=1,
        total_executor_cores=1,
        conf={
            "spark.eventLog.enabled": "true",
            "spark.eventLog.dir": "file:///spark-events",
        },
    )
    verify >> smoke
