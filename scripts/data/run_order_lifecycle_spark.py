"""Run one order-lifecycle v2 Spark job (Bronze / Silver) in the Airflow image, connected to Lakekeeper/MinIO.

Requires: docker compose --profile catalog up -d postgres minio lakekeeper. The fixture is mounted read-only;
Iceberg/PostgreSQL jars come from the ivy cache in the platform_airflow-data volume (no network downloads).
Walmart via JDBC: load it first with scripts/data/load_order_lifecycle_walmart.py, then pass
--walmart-jdbc walmart_lifecycle.orders.

    FIXTURE=out/<fixture>/default python3 -m scripts.data.run_order_lifecycle_spark \\
        /opt/spark_jobs/bronze/ingest_order_lifecycle.py --root /data/order_lifecycle --run-id X
    RECEIPTS=out/source-v2-receipts python3 -m scripts.data.run_order_lifecycle_spark \\
        /opt/spark_jobs/silver/build_order_lifecycle.py --run-id Y --receipt /receipts/silver.json
"""

import os
import sys
from pathlib import Path

from scripts.common import ROOT, env, load_env

JARS = "/ivy/.ivy2/jars"
JAR_FILES = [
    "org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.11.0.jar",
    "org.apache.iceberg_iceberg-aws-bundle-1.11.0.jar",
    "org.postgresql_postgresql-42.7.4.jar",
]


def docker_command(fixture: Path, receipts: Path, job_args: list[str]) -> list[str]:
    """docker run … spark-submit <job_args>; credentials are passed by name so values stay out of argv."""
    secrets = [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "POSTGRES_DB",
    ]
    return [
        "docker",
        "run",
        "--rm",
        "--network",
        "platform_default",
        "--user",
        f"{os.getuid()}:0",
        *[arg for name in secrets for arg in ("-e", name)],
        "-e",
        "AWS_REGION=local-01",
        "-e",
        "SPARK_LOCAL_HOSTNAME=localhost",
        "-e",
        "POSTGRES_HOST=postgres",
        "-e",
        "POSTGRES_PORT=5432",
        "-v",
        "platform_airflow-data:/ivy:ro",
        "-v",
        f"{ROOT}/spark_jobs:/opt/spark_jobs:ro",
        "-v",
        f"{ROOT}/contracts:/opt/contracts:ro",
        "-v",
        f"{fixture.resolve()}:/data/order_lifecycle:ro",
        "-v",
        f"{ROOT}/out/spark-events:/spark-events",
        "-v",
        f"{receipts.resolve()}:/receipts",
        "platform-airflow:2.10.5-spark3.5.3",
        # The Airflow entrypoint runs `airflow <args>` unless the first arg is bash/python ⇒ go through bash.
        "bash",
        "-c",
        'exec spark-submit "$@"',
        "spark-submit",
        "--master",
        "local[2]",
        "--driver-memory",
        "1g",
        "--conf",
        "spark.jars.ivy=/tmp/.ivy2",
        "--jars",
        ",".join(f"{JARS}/{jar}" for jar in JAR_FILES),
        *job_args,
    ]


def main() -> None:
    """Replace this process with the container run, so its exit code is the job's."""
    os.chdir(ROOT)
    fixture = Path(env("FIXTURE"))
    receipts = Path(os.environ.get("RECEIPTS", "out/source-v2-receipts"))
    receipts.mkdir(parents=True, exist_ok=True)
    (ROOT / "out/spark-events").mkdir(parents=True, exist_ok=True)
    load_env()
    os.environ["AWS_ACCESS_KEY_ID"] = env("MINIO_ROOT_USER")
    os.environ["AWS_SECRET_ACCESS_KEY"] = env("MINIO_ROOT_PASSWORD")
    cmd = docker_command(fixture, receipts, sys.argv[1:])
    os.execvp(cmd[0], cmd)


if __name__ == "__main__":
    main()
