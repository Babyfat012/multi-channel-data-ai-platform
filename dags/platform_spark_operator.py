"""Spark operator that fetches shared Airflow configuration only at task runtime."""

from __future__ import annotations

import os

from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from platform_datahub import report_task
from platform_runtime import resolve_runtime_env

from spark_jobs.lib.assertion_result import parse


class _Lookup(dict):
    """Dict that fetches a key on first read: `resolve_runtime_env` alone decides what it needs."""

    def __init__(self, fetch):
        """Keep the fetch function; nothing is looked up yet."""
        super().__init__()
        self._fetch = fetch

    def __missing__(self, key):
        """Fetch, cache and return the value for a key not read before."""
        self[key] = value = self._fetch(key)
        return value


class PlatformSparkSubmitOperator(SparkSubmitOperator):
    """Inject selected connection groups into spark-submit without templating secrets."""

    template_fields = tuple(
        field for field in SparkSubmitOperator.template_fields if field != "env_vars"
    )
    # The driver runs inside the Airflow container; executors call back to it by this host name.
    DEFAULT_CONF = {
        "spark.driver.host": "airflow",
        "spark.driver.bindAddress": "0.0.0.0",
        "spark.jars.ivy": "/opt/airflow/.ivy2",
    }

    def __init__(self, *, runtime_groups: tuple[str, ...] = ("iceberg",), conf=None, **kwargs):
        """Create a Spark task with the Airflow connection groups it needs; `conf` extends the defaults."""
        self.runtime_groups = runtime_groups
        super().__init__(conf={**self.DEFAULT_CONF, **(conf or {})}, **kwargs)
        self._check_results: list[dict] = []

    def _get_hook(self):
        """Hook whose spark-submit output also feeds `DATAHUB_ASSERTION` lines into `_check_results`."""
        hook = super()._get_hook()
        process = hook._process_spark_submit_log

        def lines(itr):
            for line in itr:
                try:
                    record = parse(line)
                except ValueError:
                    self.log.warning("Unreadable check result line: %s", line.strip())
                    record = None
                if record is not None:
                    self._check_results.append(record)
                yield line

        hook._process_spark_submit_log = lambda itr: process(lines(itr))
        return hook

    def execute(self, context):
        """Resolve credentials at execution time and restore operator state afterward."""

        def connection(conn_id):
            """Airflow Connection -> the plain fields `resolve_runtime_env` reads."""
            conn = BaseHook.get_connection(conn_id)
            return {
                "host": conn.host,
                "port": conn.port,
                "schema": conn.schema,
                "login": conn.login,
                "password": conn.password,
            }

        storage = os.environ.get("PLATFORM_STORAGE", "minio")
        runtime_env = resolve_runtime_env(
            _Lookup(connection), _Lookup(Variable.get), self.runtime_groups, storage=storage
        )
        original_env = self.env_vars
        self.env_vars = {
            "PYTHONPATH": os.environ.get("PYTHONPATH", "/opt"),
            **(original_env or {}),
            **runtime_env,
        }
        inherited_secrets = {
            key: os.environ.pop(key)
            for key in (
                "MINIO_ROOT_PASSWORD",
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "AWS_SESSION_TOKEN",
                "POSTGRES_PASSWORD",
                "LAKEKEEPER_ENCRYPTION_KEY",
            )
            if key in os.environ
        }
        self._check_results = []
        try:
            return super().execute(context)
        finally:
            self.env_vars = original_env
            os.environ.update(inherited_secrets)
            # PASS or FAIL: the validator printed its results before raising (DataHub shows failed runs too).
            report_task(self._check_results, context)
