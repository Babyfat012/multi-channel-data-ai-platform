"""Deploy the always-on Flink job counting new orders per 5 minutes into Postgres (flink_jobs/ops/order_counts_window.sql).

First makes sure Postgres has what the job and Grafana need (re-runnable): DB `platform_ops`, table
`ops.order_counts_window`, and read-only role `grafana_ro` (SELECT on schema ops only) when GRAFANA_RO_PASSWORD
is set. Then redeploys the job from a savepoint like run_bronze_stream_jobs (FRESH=1 skips it).

    python3 -m scripts.data.run_ops_order_counts
"""

from __future__ import annotations

import os
import re
import shlex

from scripts.common import ROOT, env, load_env, run
from scripts.data.run_bronze_stream_jobs import render, stop_with_savepoint

SQL = ROOT / "flink_jobs/ops/order_counts_window.sql"
JOB_NAME = "ops-order-counts"
DB = "platform_ops"
TABLE_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.order_counts_window (
  channel text NOT NULL,
  window_start timestamp NOT NULL,
  window_end timestamp NOT NULL,
  orders bigint NOT NULL,
  PRIMARY KEY (channel, window_start)
);
"""
# :'pw' is a psql variable set on stdin (never on the command line, where `ps` shows it); psql quotes it.
ROLE_DDL = """
SELECT format('CREATE ROLE grafana_ro LOGIN PASSWORD %L', :'pw')
  WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'grafana_ro') \\gexec
ALTER ROLE grafana_ro PASSWORD :'pw';
REVOKE ALL ON DATABASE platform_ops FROM PUBLIC;
GRANT CONNECT ON DATABASE platform_ops TO grafana_ro;
GRANT USAGE ON SCHEMA ops TO grafana_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA ops TO grafana_ro;
"""


def psql(compose: list[str], db: str, sql: str) -> str:
    """Run SQL from stdin in the postgres container as the platform user."""
    user = env("POSTGRES_USER")
    return run(
        [
            *compose,
            "exec",
            "-T",
            "postgres",
            "psql",
            "-v",
            "ON_ERROR_STOP=1",
            "-qtA",
            "-U",
            user,
            "-d",
            db,
        ],
        stdin=sql,
        capture=True,
    )


def prepare_postgres(compose: list[str]) -> None:
    """Create the DB, table and (if a password is given) the Grafana read-only role."""
    exists = psql(compose, env("POSTGRES_DB"), f"SELECT 1 FROM pg_database WHERE datname = '{DB}'")
    if not exists.strip():
        psql(compose, env("POSTGRES_DB"), f"CREATE DATABASE {DB}")
    psql(compose, DB, TABLE_DDL)
    password = os.environ.get("GRAFANA_RO_PASSWORD")
    if password:
        # Same rule as postgres_password in Ansible, so `\set pw` needs no escaping.
        if not re.fullmatch(r"[A-Za-z0-9_-]{16,}", password):
            raise ValueError("GRAFANA_RO_PASSWORD must be 16+ characters of A-Z a-z 0-9 _ -")
        psql(compose, DB, f"\\set pw '{password}'\n" + ROLE_DDL)
    else:
        print("GRAFANA_RO_PASSWORD not set: role grafana_ro not created/updated")


def main() -> None:
    """Prepare Postgres, stop the old job with a savepoint, submit the rendered SQL through sql-client."""
    os.chdir(ROOT)
    load_env(required=False, override=False)
    compose = shlex.split(
        os.environ.get("COMPOSE", "docker compose --profile stream --profile flink")
    )
    prepare_postgres(compose)
    base = f"http://localhost:{os.environ.get('FLINK_UI_PORT', '8082')}"
    savepoint = None if os.environ.get("FRESH") == "1" else stop_with_savepoint(base, JOB_NAME)
    sql = render(
        SQL.read_text(),
        {
            "SAVEPOINT": f"SET 'execution.savepoint.path' = '{savepoint}';" if savepoint else "",
            "KAFKA": "kafka:9092",
            "JDBC_URL": f"jdbc:postgresql://postgres:5432/{DB}",
            "PG_USER": env("POSTGRES_USER"),
            "PG_PASSWORD": env("POSTGRES_PASSWORD"),
        },
    )
    run(
        [
            *compose,
            "exec",
            "-T",
            "flink-jobmanager",
            "bash",
            "-c",
            'f=$(mktemp); cat > "$f"; ./bin/sql-client.sh -f "$f"; rm -f "$f"',
        ],
        stdin=sql,
    )


if __name__ == "__main__":
    main()
