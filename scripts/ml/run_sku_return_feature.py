"""Submit the Flink SQL stream feature job (flink_jobs/ml/sku_return_feature.sql) to the Flink cluster.

A RUNNING job with the same name is cancelled first, so a redeploy never leaves two jobs in parallel.
PostgreSQL credentials (only when the SQL has @@PG_*@@ placeholders): the environment, else Vault (VAULT_ADDR +
VAULT_TOKEN, as CI does), else .env on a manual run. Variant A (sku_return_feature_a.sql) needs none:

    FEATURE_SQL=flink_jobs/ml/sku_return_feature_a.sql python3 -m scripts.ml.run_sku_return_feature
"""

import json
import os
import urllib.request
from pathlib import Path

from scripts.common import ROOT, env, load_env, run, vault_secret

DEFAULT_SQL = "flink_jobs/ml/sku_return_feature.sql"
JOB_NAME = "ml-sku-return-24h"


def cancel_running(rest: str, name: str) -> None:
    """Cancel every RUNNING Flink job with this name through the Flink REST API."""
    with urllib.request.urlopen(f"{rest}/jobs/overview", timeout=10) as response:
        jobs = json.load(response)["jobs"]
    for job in jobs:
        if job["name"] == name and job["state"] == "RUNNING":
            request = urllib.request.Request(
                f"{rest}/jobs/{job['jid']}?mode=cancel", method="PATCH"
            )
            urllib.request.urlopen(request, timeout=10).close()
            print(f"cancelled old job {name} {job['jid']}")


def postgres_credentials() -> None:
    """Fill POSTGRES_* from Vault (CI) or .env (manual run) when the environment does not have them."""
    if os.environ.get("POSTGRES_PASSWORD"):
        return
    if os.environ.get("VAULT_TOKEN"):
        data = vault_secret("postgres", os.environ["VAULT_TOKEN"]) or {}
        os.environ.update(
            POSTGRES_USER=data.get("user", ""),
            POSTGRES_PASSWORD=data.get("password", ""),
            POSTGRES_DB=data.get("db", ""),
        )
    else:
        load_env(required=False)


def render(sql: str) -> str:
    """Fill the @@PG_*@@ placeholders; required only when the SQL actually has them."""
    if "@@PG_" not in sql:
        return sql
    values = {
        "@@PG_DB@@": env("POSTGRES_DB"),
        "@@PG_USER@@": env("POSTGRES_USER"),
        "@@PG_PASSWORD@@": env("POSTGRES_PASSWORD"),
    }
    for placeholder, value in values.items():
        sql = sql.replace(placeholder, value)
    return sql


def main() -> None:
    """Cancel the old job, then submit the SQL through sql-client inside the jobmanager (SQL via stdin)."""
    os.chdir(ROOT)
    postgres_credentials()
    sql = render(Path(os.environ.get("FEATURE_SQL") or DEFAULT_SQL).read_text())
    cancel_running(f"http://localhost:{os.environ.get('FLINK_UI_PORT', '8082')}", JOB_NAME)
    run(
        [
            "docker",
            "compose",
            "--profile",
            "flink",
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
