"""Copy the last 30 days of the five-Channel Gold fact to the PostgreSQL warehouse, with an index.

The warehouse keeps the "hot" part for fast per-SKU lookups; the full history is in Iceberg. Few columns are kept
to fit the EBS disk. Write-audit-publish (gap D8): load a staging table, reconcile row counts per Channel with Gold
and index it, then swap it in with one transaction. Readers see the old table or the new one, never an empty or
half-loaded one, and a failed reconciliation leaves the serving table untouched. Cost: the disk holds both copies
during the load.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

from spark_jobs.lib.assertion_result import emit
from spark_jobs.lib.iceberg_session import CATALOG, build_spark
from spark_jobs.lib.label_channels import count_mismatches

IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
SCHEMA = "warehouse"


def connect(dsn=None):
    """Use whichever driver is available: psycopg in the venv or psycopg2 in the Airflow image."""
    try:
        import psycopg as driver
    except ImportError:
        import psycopg2 as driver
    if dsn:
        return driver.connect(dsn)
    return driver.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "sources"),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def cutoff_utc(value):
    """Normalize a time-zone-aware cutoff to the naive UTC datetime Spark/JDBC use."""
    instant = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if instant.tzinfo is None:
        raise ValueError("cutoff must have a time zone")
    return instant.astimezone(timezone.utc).replace(tzinfo=None)


def drop_stale_stages(cur, table: str) -> None:
    """Drop staging left by a run killed before its cleanup (driver OOM): each one holds a full copy on a tight disk.
    Safe because the DAG allows one run at a time; a parallel manual run would fail loudly, not publish wrong data."""
    cur.execute(
        "SELECT tablename FROM pg_tables WHERE schemaname = %s AND tablename LIKE %s",
        (SCHEMA, f"{table}\\_\\_load\\_%"),
    )
    for (stage,) in cur.fetchall():
        cur.execute(f"DROP TABLE {SCHEMA}.{stage}")


def lock_not_available(error: Exception) -> bool:
    """SQLSTATE 55P03 (lock_timeout hit), for both drivers connect() may use (psycopg: sqlstate, psycopg2: pgcode)."""
    return "55P03" in (getattr(error, "sqlstate", None), getattr(error, "pgcode", None))


def publish(
    conn,
    table: str,
    stage: str,
    gold: dict[str, int],
    lock_timeout: str = "300s",
    attempts: int = 3,
    pause: float = 5.0,
) -> dict[str, int]:
    """Reconcile staging with Gold, index it, then swap it in atomically. Raises before touching the serving
    table when a Channel is short, missing or empty. Returns the per-Channel counts loaded.

    The swap waits at most `lock_timeout` for its lock, `attempts` times: Postgres queues new readers BEHIND a
    waiting ACCESS EXCLUSIVE request, so one long query plus a waiting swap would freeze every reader. Giving up
    frees that queue; after the last attempt the job fails and readers keep the old table."""
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"SELECT channel, count(*) FROM {SCHEMA}.{stage} GROUP BY channel")
        loaded = dict(cur.fetchall())
        bad = count_mismatches(gold, loaded)
        emit(
            f"{SCHEMA}.{table}",
            not bad,
            {
                "source_rows": sum(gold.values()),
                "dataset_rows": sum(loaded.values()),
                "bad_channel_counts": len(bad),
            },
            "30-day warehouse matches Gold per Channel (all 5 Channels) before creating the index",
        )
        if bad:
            raise ValueError(f"Warehouse staging differs from Gold or a Channel is empty: {bad}")
        # Built on staging, so readers of the serving table never wait for it.
        cur.execute(f"CREATE INDEX ON {SCHEMA}.{stage} (seller_sku)")
        cur.execute(f"ANALYZE {SCHEMA}.{stage}")
    # Postgres DDL is transactional: DROP + RENAME commit together. A reader blocks for the moment of the swap
    # (ACCESS EXCLUSIVE lock), then resolves the name again and reads the new table.
    for attempt in range(1, attempts + 1):
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                cur.execute(f"SET LOCAL lock_timeout = '{lock_timeout}'")  # this transaction only
                cur.execute(f"DROP TABLE IF EXISTS {SCHEMA}.{table}")
                cur.execute(f"ALTER TABLE {SCHEMA}.{stage} RENAME TO {table}")
            conn.commit()
            return loaded
        except Exception as error:
            conn.rollback()
            if not lock_not_available(error) or attempt == attempts:
                raise
            print(f"swap attempt {attempt}/{attempts}: lock busy > {lock_timeout}, retrying")
            time.sleep(pause)
        finally:
            conn.autocommit = True
    raise ValueError("attempts must be ≥ 1")


def main(argv: list[str] | None = None) -> int:
    """Export the recent Gold window to PostgreSQL, reconcile per Channel, then index it."""
    parser = argparse.ArgumentParser(
        description="Five-Channel Gold (30 days) → PostgreSQL warehouse"
    )
    parser.add_argument("--source-table", default="fact_order_lines_dp2")
    parser.add_argument("--table", default="fact_order_lines_30d_dp2")
    parser.add_argument("--cutoff", required=True)
    parser.add_argument("--days", type=int, default=30)
    args = parser.parse_args(argv)
    for value in (args.source_table, args.table):
        if not IDENTIFIER.fullmatch(value):
            parser.error(f"Invalid table name: {value!r}")
    from pyspark.sql import (
        functions as F,
    )  # here: publish/drop_stale_stages are tested without Spark

    end = cutoff_utc(args.cutoff)
    start = end - timedelta(days=args.days)

    spark = build_spark("export-gold-warehouse", shuffle_partitions=32)
    try:
        rows = (
            spark.table(f"{CATALOG}.gold.{args.source_table}")
            .filter((F.col("created_at") >= F.lit(start)) & (F.col("created_at") < F.lit(end)))
            .select(
                "order_line_key",
                "channel",
                "channel_order_id",
                "created_at",
                "seller_sku",
                "line_net_amount",
                "currency",
            )
            .cache()
        )
        gold = {r["channel"]: r["count"] for r in rows.groupBy("channel").count().collect()}
        host = os.environ.get("POSTGRES_HOST", "postgres")
        port = os.environ.get("POSTGRES_PORT", "5432")
        database = os.environ.get("POSTGRES_DB", "sources")
        # Postgres names stop at 63 characters: table (≤ 31 here) + "__load_" + 12 hex.
        stage = f"{args.table}__load_{uuid.uuid4().hex[:12]}"
        options = {
            "url": f"jdbc:postgresql://{host}:{port}/{database}",
            "dbtable": f"{SCHEMA}.{stage}",
            "user": os.environ["POSTGRES_USER"],
            "password": os.environ["POSTGRES_PASSWORD"],
            "driver": "org.postgresql.Driver",
            "batchsize": "5000",
        }
        conn = connect()
        conn.autocommit = True
        try:
            # A fresh Postgres (AWS) has no schema yet: JDBC does not create schemas, only tables.
            with conn.cursor() as cur:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
                drop_stale_stages(cur, args.table)
            # errorifexists: a fresh name every run, never write into someone else's table.
            rows.repartition(4, "seller_sku").write.format("jdbc").options(**options).mode(
                "errorifexists"
            ).save()
            loaded = publish(conn, args.table, stage, gold)
        finally:
            # After a swap the stage name no longer exists; after a failure this frees the disk.
            conn.rollback()
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {SCHEMA}.{stage}")
            conn.close()
        print(
            f"Published {SCHEMA}.{args.table} [{start} → {end}) + index seller_sku: "
            f"gold={gold} postgres={loaded}"
        )
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
