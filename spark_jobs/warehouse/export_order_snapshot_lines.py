"""Copy the last 30 days of the five-Channel Gold fact to the PostgreSQL warehouse, with an index.

The warehouse keeps the "hot" part for fast per-SKU lookups; the full history is in Iceberg. Few columns are kept
to fit the EBS disk. After writing: reconcile row counts per Channel with Gold, then create the index.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import timedelta

from pyspark.sql import functions as F

from spark_jobs.lib.iceberg_session import CATALOG, build_spark
from spark_jobs.lib.label_channels import count_mismatches
from spark_jobs.lib.order_lifecycle_warehouse import connect, cutoff_utc

IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


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
        target = f"warehouse.{args.table}"
        options = {
            "url": f"jdbc:postgresql://{host}:{port}/{database}",
            "dbtable": target,
            "user": os.environ["POSTGRES_USER"],
            "password": os.environ["POSTGRES_PASSWORD"],
            "driver": "org.postgresql.Driver",
            "batchsize": "5000",
        }
        # A fresh Postgres (AWS) has no schema yet: JDBC does not create schemas, only tables.
        conn = connect()
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS warehouse")
        conn.close()
        # Limitation: overwrites directly, so the table is missing/empty during reload; for continuous reads use staging +
        # rename like spark_jobs/lib/order_lifecycle_warehouse.py.
        rows.repartition(4, "seller_sku").write.format("jdbc").options(**options).mode(
            "overwrite"
        ).save()
        conn = connect()
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT channel, count(*) FROM {target} GROUP BY channel")
                loaded = dict(cur.fetchall())
                bad = count_mismatches(gold, loaded)
                print(f"Warehouse {target} [{start} → {end}): gold={gold} postgres={loaded}")
                if bad:
                    raise ValueError(f"Warehouse differs from Gold or a Channel is empty: {bad}")
                cur.execute(f"CREATE INDEX ON {target} (seller_sku)")
                cur.execute(f"ANALYZE {target}")
        finally:
            conn.close()
        print(f"Warehouse written + index seller_sku: {target}; rows={sum(loaded.values())}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
