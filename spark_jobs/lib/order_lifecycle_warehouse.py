"""Load JDBC staging, reconcile against Gold and publish warehouse v2 in a single transaction."""

from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

TABLES = ("fact_order_lines", "label_return", "feat_sku_return_rate_90d")
MONEY = (
    "ordered_product_net_minor",
    "canceled_product_net_minor",
    "headline_product_net_minor",
    "product_refund_success_minor",
)
CHANNELS = {"amazon", "walmart", "shopee", "shopify", "tiktok_shop"}
LOCK_ID = 802521


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


def statements(path):
    """Read fixed migrations, strip comments and split statements that contain no semicolon literals."""
    return [
        s.strip() for s in re.sub(r"--[^\n]*", "", Path(path).read_text()).split(";") if s.strip()
    ]


def ddl_tables(sql_dir):
    """Get the unique table definitions from versioned migrations."""
    return {
        name: next(
            s
            for s in statements(Path(sql_dir) / "001_tables.sql")
            if s.startswith(f"CREATE TABLE warehouse_lifecycle.{name} (")
        )
        for name in TABLES
    }


def index_statements(sql_dir, tables=TABLES):
    """Select the indexes of exactly the tables being published."""
    return [
        s
        for s in statements(Path(sql_dir) / "002_indexes.sql")
        if any(f"ON warehouse_lifecycle.{t} (" in s for t in tables)
    ]


def should_publish(checks):
    """Only allow the swap when checks exist and every check explicitly succeeded."""
    return bool(checks) and all(value is True for value in checks.values())


def cutoff_utc(value):
    """Normalize a time-zone-aware cutoff to the naive UTC datetime Spark/JDBC use."""
    instant = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if instant.tzinfo is None:
        raise ValueError("cutoff must have a time zone")
    return instant.astimezone(timezone.utc).replace(tzinfo=None)


def table_metrics(cur, table):
    """Count actual staging rows in PostgreSQL and sum each amount separately per channel/currency."""
    cur.execute(f"SELECT count(*) FROM warehouse_lifecycle.{table}")
    result = {"rows": cur.fetchone()[0]}
    if table.startswith("fact_order_lines"):
        sums = ", ".join(f"sum({c}::numeric)" for c in MONEY)
        cur.execute(
            f"SELECT channel, currency, count(*), {sums} FROM warehouse_lifecycle.{table} "
            "GROUP BY channel, currency ORDER BY channel, currency"
        )
        result["money_by_channel_currency"] = [
            list(r[:3]) + [int(v) if v is not None else None for v in r[3:]] for r in cur.fetchall()
        ]
    return result


def source_metrics(frame, name):
    """Read Gold logical rows with a pinned snapshot, summing decimals to avoid bigint sum overflow."""
    from pyspark.sql import functions as F

    result = {"rows": frame.count()}
    if name == "fact_order_lines":
        groups = frame.groupBy("channel", "currency").agg(
            F.count("*").alias("rows"),
            *[F.sum(F.col(c).cast("decimal(38,0)")).alias(c) for c in MONEY],
        )
        result["money_by_channel_currency"] = [
            [r["channel"], r["currency"], r["rows"]]
            + [int(r[c]) if r[c] is not None else None for c in MONEY]
            for r in groups.orderBy("channel", "currency").collect()
        ]
    return result


def publish(cur, stages, token, sql_dir, checks):
    """Swap all validated staging tables; the caller commits/rolls back both DDL and ANALYZE."""
    if not should_publish(checks):
        raise ValueError("Reconciliation failed; not swapping")
    old_tables = []
    for name, stage in stages.items():
        cur.execute("SELECT to_regclass(%s)", (f"warehouse_lifecycle.{name}",))
        if cur.fetchone()[0] is not None:
            old = f"{name}__old_{token}"
            cur.execute(f"ALTER TABLE warehouse_lifecycle.{name} RENAME TO {old}")
            # Index names are schema-wide: free the names before re-applying 002.
            cur.execute(
                "SELECT indexname FROM pg_indexes WHERE schemaname=%s AND tablename=%s",
                ("warehouse_lifecycle", old),
            )
            for i, (idx,) in enumerate(cur.fetchall()):
                escaped = idx.replace('"', '""')
                digest = hashlib.sha256(name.encode()).hexdigest()[:8]
                cur.execute(
                    f'ALTER INDEX warehouse_lifecycle."{escaped}" RENAME TO old_{token}_{digest}_{i}'
                )
            old_tables.append(old)
        cur.execute(f"ALTER TABLE warehouse_lifecycle.{stage} RENAME TO {name}")
    for statement in index_statements(sql_dir, stages):
        cur.execute(statement)
    for name in stages:
        cur.execute(f"ANALYZE warehouse_lifecycle.{name}")
    for old in old_tables:
        cur.execute(f"DROP TABLE warehouse_lifecycle.{old}")


def export_frames(
    spark, frames, run_id, cutoff, receipt_path, sql_dir, dsn=None, jdbc=None, stage_transform=None
):
    """Load via JDBC then check/swap; the hook is only used by the harness to inject staging drift before reconciliation."""
    from pyspark.sql import functions as F

    if not frames or not set(frames).issubset(TABLES):
        raise ValueError("Invalid table list")
    token = hashlib.sha256(run_id.encode()).hexdigest()[:12]
    stages = {name: f"{name}__load_{token}" for name in frames}
    document = {"run_id": run_id, "cutoff": cutoff, "status": "failed", "checks": {}, "tables": {}}
    conn = connect(dsn)
    conn.autocommit = True
    cached = []
    created_stages = []
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(%s)", (LOCK_ID,))
            cur.execute("SET timezone = 'UTC'")
            cur.execute("CREATE SCHEMA IF NOT EXISTS warehouse_lifecycle")
            definitions = ddl_tables(sql_dir)
            for name, original in frames.items():
                frame = original.cache()
                cached.append(frame)
                expected = source_metrics(frame, name)
                checks = document["checks"]
                checks[f"{name}.nonempty"] = expected["rows"] > 0
                checks[f"{name}.generation"] = (
                    not frame.filter(~F.col("generation_id").eqNullSafe(F.lit(run_id)))
                    .limit(1)
                    .count()
                )
                if name == "label_return":
                    checks["labels.cutoff"] = (
                        not frame.filter(~F.col("cutoff").eqNullSafe(F.lit(cutoff_utc(cutoff))))
                        .limit(1)
                        .count()
                    )
                if name == "fact_order_lines":
                    checks["fact.five_channels"] = {
                        r[0] for r in frame.select("channel").distinct().collect()
                    } == CHANNELS
                    checks["fact.before_cutoff"] = (
                        not frame.filter(
                            F.col("created_at").isNull()
                            | (F.col("created_at") >= F.lit(cutoff_utc(cutoff)))
                        )
                        .limit(1)
                        .count()
                    )
                stage = stages[name]
                cur.execute(
                    definitions[name].replace(
                        f"warehouse_lifecycle.{name}", f"warehouse_lifecycle.{stage}", 1
                    )
                )
                created_stages.append(stage)
                options = jdbc or dict(
                    url=f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'postgres')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'sources')}",
                    user=os.environ["POSTGRES_USER"],
                    password=os.environ["POSTGRES_PASSWORD"],
                )
                frame.write.format("jdbc").options(
                    **options,
                    dbtable=f"warehouse_lifecycle.{stage}",
                    driver="org.postgresql.Driver",
                    batchsize="5000",
                ).mode("append").save()
                if stage_transform:
                    stage_transform(cur, name, stage)
                actual = table_metrics(cur, stage)
                document["tables"][name] = {"gold": expected, "postgres": actual}
                checks[f"{name}.rows"] = expected["rows"] == actual["rows"]
                if name == "fact_order_lines":
                    checks["fact.money_by_channel_currency"] = (
                        expected["money_by_channel_currency"] == actual["money_by_channel_currency"]
                    )
            if not should_publish(document["checks"]):
                raise ValueError("Reconciliation failed; serving tables unchanged")
            conn.autocommit = False
            try:
                publish(cur, stages, token, sql_dir, document["checks"])
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.autocommit = True
            document["status"] = "passed"
    except Exception as error:
        # Do not write the JDBC exception string: it may contain the URL/connection details.
        document["error_type"] = type(error).__name__
        raise
    finally:
        # Only drop staging created by this call; never touch pre-existing staging.
        try:
            conn.rollback()
            conn.autocommit = True
            with conn.cursor() as cur:
                for stage in created_stages:
                    cur.execute(f"DROP TABLE IF EXISTS warehouse_lifecycle.{stage}")
        except Exception as cleanup_error:
            document["cleanup_error_type"] = type(cleanup_error).__name__
        for frame in cached:
            frame.unpersist()
        conn.close()  # closing the session releases the advisory lock even on error
        target = Path(receipt_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(document, ensure_ascii=False, indent=2) + "\n")
    return document
