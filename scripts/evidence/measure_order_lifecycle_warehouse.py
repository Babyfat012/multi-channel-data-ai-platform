"""Measure EXPLAIN JSON on the same data, PK only and then with indexes; roll back all benchmark DDL."""

from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path

from spark_jobs.lib.order_lifecycle_warehouse import (
    LOCK_ID,
    TABLES,
    connect,
    cutoff_utc,
    index_statements,
)


def queries():
    """Three representative queries, always keeping grain/currency and cutoff/generation when joining."""
    return {
        "headline_30d": """SELECT created_at::date AS day, channel, currency,
            sum(headline_product_net_minor::numeric) AS headline_minor
            FROM warehouse_lifecycle.fact_order_lines
            WHERE created_at >= %(cutoff)s::timestamp - interval '30 days'
              AND created_at < %(cutoff)s::timestamp
            GROUP BY created_at::date, channel, currency""",
        "return_rate_at_cutoff": """SELECT f.dataset_version, f.channel, f.seller_sku,
            count(*) AS labeled_count, avg(l.label::numeric) AS return_rate
            FROM warehouse_lifecycle.fact_order_lines f JOIN warehouse_lifecycle.label_return l
              ON f.dataset_version=l.dataset_version AND f.line_id=l.line_id
             AND f.generation_id=l.generation_id
            WHERE l.cutoff=%(cutoff)s::timestamp AND l.state='labeled'
              AND f.seller_sku=%(sku)s
            GROUP BY f.dataset_version, f.channel, f.seller_sku""",
        "feature_pit": """SELECT dataset_version, line_id, seller_sku, event_timestamp, created,
            return_or_refund_rate_90d, labeled_count_90d, positive_count_90d
            FROM warehouse_lifecycle.feat_sku_return_rate_90d
            WHERE dataset_version=%(dataset)s AND seller_sku=%(sku)s
              AND event_timestamp <= %(event_timestamp)s::timestamp
              AND created <= %(as_of)s::timestamp
            ORDER BY event_timestamp DESC, created DESC, line_id LIMIT 1""",
    }


def summarize(raw):
    """Take buffers at the root node (already includes children) to avoid double counting."""
    plan = raw[0]["Plan"]
    return {
        "root_node": plan["Node Type"],
        "rows": plan["Actual Rows"],
        "shared_read_blocks": plan.get("Shared Read Blocks", 0),
        "shared_hit_blocks": plan.get("Shared Hit Blocks", 0),
        "shared_total_blocks": plan.get("Shared Read Blocks", 0) + plan.get("Shared Hit Blocks", 0),
        "plan": raw,
    }


def benchmark(conn, sql_dir, parameters, repeats=5):
    """Lock writers, measure two phases in the same transaction/snapshot, then roll back the indexes too."""
    if repeats < 5:
        raise ValueError("At least 5 measurements are required")
    result = {
        "repeats": repeats,
        "parameters": {k: str(v) for k, v in parameters.items()},
        "phases": {},
    }
    # The advisory lock is taken before the snapshot; LOCK SHARE blocks INSERT/UPDATE from other sessions.
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(%s)", (LOCK_ID,))
            conn.autocommit = False
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            cur.execute("SET LOCAL timezone = 'UTC'")
            cur.execute(
                "LOCK TABLE "
                + ", ".join(f"warehouse_lifecycle.{t}" for t in TABLES)
                + " IN SHARE MODE"
            )
            cur.execute(
                "SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid "
                "JOIN pg_class t ON t.oid=i.indrelid JOIN pg_namespace n ON n.oid=t.relnamespace "
                "WHERE n.nspname='warehouse_lifecycle' AND t.relname = ANY(%s) AND NOT i.indisprimary",
                (list(TABLES),),
            )
            for (name,) in cur.fetchall():
                cur.execute('DROP INDEX warehouse_lifecycle."' + name.replace('"', '""') + '"')
            for table in TABLES:
                cur.execute(f"ANALYZE warehouse_lifecycle.{table}")
            for phase in ("pk_only", "indexed"):
                if phase == "indexed":
                    for statement in index_statements(sql_dir):
                        cur.execute(statement)
                results = {}
                for name, query in queries().items():
                    samples = []
                    for _ in range(repeats):
                        cur.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + query, parameters)
                        samples.append(summarize(cur.fetchone()[0]))
                    results[name] = {
                        "samples": samples,
                        "root_nodes": sorted({s["root_node"] for s in samples}),
                        "median": {
                            key: statistics.median(s[key] for s in samples)
                            for key in (
                                "rows",
                                "shared_read_blocks",
                                "shared_hit_blocks",
                                "shared_total_blocks",
                            )
                        },
                    }
                result["phases"][phase] = results
        return result
    finally:
        conn.rollback()
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s)", (LOCK_ID,))


def main(argv=None):
    """Take explicit cutoff/SKU/timestamps, save all plans and medians as JSON."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--cutoff", required=True)
    parser.add_argument("--sku", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--event-timestamp", required=True)
    parser.add_argument("--as-of", required=True, help="Time at which feature.created is known")
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument(
        "--sql-dir",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "sql/warehouse_lifecycle",
    )
    parser.add_argument("--repeats", type=int, default=5)
    args = parser.parse_args(argv)
    params = {
        "cutoff": cutoff_utc(args.cutoff),
        "sku": args.sku,
        "dataset": args.dataset,
        "event_timestamp": cutoff_utc(args.event_timestamp),
        "as_of": cutoff_utc(args.as_of),
    }
    conn = connect()
    try:
        result = benchmark(conn, args.sql_dir, params, args.repeats)
        args.receipt.parent.mkdir(parents=True, exist_ok=True)
        args.receipt.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
