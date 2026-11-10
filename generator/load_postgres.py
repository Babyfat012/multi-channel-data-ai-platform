"""Load `pg-load/` into PostgreSQL, simulating another department's system.

The `batch` command only writes files. Rows meant for PostgreSQL live in `pg-load/` (a local
directory or MinIO), and this module puts them into tables:

    walmart.orders   one row per pull of an order from Walmart by that system, API body kept as is
    seller.skus      the Seller's SKU catalog

`walmart.orders` deliberately has no primary key: overlapping exports create duplicates by
`purchase_order_id`, and deduplication belongs to the downstream pipeline (Spark), not the
source system. The `export_date` column comes from the directory name, so DP1 can pull day by day.

Each run truncates the tables and reloads them in one transaction, so rerunning
any number of times gives the same result. Data goes in with `COPY`, much faster than
row-by-row `INSERT`.

`sample_days` loads an evenly spread set of export days instead of the whole range. Two reasons:

  * Cheap trial runs. The real source is 13 GB / 180 days; 20 evenly spread days are only ~1.4 GB. The
    PostgreSQL volume sits on the same `/` partition as the source files, so loading the whole range uses
    the space twice on an already tight partition.
  * Airflow (DP1) will need to pull day by day anyway — the `export_date` column already exists for
    that. So this is required work, not a stopgap for one trial run.

`seller.skus` is not split by day, so this flag does not apply to it; see `filter_to_sample_days`.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any

from generator.day_selection import filter_to_sample_days
from generator.storage import OutputStore

DDL = """
CREATE SCHEMA IF NOT EXISTS walmart;
CREATE SCHEMA IF NOT EXISTS seller;

CREATE TABLE IF NOT EXISTS walmart.orders (
    purchase_order_id TEXT        NOT NULL,
    fetched_at        TIMESTAMPTZ NOT NULL,
    export_date       DATE        NOT NULL,
    raw_payload       JSONB       NOT NULL
);

CREATE TABLE IF NOT EXISTS seller.skus (
    sku            TEXT PRIMARY KEY,
    name           TEXT          NOT NULL,
    category       TEXT          NOT NULL,
    base_price_usd NUMERIC(10,2) NOT NULL,
    unit_cost_usd  NUMERIC(10,2) NOT NULL
);
"""


@dataclass(frozen=True)
class TableLoad:
    """How to load a table: which files under `pg-load/` to read, and how each record becomes a row."""

    table: str
    columns: tuple[str, ...]
    pattern: str
    to_row: Callable[[dict[str, Any], str], tuple[Any, ...]]


def _walmart_order_row(record: dict[str, Any], path: str) -> tuple[Any, ...]:
    """Turn a Walmart record into a row; `export_date` comes from the file's directory."""
    export_date = path.split("export_date=", 1)[1].split("/", 1)[0]
    return (
        record["purchase_order_id"],
        record["fetched_at"],
        export_date,
        json.dumps(record["raw_payload"], ensure_ascii=False, separators=(",", ":")),
    )


def _sku_row(record: dict[str, Any], path: str) -> tuple[Any, ...]:
    """Turn a SKU into a row of `seller.skus`."""
    return (
        record["sku"],
        record["name"],
        record["category"],
        record["base_price_usd"],
        record["unit_cost_usd"],
    )


TABLES = (
    TableLoad(
        "walmart.orders",
        ("purchase_order_id", "fetched_at", "export_date", "raw_payload"),
        "pg-load/walmart.orders/export_date=*/part-*.jsonl",
        _walmart_order_row,
    ),
    TableLoad(
        "seller.skus",
        ("sku", "name", "category", "base_price_usd", "unit_cost_usd"),
        "pg-load/seller.skus/part-*.jsonl",
        _sku_row,
    ),
)


def iter_rows(
    store: OutputStore, load: TableLoad, sample_days: int = 0
) -> Iterator[tuple[Any, ...]]:
    """Read the files of `load` under `store` and yield rows ready for `COPY`.

    With `sample_days > 0`, only files of N evenly spread export days are read. The default 0 = read all,
    so the old behavior is unchanged.
    """
    for path in filter_to_sample_days(store.glob(load.pattern), sample_days):
        for record in store.read_jsonl(path):
            yield load.to_row(record, path)


def load_postgres(source_root: str, dsn: str, sample_days: int = 0) -> dict[str, int]:
    """Create tables if missing, delete old data, then reload. Return row counts per table.

    `sample_days > 0` loads only N evenly spread export days. TRUNCATE still empties the tables first, so
    the result is always "exactly those N days and nothing else", however many times it reruns.
    """
    import psycopg  # lazy import so tests of the file reading do not need the PostgreSQL library

    store = OutputStore(source_root)
    counts: dict[str, int] = {}
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(DDL)
        for load in TABLES:
            cur.execute(f"TRUNCATE {load.table}")
            n = 0
            with cur.copy(f"COPY {load.table} ({', '.join(load.columns)}) FROM STDIN") as copy:
                for row in iter_rows(store, load, sample_days):
                    copy.write_row(row)
                    n += 1
            counts[load.table] = n
    return counts
