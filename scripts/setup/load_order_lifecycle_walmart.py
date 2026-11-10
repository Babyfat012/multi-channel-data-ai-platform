"""Load the v2 source's bootstrap Walmart Orders into PostgreSQL `walmart_lifecycle.orders` (simulating another department's DB).

After this step the job `bronze/ingest_order_lifecycle.py --walmart-jdbc walmart_lifecycle.orders` reads Walmart via JDBC,
exactly at the boundary the manifest declares. The v1 table `walmart.orders` is not touched.

`raw_payload` is stored as TEXT, not JSONB: JSONB reorders keys and strips whitespace, i.e. the DB "fixes"
the original string before Bronze copies it. Reloading the same dataset_version deletes then inserts in ONE
transaction, so reruns do not duplicate rows.

    .venv/bin/python scripts/setup/load_order_lifecycle_walmart.py --root out/<fixture>/default
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import NamedTuple

DDL = """
CREATE SCHEMA IF NOT EXISTS walmart_lifecycle;
CREATE TABLE IF NOT EXISTS walmart_lifecycle.orders (
    dataset_version   TEXT        NOT NULL,
    purchase_order_id TEXT        NOT NULL,
    fetched_at        TIMESTAMPTZ NOT NULL,
    raw_payload       TEXT        NOT NULL,
    PRIMARY KEY (dataset_version, purchase_order_id)
);
"""


class Row(NamedTuple):
    """One Walmart Order ready to insert."""

    dataset_version: str
    purchase_order_id: str
    fetched_at: str
    raw_payload: str


def bootstrap_rows(root: Path) -> list[Row]:
    """Read the Walmart bootstrap file per the manifest; the payload keeps the file's exact canonical JSON string."""
    manifest = json.loads((root / "manifest.json").read_text())
    files = [f for f in manifest["files"] if f["feed"] == "walmart.orders"]
    if len(files) != 1:
        raise ValueError(
            f"Expected exactly one walmart.orders file in the manifest, got {len(files)}"
        )
    rows = []
    for line in (root / files[0]["path"]).read_text(encoding="utf-8").splitlines():
        record = json.loads(line)
        payload = json.dumps(
            record["raw_payload"],
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        rows.append(
            Row(
                manifest["dataset_version"],
                record["purchase_order_id"],
                record["fetched_at"],
                payload,
            )
        )
    if len(rows) != files[0]["rows"]:
        raise ValueError("Walmart file row count differs from the manifest")
    return rows


def load(rows: list[Row], settings: dict[str, str]) -> int:
    """Replace all rows of this dataset_version in one transaction; return the number of rows inserted."""
    import psycopg  # late import so the file-reading tests do not need the PostgreSQL library

    with (
        psycopg.connect(
            host="127.0.0.1",
            port=int(settings.get("POSTGRES_PORT", "5432")),
            dbname=settings["POSTGRES_DB"],
            user=settings["POSTGRES_USER"],
            password=settings["POSTGRES_PASSWORD"],
        ) as conn,
        conn.cursor() as cur,
    ):
        cur.execute(DDL)
        cur.execute(
            "DELETE FROM walmart_lifecycle.orders WHERE dataset_version = %s",
            (rows[0].dataset_version,),
        )
        cur.executemany(
            "INSERT INTO walmart_lifecycle.orders (dataset_version, purchase_order_id, fetched_at, raw_payload) "
            "VALUES (%s, %s, %s, %s)",
            rows,
        )
        cur.execute(
            "SELECT count(*) FROM walmart_lifecycle.orders WHERE dataset_version = %s",
            (rows[0].dataset_version,),
        )
        return cur.fetchone()[0]


def main() -> int:
    """Load the Walmart bootstrap for one fixture variant."""
    from scripts.setup.setup_order_snapshots_debezium import load_settings

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    args = parser.parse_args()
    rows = bootstrap_rows(args.root)
    count = load(rows, load_settings(args.env_file))
    print(
        f"walmart_lifecycle.orders: dataset_version={rows[0].dataset_version}, file={len(rows)}, DB={count}"
    )
    return 0 if count == len(rows) else 1


if __name__ == "__main__":
    sys.exit(main())
