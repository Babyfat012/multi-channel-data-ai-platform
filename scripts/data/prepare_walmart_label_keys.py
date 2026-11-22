"""Build Walmart label join keys with the host's Python before Spark reads the Parquet.

The Airflow driver uses Python 3.11 while Spark workers use 3.10, so Python UDFs
do not run in this cluster. The script reads in batches and writes only four key columns.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from generator.channels.base import native_digits


def main() -> None:
    """Build the Walmart key bridge used to associate labels with Gold rows."""
    parser = argparse.ArgumentParser(description="Prepare Walmart label keys")
    parser.add_argument("--ground-truth", type=Path, default=Path("/data/raw/ground-truth"))
    parser.add_argument("--manifest", type=Path, default=Path("/data/raw/run_manifest.json"))
    parser.add_argument(
        "--output", type=Path, default=Path("out/ground-truth-bridge/walmart-keys.parquet")
    )
    args = parser.parse_args()

    manifest = json.loads(args.manifest.read_text())
    seed = int(manifest["config"]["seed"])
    source = ds.dataset(args.ground_truth / "order_line_keys" / "walmart", format="parquet")
    schema = pa.schema(
        [
            ("order_line_id", pa.string()),
            ("channel_order_id", pa.string()),
            ("channel_line_id", pa.string()),
            ("channel_secondary_order_id", pa.string()),
        ]
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    rows = 0
    with pq.ParquetWriter(args.output, schema=schema, compression="zstd") as writer:
        for batch in source.scanner(
            columns=[
                "order_line_id",
                "channel_order_id",
                "channel_line_id",
            ],
            batch_size=50_000,
        ).to_batches():
            order_line_ids = batch.column(0).to_pylist()
            secondary_ids = []
            for order_line_id in order_line_ids:
                if not order_line_id or "-" not in order_line_id:
                    raise ValueError(f"Invalid order_line_id: {order_line_id!r}")
                order_id = order_line_id.rsplit("-", 1)[0]
                secondary_ids.append(native_digits(seed, "walmart_co", order_id, 13))
            output_batch = pa.record_batch(
                [
                    batch.column(0),
                    batch.column(1),
                    batch.column(2),
                    pa.array(secondary_ids, type=pa.string()),
                ],
                schema=schema,
            )
            writer.write_batch(output_batch)
            rows += len(order_line_ids)
    print(f"Walmart keys: {rows:,} rows; seed={seed}; {args.output}")


if __name__ == "__main__":
    main()
