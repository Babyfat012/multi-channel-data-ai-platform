"""Command-line entry point of the generator.

Examples:
    uv run python -m generator batch --config configs/small.yaml
    uv run python -m generator batch --config configs/default.yaml --output s3://platform
    uv run python -m generator load-postgres --source s3://platform --dsn postgresql://platform:platform@localhost:5432/sources
    uv run python -m generator load-postgres --source /data/raw --sample-days 20 --dsn postgresql://platform:platform@localhost:5432/sources
    uv run python -m generator report --source s3://platform --dsn postgresql://platform:platform@localhost:5432/sources
    uv run python -m generator stream --config configs/small.yaml --sink file --speed 0
    uv run python -m generator stream --config configs/default.yaml --sink kafka --bootstrap localhost:9092

With `s3://`, MinIO credentials come from the environment variables `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY` and `FSSPEC_S3_ENDPOINT_URL` (e.g. `http://localhost:9000`).
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import time


def main() -> None:
    """Parse command-line arguments and run the selected mode."""
    parser = argparse.ArgumentParser(
        prog="generator", description="Generate simulated multi-Channel data"
    )
    sub = parser.add_subparsers(dest="mode", required=True)

    batch = sub.add_parser("batch", help="Generate Batch data (history up to the Cutoff)")
    batch.add_argument("--config", required=True, help="Path to the YAML config file")
    batch.add_argument("--output", help="Override storage.output_root from the config")
    batch.add_argument(
        "--restart",
        action="store_true",
        help="Ignore the previous run's stamps and regenerate from day 1 (default is to resume)",
    )

    load = sub.add_parser("load-postgres", help="Load a run's pg-load/ into PostgreSQL")
    load.add_argument(
        "--source", required=True, help="Output root of the run (directory or s3://...)"
    )
    load.add_argument("--dsn", required=True, help="PostgreSQL connection string")
    load.add_argument(
        "--sample-days",
        type=int,
        default=0,
        help="Load only N export days SPREAD EVENLY instead of the whole range. Spread "
        "evenly (not the last N days) because data grows over time, so "
        "consecutive days at the end are not a representative sample. 0 = load all. "
        "Table seller.skus is not split by day, so it is always loaded in full.",
    )

    report = sub.add_parser("report", help="Print the data quality report of a run")
    report.add_argument(
        "--source", required=True, help="Output root of the run (directory or s3://...)"
    )
    report.add_argument("--dsn", help="Read Walmart and SKUs from PostgreSQL instead of pg-load/")
    report.add_argument(
        "--markdown", action="store_true", help="Print as Markdown to paste into docs/"
    )

    stream = sub.add_parser("stream", help="Emit the post-Cutoff Stream to Kafka or to files")
    stream.add_argument("--config", required=True, help="Path to the YAML config file")
    stream.add_argument(
        "--output", help="Override storage.output_root (where stream_manifest.json is written)"
    )
    stream.add_argument("--sink", choices=["kafka", "file"], default="kafka")
    stream.add_argument(
        "--bootstrap", default="localhost:9092", help="Kafka address when --sink kafka"
    )
    stream.add_argument(
        "--speed",
        type=float,
        default=1.0,
        help="1 is the configured pace, 60 is 60 times faster, 0 is no waiting",
    )

    args = parser.parse_args()
    started = time.perf_counter()
    if args.mode == "batch":
        _run_batch(args)
    elif args.mode == "stream":
        _run_stream(args)
    elif args.mode == "load-postgres":
        from generator.load_postgres import load_postgres

        print(json.dumps(load_postgres(args.source, args.dsn, args.sample_days), indent=2))
    else:
        from generator.report import QualityReport, render_markdown, render_text

        render = render_markdown if args.markdown else render_text
        print("\n\n".join(render(t) for t in QualityReport(args.source, args.dsn).all_tables()))
    print(f"Done in {time.perf_counter() - started:.1f} seconds")


def _run_batch(args: argparse.Namespace) -> None:
    """Generate the Batch from the config and print a manifest summary."""
    from generator.config import load_config
    from generator.generate_order_snapshots import BatchRun

    config = load_config(args.config)
    if args.output:
        config = dataclasses.replace(
            config, storage=dataclasses.replace(config.storage, output_root=args.output)
        )
    manifest = BatchRun(config).run(restart=args.restart)
    print(
        json.dumps({k: manifest[k] for k in ("datasets", "labels")}, indent=2, ensure_ascii=False)
    )
    print(f"Written to {config.storage.output_root}")


def _run_stream(args: argparse.Namespace) -> None:
    """Emit the Stream from the config and print the measurements."""
    from generator.config import load_config
    from generator.publish_order_snapshot_events import FileSink, KafkaSink, run_stream
    from generator.storage import OutputStore

    config = load_config(args.config)
    if args.output:
        config = dataclasses.replace(
            config, storage=dataclasses.replace(config.storage, output_root=args.output)
        )
    sink = (
        FileSink(OutputStore(config.storage.output_root))
        if args.sink == "file"
        else KafkaSink(args.bootstrap)
    )
    print(json.dumps(run_stream(config, sink, args.speed), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
