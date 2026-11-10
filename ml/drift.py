"""Monitor data drift of the return-risk model inputs with Evidently.

Compare a "current" batch with a reference batch (the training period) on the input columns, using
Jensen-Shannon distance. Any drifted column ⇒ `alert` ⇒ re-check the model before trusting the score.
Run: `python -m ml.drift --reference <parquet> [--sale-truth <_truth/lines.jsonl>] --out out/drift`.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

COLUMNS = ["seller_sku", "label"]


def summarize(snapshot: dict) -> dict:
    """Condense the Evidently result: per-column score, drifted columns, alert flag.

    The comparison direction depends on the method named in the metric: a p-value drifts when BELOW the threshold,
    a distance (Jensen-Shannon...) drifts when GREATER than or equal to the threshold.
    """
    columns = {}
    for metric in snapshot["metrics"]:
        name = metric.get("metric_name", "")
        if not name.startswith("ValueDrift("):
            continue
        fields = dict(part.split("=", 1) for part in name[len("ValueDrift(") : -1].split(","))
        score, threshold = float(metric["value"]), float(fields["threshold"])
        drift = score < threshold if "p_value" in fields["method"] else score >= threshold
        columns[fields["column"]] = {
            "method": fields["method"],
            "score": score,
            "threshold": threshold,
            "drift": drift,
        }
    drifted = sorted(c for c, v in columns.items() if v["drift"])
    return {
        "columns": columns,
        "drifted": drifted,
        "alert": bool(drifted),
    }


def check(reference: pd.DataFrame, current: pd.DataFrame, html: Path) -> dict:
    """Run DataDriftPreset on COLUMNS (all categorical), save HTML, return the summary."""
    from evidently import DataDefinition, Dataset, Report
    from evidently.presets import DataDriftPreset

    definition = DataDefinition(categorical_columns=COLUMNS)
    data = [
        Dataset.from_pandas(f[COLUMNS].astype(str), data_definition=definition)
        for f in (current, reference)
    ]
    # Jensen-Shannon for every categorical column. seller_sku has ~100 values: sampling noise of ~1,000 rows
    # alone gives a distance of ~0.14, hence threshold 0.2; label has 2 values and keeps the default 0.1.
    preset = DataDriftPreset(
        cat_method="jensenshannon", per_column_threshold={"seller_sku": 0.2, "label": 0.1}
    )
    snapshot = Report([preset]).run(*data)
    html.parent.mkdir(parents=True, exist_ok=True)
    snapshot.save_html(str(html))
    return summarize(snapshot.dict())


def sale_batch(truth_path: str) -> pd.DataFrame:
    """'Sale event' batch from the generator truth file: SKU + return/refund label within 30 days."""
    # Imported here: the Job on EKS (pipeline A) does not ship the generator code; only needed for B's Sale batch.
    from generator.generate_order_lifecycle import POSITIVE_SCENARIOS

    rows = [json.loads(line) for line in Path(truth_path).read_text().splitlines()]
    return pd.DataFrame(
        {
            "seller_sku": [r["sku"] for r in rows],
            "label": [int(r["scenario"] in POSITIVE_SCENARIOS) for r in rows],
        }
    )


def comparisons(frame: pd.DataFrame, reference_days: int, shift_channel: str | None = None) -> dict:
    """(reference, current) pairs: `stable` = first N days vs the rest; `channel_shift` = first N days vs
    the rest of one Channel only (order mix shifting to one Marketplace) — a drift scenario buildable on pipeline A."""
    day = pd.to_datetime(frame["event_timestamp"], utc=True).dt.normalize()
    boundary = day.min() + pd.Timedelta(days=reference_days)
    reference, later = frame[day < boundary], frame[day >= boundary]
    pairs = {"stable": (reference, later)}
    if shift_channel:
        current = later[later["channel"] == shift_channel]
        if current.empty:
            raise ValueError(f"no rows for Channel {shift_channel!r} after the reference boundary")
        pairs["channel_shift"] = (reference, current)
    return pairs


def main(argv=None):
    """Comparisons: stable, Channel shift (if given), pipeline B Sale event (if given)."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--reference", required=True, help="Parquet training_lines (local or s3://)"
    )
    parser.add_argument("--reference-days", type=int, default=30)
    parser.add_argument(
        "--shift-channel", help="e.g. shopee: the current batch holds one Channel only"
    )
    parser.add_argument("--sale-truth")
    parser.add_argument("--out", type=Path, default=Path("out/drift"))
    args = parser.parse_args(argv)
    frame = pd.read_parquet(args.reference)
    pairs = comparisons(frame, args.reference_days, args.shift_channel)
    results = {
        name: {
            "reference_rows": len(ref),
            "current_rows": len(cur),
            **check(ref, cur, args.out / f"{name}.html"),
        }
        for name, (ref, cur) in pairs.items()
    }
    if args.sale_truth:
        results["sale_event"] = check(
            pairs["stable"][0], sale_batch(args.sale_truth), args.out / "sale_event.html"
        )
    (args.out / "summary.json").write_text(json.dumps(results, indent=2))
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
