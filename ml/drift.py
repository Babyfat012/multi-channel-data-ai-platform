"""Data drift of the return-risk model with Evidently.

Monitor (KFP `return-risk-monitor`, ml/training_pipeline.py): reference = the train set of the model holding
alias `production` (MLflow artifact `data/train`); current = labeled lines newer than everything that model saw
(its train + test windows); columns = the model inputs (train_lib.FEATURES) + label. Labels arrive ~30 days
after the Order, so "current" lags the calendar by that much. Drift ⇒ train, unless a challenger awaits approval.

Demo CLI (AWS evidence 2026-10-31, Job infra/k8s/ml/drift-job.yaml): slices of one training_lines file on
DEMO_COLUMNS, which are NOT model inputs (seller_sku is not a feature, label is the target).
Run: `python -m ml.drift --reference <parquet> [--sale-truth <_truth/lines.jsonl>] --out out/drift`.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

# Standalone on purpose (no ml.train_lib import): the demo Job mounts only this file. The monitor passes
# train_lib.CATEGORICAL + ["label"] and train_lib.NUMERIC itself.
DEMO_COLUMNS = ["seller_sku", "label"]
# Jensen-Shannon thresholds for categoricals; others keep Evidently's default (0.1, or its numeric method's).
# seller_sku has ~100 values: sampling noise of ~1,000 rows alone gives a distance of ~0.14, hence 0.2.
THRESHOLDS = {"seller_sku": 0.2, "label": 0.1}


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


def empty_columns(reference: pd.DataFrame, current: pd.DataFrame, columns) -> list[str]:
    """Columns with no value at all on either side (e.g. approved_returns_24h, gap M4)."""
    return sorted(c for c in columns if reference[c].isna().all() or current[c].isna().all())


def check(
    reference: pd.DataFrame,
    current: pd.DataFrame,
    html: Path,
    categorical: list[str] = DEMO_COLUMNS,
    numeric: list[str] = (),
) -> dict:
    """Run DataDriftPreset (Jensen-Shannon for categoricals, Evidently's default for numerics), save HTML,
    return the summary. A numeric column empty on either side cannot be scored (Evidently raises): it is left
    out and listed under `empty`. That is a data break, not drift: it must not trigger training on broken data."""
    from evidently import DataDefinition, Dataset, Report
    from evidently.presets import DataDriftPreset

    empty = empty_columns(reference, current, numeric)
    numeric = [c for c in numeric if c not in empty]
    definition = DataDefinition(
        categorical_columns=list(categorical), numerical_columns=list(numeric)
    )

    def typed(frame):
        """Categoricals as text, numerics as float (NaN stays missing), only the checked columns."""
        out = frame[list(categorical)].astype(str)
        for col in numeric:
            out[col] = pd.to_numeric(frame[col], errors="coerce").astype("float64")
        return out

    data = [Dataset.from_pandas(typed(f), data_definition=definition) for f in (current, reference)]
    preset = DataDriftPreset(
        cat_method="jensenshannon",
        per_column_threshold={c: t for c, t in THRESHOLDS.items() if c in categorical},
    )
    snapshot = Report([preset]).run(*data)
    html.parent.mkdir(parents=True, exist_ok=True)
    snapshot.save_html(str(html))
    return summarize(snapshot.dict()) | {"empty": empty}


def unseen(frame: pd.DataFrame, *seen: pd.DataFrame) -> pd.DataFrame:
    """Lines newer than everything the production model saw: comparing it with its own test window
    would only re-measure data it was already judged on."""
    cutoff = max(pd.to_datetime(f["event_timestamp"], utc=True).max() for f in seen)
    return frame[pd.to_datetime(frame["event_timestamp"], utc=True) > cutoff]


def decide(alert: bool, current_rows: int, min_rows: int, pending: str | None) -> tuple[str, str]:
    """("train" | "skip", reason). Too few rows is checked first: a drift score on a handful of lines is noise.
    A pending challenger blocks training: otherwise persistent drift retrains every day, the challenger alias
    moves daily and promote_return_risk refuses the version the reviewer was looking at."""
    if current_rows < min_rows:
        return (
            "skip",
            f"{current_rows} new labeled lines < {min_rows} (labels lag the Order ~30 days)",
        )
    if not alert:
        return "skip", "no drift"
    if pending is not None:
        return "skip", f"drift, but challenger v{pending} is still awaiting approval"
    return "train", "drift"


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
