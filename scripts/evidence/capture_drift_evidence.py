"""Capture each Evidently drift step: data → stable report → channel_shift report → JS results table.

Run (local simulated data, only to check the flow works before going to EKS):
  uv run --no-project --with evidently==0.7.23 --with pandas==2.3.3 --with pyarrow --with aiohttp \
    env PYTHONPATH=. python -m scripts.evidence.capture_drift_evidence --out docs/evidence --tag local-20261104
On EKS: fetch the HTML from the Job pod (see docs/ml-llm-platform/drift_monitoring.md), then run with --html-dir <HTML directory> --summary <summary.json>
Step 1 image (two data windows): add --data-only [--input <training_lines.parquet>]; with --input it uses the Job's exact input data,
otherwise simulated data.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import html
import json
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

from ml import drift
from scripts.evidence.chrome import chrome_page

CHANNELS = ("amazon", "walmart", "shopify", "tiktok", "shopee")
REFERENCE_DAYS = 30  # matches `--reference-days 30` of the mle-drift Job


def synthetic_lines(
    seed: int = 7, days: int = 60, per_day: int = 400, skus: int = 100
) -> pd.DataFrame:
    """Simulated training_lines: Zipf SKUs; after day 30, the shopee Channel shifts to a different SKU group (causing drift)."""
    rng = np.random.default_rng(seed)
    rows = []
    for day in range(days):
        n = per_day
        channel = rng.choice(CHANNELS, n)
        weights = 1 / np.arange(1, skus + 1) ** 0.8
        weights /= weights.sum()
        sku = rng.choice(skus, n, p=weights)
        if day >= 30:  # shopee after the cutover: SKUs shift to the second half of the catalog
            late = channel == "shopee"
            sku[late] = rng.integers(skus // 2, skus, late.sum())
        rows.append(
            pd.DataFrame(
                {
                    "event_timestamp": pd.Timestamp("2026-01-01", tz="UTC")
                    + pd.Timedelta(days=day),
                    "channel": channel,
                    "seller_sku": [f"SKU-{s:03d}" for s in sku],
                    "label": (rng.random(n) < 0.2).astype(int),
                }
            )
        )
    return pd.concat(rows, ignore_index=True)


def summary_page(summary: dict) -> str:
    """HTML results table: one row per comparison, JS score per column vs threshold, alert."""
    lines = []
    for name, res in summary.items():
        for col, info in res["columns"].items():
            lines.append(
                f"<tr><td>{html.escape(name)}</td><td>{res['reference_rows']:,} / {res['current_rows']:,}</td>"
                f"<td>{html.escape(col)}</td><td>{info['score']:.4f}</td><td>{info['threshold']}</td>"
                f"<td>{'DRIFT' if info['drift'] else 'stable'}</td><td>{'YES' if res['alert'] else 'no'}</td></tr>"
            )
    return (
        "<meta charset=utf-8><style>body{font:16px sans-serif;margin:24px}table{border-collapse:collapse}"
        "td,th{border:1px solid #888;padding:6px 12px}</style><h2>Evidently: Jensen-Shannon per column</h2><table>"
        "<tr><th>Comparison</th><th>Reference / current rows</th><th>Column</th><th>JS</th><th>Threshold</th>"
        "<th>Result</th><th>Alert</th></tr>" + "".join(lines) + "</table>"
    )


def window_rows(pairs: dict) -> list[dict]:
    """Step 1: each comparison → first/last day, row count, positive label rate, SKU count and top three Channels of both windows."""
    rows = []
    for name, (reference, current) in pairs.items():
        row: dict = {"pair": name}
        for side, frame in (("reference", reference), ("current", current)):
            days = pd.to_datetime(frame["event_timestamp"], utc=True)
            row[side] = {
                "from": days.min().date().isoformat(),
                "to": days.max().date().isoformat(),
                "rows": len(frame),
                "positive": float(frame["label"].mean()),
                "skus": int(frame["seller_sku"].nunique()),
                "channels": frame["channel"].value_counts().head(3).to_dict(),
            }
        rows.append(row)
    return rows


def data_page(rows: list[dict], source: str) -> str:
    """Step 1 HTML: the two data windows of each comparison (days, rows, positive labels, SKUs, Channels)."""
    lines = []
    for row in rows:
        for side in ("reference", "current"):
            w = row[side]
            channels = ", ".join(f"{k} {v:,}" for k, v in w["channels"].items())
            lines.append(
                f"<tr><td>{html.escape(row['pair'])}</td><td>{'reference' if side == 'reference' else 'current'}</td>"
                f"<td>{w['from']} → {w['to']}</td><td>{w['rows']:,}</td><td>{w['positive']:.2%}</td>"
                f"<td>{w['skus']:,}</td><td>{html.escape(channels)}</td></tr>"
            )
    return (
        "<meta charset=utf-8><style>body{font:16px sans-serif;margin:24px}table{border-collapse:collapse}"
        "td,th{border:1px solid #888;padding:6px 12px}</style>"
        f"<h2>Step 1: reference data and current data</h2><p>Source: {html.escape(source)}</p><table>"
        "<tr><th>Comparison</th><th>Window</th><th>event_timestamp</th><th>Rows</th><th>Positive labels</th>"
        "<th>Distinct SKUs</th><th>Top three Channels</th></tr>" + "".join(lines) + "</table>"
    )


async def shoot(page, url: str, path: Path, wait_text: str | None = None) -> None:
    """Open the URL, wait for the content to render, then capture the full page."""
    await page.navigate(url)
    for _ in range(60):
        if wait_text is None or wait_text in await page.evaluate("document.body.innerText"):
            break
        await asyncio.sleep(0.5)
    await asyncio.sleep(1)
    size = (await page.call("Page.getLayoutMetrics"))["cssContentSize"]
    height = min(size["height"], 6000)
    shot = await page.call(
        "Page.captureScreenshot",
        {
            "format": "png",
            "captureBeyondViewport": True,
            "clip": {"x": 0, "y": 0, "width": size["width"], "height": height, "scale": 1},
        },
    )
    path.write_bytes(base64.b64decode(shot["data"]))


async def data_png(frame: pd.DataFrame, source: str, png: Path) -> None:
    """Capture the step 1 image (two data windows) from the training_lines DataFrame; the intermediate HTML lives in a temp directory."""
    pairs = drift.comparisons(frame, REFERENCE_DAYS, "shopee")
    with tempfile.TemporaryDirectory() as tmp:
        page_file = Path(tmp) / "data.html"
        page_file.write_text(data_page(window_rows(pairs), source))
        async with chrome_page(window_size="1600,500", disable_dev_shm_usage=True) as page:
            await shoot(page, page_file.resolve().as_uri(), png)


async def main_async(args) -> None:
    """Build/read data, run drift (if simulated), capture three images for the steps."""
    out = args.out
    out.mkdir(parents=True, exist_ok=True)
    if args.data_only:
        frame = pd.read_parquet(args.input) if args.input else synthetic_lines()
        source = args.source or (
            str(args.input) if args.input else "simulated data (synthetic_lines)"
        )
        await data_png(frame, source, out / f"drift-data-{args.tag}.png")
        return
    if args.html_dir:
        html_dir, summary = args.html_dir, json.loads(args.summary.read_text())
        frame = pd.read_parquet(args.input) if args.input else None
    else:
        html_dir = out / f"drift-{args.tag}"
        frame = synthetic_lines()
        parquet = html_dir / "training_lines.parquet"
        html_dir.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(parquet)
        drift.main(
            ["--reference", str(parquet), "--shift-channel", "shopee", "--out", str(html_dir)]
        )
        summary = json.loads((html_dir / "summary.json").read_text())
    (html_dir / "summary.html").write_text(summary_page(summary))
    async with chrome_page(
        window_size="1600,1000", disable_dev_shm_usage=True, max_msg_size=60_000_000
    ) as page:
        for name in ("stable", "channel_shift"):
            await shoot(
                page,
                (html_dir / f"{name}.html").resolve().as_uri(),
                out / f"drift-{name}-{args.tag}.png",
                "Drift",
            )
        await shoot(
            page,
            (html_dir / "summary.html").resolve().as_uri(),
            out / f"drift-summary-{args.tag}.png",
        )
    if frame is not None:
        source = args.source or (
            str(args.input) if args.input else "simulated data (synthetic_lines)"
        )
        await data_png(frame, source, out / f"drift-data-{args.tag}.png")
    print(
        json.dumps(
            {
                "summary": summary,
                "images": sorted(p.name for p in out.glob(f"drift-*-{args.tag}.png")),
            },
            ensure_ascii=False,
            indent=1,
        )
    )


def main(argv=None) -> None:
    """CLI: --out image directory, --tag file name suffix, or --html-dir + --summary for EKS data."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=Path("docs/evidence"))
    parser.add_argument("--tag", default="local")
    parser.add_argument("--html-dir", type=Path)
    parser.add_argument("--summary", type=Path)
    parser.add_argument(
        "--input", type=Path, help="training_lines parquet of the EKS Job, for the step 1 image"
    )
    parser.add_argument(
        "--source", help="data source label shown on the step 1 image (e.g. an S3 path)"
    )
    parser.add_argument(
        "--data-only",
        action="store_true",
        help="only capture the step 1 image (do not run/capture Evidently)",
    )
    args = parser.parse_args(argv)
    if args.html_dir and not args.summary and not args.data_only:
        parser.error("--html-dir requires --summary")
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
