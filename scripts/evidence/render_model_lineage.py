"""Render the model → run → data trace-back chain from the MLflow receipt collected on AWS (no new MLflow calls).

python -m scripts.evidence.render_model_lineage --receipt docs/evidence/aws/mlflow-model-lineage.json \
  --out docs/evidence/aws/model-lineage-chain.png
This image is rebuilt from the `mlflow_run_data_proof.py` receipt, NOT an MLflow UI screenshot
(UI screenshots of the run, Tags, Artifacts are in training_pipeline.md).
"""

from __future__ import annotations

import argparse
import asyncio
import html
import json
from datetime import UTC, datetime
from pathlib import Path

from scripts.evidence.render_ab_comparison import shoot

CSS = (
    "body{font:15px sans-serif;margin:24px;max-width:1000px}h2{margin:0 0 14px}"
    ".box{border:2px solid #2d6cb5;border-radius:8px;padding:10px 14px;margin:0}"
    ".arrow{text-align:center;font-size:22px;color:#2d6cb5;margin:4px 0}"
    "code{background:#eef;padding:2px 4px 5px;border-radius:3px;word-break:break-all;line-height:1.9}"
    "table{border-collapse:collapse;width:100%}td,th{border:1px solid #999;padding:4px 8px;text-align:left}"
    ".ok{color:#1a7f37;font-weight:bold}.bad{color:#c0392b;font-weight:bold}"
)


def verdict(match: bool) -> str:
    """SHA-256 verdict cell: whether the hash recomputed from the artifact matches the run tag."""
    return '<span class="ok">✓ match</span>' if match else '<span class="bad">✗ MISMATCH</span>'


def started(ms: int) -> str:
    """Run start time (epoch milliseconds) → UTC string."""
    return datetime.fromtimestamp(ms / 1000, tz=UTC).strftime("%Y-%m-%d %H:%M UTC")


def page(receipt: dict) -> str:
    """HTML of four chained blocks: model, run, data tags, data artifacts with SHA-256."""
    model, tags, metrics = receipt["model"], receipt["tags"], receipt["metrics"]
    rows = "".join(
        f"<tr><td><code>{html.escape(a['path'])}</code></td><td>{a['bytes']:,} byte</td>"
        f"<td><code>{a['sha256']}</code></td>"
        f"<td><code>{html.escape(receipt[name]['tag'])}</code></td><td>{verdict(receipt[name]['sha256_match'])}</td></tr>"
        for name in ("train", "test")
        for a in receipt[name]["artifacts"]
    )
    return (
        f"<meta charset=utf-8><style>{CSS}</style>"
        "<h2>Tracing model <code>return-risk</code> back to the run and data that produced it (AWS)</h2>"
        f'<div class="box"><b>① Model Registry</b>: <code>{html.escape(model["name"])}</code> '
        f"version <b>{html.escape(model['version'])}</b>, source <code>{html.escape(model['source'])}</code>, "
        f'registered run_id <code>{model["run_id"]}</code></div><div class="arrow">▼ run_id</div>'
        f'<div class="box"><b>② Run MLflow</b> <code>{html.escape(receipt["run_name"])}</code> '
        f"(<code>{receipt['run_id']}</code>, {started(receipt['started'])}): "
        f"algorithm {html.escape(receipt['params']['algo'])}, seed {html.escape(receipt['params']['seed'])}; "
        f"PR-AUC model {metrics['pr_auc_model']:.4f} vs baseline {metrics['pr_auc_baseline']:.4f}, "
        f'test_rows {int(metrics["test_rows"]):,}</div><div class="arrow">▼ tag <code>data.*</code> written at training time</div>'
        f'<div class="box"><b>③ Data per the run tags</b>: dataset <code>{html.escape(tags["data.dataset_version"])}</code>, '
        f"generation <code>{html.escape(tags['data.generation_id'])}</code>, "
        f"source <code>{html.escape(tags['data.entity_path'])}</code></div>"
        '<div class="arrow">▼ SHA-256 recomputed from the run\'s own artifacts</div>'
        '<div class="box"><b>④ Run data artifacts</b><table>'
        "<tr><th>Artifact</th><th>Size</th><th>Recomputed SHA-256</th><th>Run tag</th><th>Verdict</th></tr>"
        f"{rows}</table></div>"
        "<p>Rebuilt from receipt <code>mlflow-model-lineage.json</code> (04/11/2026). The link to the KFP run relies on "
        "timing, because the KFP run ID is not yet written as an MLflow tag.</p>"
    )


def main(argv=None) -> None:
    """CLI: receipt JSON → one PNG."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    asyncio.run(shoot(page(json.loads(args.receipt.read_text())), args.out))


if __name__ == "__main__":
    main()
