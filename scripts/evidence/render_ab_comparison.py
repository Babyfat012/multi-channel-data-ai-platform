"""Render the A/B Agent comparison table of two model configurations from eval JSONL measured on AWS (no re-run).

python -m scripts.evidence.render_ab_comparison --a docs/evidence/aws/agent-eval-data-agent-20261031.jsonl \
  --b docs/evidence/aws/agent-eval-data-agent-b-20261031.jsonl --out docs/evidence/ab-agent-comparison-20261031.png
This image is a table rebuilt from eval data, NOT a Grafana screenshot (the Grafana dashboard is in ab_testing.md).
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import html
import json
from pathlib import Path

from scripts.evidence.chrome import chrome_page


def load(path: Path) -> dict[str, dict]:
    """Eval JSONL → {sku: row}; keeps the first row of each SKU."""
    rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    return {r["sku"]: r for r in rows}


def mark(row: dict | None) -> str:
    """Cell for one branch: ✓/✗/– with seconds."""
    if row is None:
        return "–"
    if not row.get("gradable"):
        return f"– ({row['seconds']:.1f} s)"
    return f"{'✓' if row['pass'] else '✗'} ({row['seconds']:.1f} s)"


def page(a: dict, b: dict, label_a: str, label_b: str) -> str:
    """HTML table per SKU plus a total row: passed/total and mean seconds per branch."""

    def total(rows):
        graded = [r for r in rows.values() if r.get("gradable")]
        passed = sum(bool(r["pass"]) for r in graded)
        seconds = sum(r["seconds"] for r in rows.values()) / len(rows) if rows else 0
        return f"{passed}/{len(graded)} passed, avg {seconds:.1f} s/question"

    body = "".join(
        f"<tr><td>{html.escape(sku)}</td><td>{mark(a.get(sku))}</td><td>{mark(b.get(sku))}</td></tr>"
        for sku in sorted(set(a) | set(b))
    )
    return (
        "<meta charset=utf-8><style>body{font:16px sans-serif;margin:24px}table{border-collapse:collapse}"
        "td,th{border:1px solid #888;padding:6px 14px;text-align:left}tfoot td{font-weight:bold}</style>"
        "<h2>A/B Agent: same prompt, same tools, different ModelConfig (eval 12 SKUs, AWS 31/10/2026)</h2><table>"
        f"<tr><th>SKU</th><th>{html.escape(label_a)}</th><th>{html.escape(label_b)}</th></tr>{body}"
        f"<tfoot><tr><td>Total</td><td>{total(a)}</td><td>{total(b)}</td></tr></tfoot></table>"
    )


async def shoot(doc: str, out: Path) -> None:
    """Render HTML in headless Chrome and save a PNG."""
    tmp = out.with_suffix(".html")
    tmp.write_text(doc)
    try:
        async with chrome_page(window_size="1000,700", disable_dev_shm_usage=True) as tab:
            await tab.navigate(tmp.resolve().as_uri())
            size = (await tab.call("Page.getLayoutMetrics"))["cssContentSize"]
            shot = await tab.call(
                "Page.captureScreenshot",
                {
                    "format": "png",
                    "captureBeyondViewport": True,
                    "clip": {
                        "x": 0,
                        "y": 0,
                        "width": size["width"],
                        "height": size["height"],
                        "scale": 1,
                    },
                },
            )
            out.write_bytes(base64.b64decode(shot["data"]))
    finally:
        tmp.unlink(missing_ok=True)


def main(argv=None) -> None:
    """CLI: two JSONL files (A, B) → one comparison PNG."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--a", type=Path, required=True)
    parser.add_argument("--b", type=Path, required=True)
    parser.add_argument("--label-a", default="A: data-agent (qwen2.5-1.5b)")
    parser.add_argument("--label-b", default="B: data-agent-b (qwen2.5-0.5b)")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    asyncio.run(shoot(page(load(args.a), load(args.b), args.label_a, args.label_b), args.out))


if __name__ == "__main__":
    main()
