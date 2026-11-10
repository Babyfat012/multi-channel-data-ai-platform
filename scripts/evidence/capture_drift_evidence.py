"""Chụp từng bước drift Evidently: dữ liệu → báo cáo stable → báo cáo channel_shift → bảng kết quả JS.

Chạy (dữ liệu mô phỏng local, chỉ để kiểm quy trình chạy được trước khi lên EKS):
  uv run --no-project --with evidently==0.7.23 --with pandas==2.3.3 --with pyarrow --with aiohttp \
    env PYTHONPATH=. python -m scripts.evidence.capture_drift_evidence --out docs/evidence --tag local-20261104
Trên EKS: lấy HTML từ pod Job (xem docs/ml-llm-platform/drift_monitoring.md) rồi chạy với --html-dir <thư mục HTML> --summary <summary.json>
Ảnh bước 1 (hai cửa sổ dữ liệu): thêm --data-only [--input <training_lines.parquet>]; có --input thì dùng đúng dữ liệu đầu vào của Job,
không có thì dùng dữ liệu mô phỏng.
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
REFERENCE_DAYS = 30  # khớp `--reference-days 30` của Job mle-drift


def synthetic_lines(
    seed: int = 7, days: int = 60, per_day: int = 400, skus: int = 100
) -> pd.DataFrame:
    """training_lines mô phỏng: SKU Zipf; sau mốc 30 ngày, kênh shopee dồn về nhóm SKU khác (gây drift)."""
    rng = np.random.default_rng(seed)
    rows = []
    for day in range(days):
        n = per_day
        channel = rng.choice(CHANNELS, n)
        weights = 1 / np.arange(1, skus + 1) ** 0.8
        weights /= weights.sum()
        sku = rng.choice(skus, n, p=weights)
        if day >= 30:  # shopee sau mốc: SKU lệch sang nửa sau của danh mục
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
    """HTML bảng kết quả: mỗi phép so một dòng, điểm JS từng cột so với ngưỡng, alert."""
    lines = []
    for name, res in summary.items():
        for col, info in res["columns"].items():
            lines.append(
                f"<tr><td>{html.escape(name)}</td><td>{res['reference_rows']:,} / {res['current_rows']:,}</td>"
                f"<td>{html.escape(col)}</td><td>{info['score']:.4f}</td><td>{info['threshold']}</td>"
                f"<td>{'DRIFT' if info['drift'] else 'ổn định'}</td><td>{'CÓ' if res['alert'] else 'không'}</td></tr>"
            )
    return (
        "<meta charset=utf-8><style>body{font:16px sans-serif;margin:24px}table{border-collapse:collapse}"
        "td,th{border:1px solid #888;padding:6px 12px}</style><h2>Evidently: Jensen-Shannon theo cột</h2><table>"
        "<tr><th>Phép so</th><th>Dòng tham chiếu / hiện tại</th><th>Cột</th><th>JS</th><th>Ngưỡng</th>"
        "<th>Kết quả</th><th>Alert</th></tr>" + "".join(lines) + "</table>"
    )


def window_rows(pairs: dict) -> list[dict]:
    """Bước 1: mỗi phép so → ngày đầu/cuối, số dòng, tỷ lệ nhãn dương, số SKU và ba kênh lớn nhất của hai cửa sổ."""
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
    """HTML bước 1: hai cửa sổ dữ liệu của mỗi phép so (ngày, số dòng, nhãn dương, SKU, kênh)."""
    lines = []
    for row in rows:
        for side in ("reference", "current"):
            w = row[side]
            channels = ", ".join(f"{k} {v:,}" for k, v in w["channels"].items())
            lines.append(
                f"<tr><td>{html.escape(row['pair'])}</td><td>{'tham chiếu' if side == 'reference' else 'hiện tại'}</td>"
                f"<td>{w['from']} → {w['to']}</td><td>{w['rows']:,}</td><td>{w['positive']:.2%}</td>"
                f"<td>{w['skus']:,}</td><td>{html.escape(channels)}</td></tr>"
            )
    return (
        "<meta charset=utf-8><style>body{font:16px sans-serif;margin:24px}table{border-collapse:collapse}"
        "td,th{border:1px solid #888;padding:6px 12px}</style>"
        f"<h2>Bước 1: dữ liệu tham chiếu và dữ liệu hiện tại</h2><p>Nguồn: {html.escape(source)}</p><table>"
        "<tr><th>Phép so</th><th>Cửa sổ</th><th>event_timestamp</th><th>Số dòng</th><th>Nhãn dương</th>"
        "<th>SKU khác nhau</th><th>Ba kênh lớn nhất</th></tr>" + "".join(lines) + "</table>"
    )


async def shoot(page, url: str, path: Path, wait_text: str | None = None) -> None:
    """Mở URL, đợi nội dung render rồi chụp toàn trang."""
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
    """Chụp ảnh bước 1 (hai cửa sổ dữ liệu) từ DataFrame training_lines; HTML trung gian nằm ở thư mục tạm."""
    pairs = drift.comparisons(frame, REFERENCE_DAYS, "shopee")
    with tempfile.TemporaryDirectory() as tmp:
        page_file = Path(tmp) / "data.html"
        page_file.write_text(data_page(window_rows(pairs), source))
        async with chrome_page(window_size="1600,500", disable_dev_shm_usage=True) as page:
            await shoot(page, page_file.resolve().as_uri(), png)


async def main_async(args) -> None:
    """Dựng/đọc dữ liệu, chạy drift (nếu mô phỏng), chụp ba ảnh cho các bước."""
    out = args.out
    out.mkdir(parents=True, exist_ok=True)
    if args.data_only:
        frame = pd.read_parquet(args.input) if args.input else synthetic_lines()
        source = args.source or (
            str(args.input) if args.input else "dữ liệu mô phỏng (synthetic_lines)"
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
            str(args.input) if args.input else "dữ liệu mô phỏng (synthetic_lines)"
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
    """CLI: --out thư mục ảnh, --tag hậu tố tên file, hoặc --html-dir + --summary cho dữ liệu EKS."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=Path("docs/evidence"))
    parser.add_argument("--tag", default="local")
    parser.add_argument("--html-dir", type=Path)
    parser.add_argument("--summary", type=Path)
    parser.add_argument(
        "--input", type=Path, help="parquet training_lines của Job EKS, để chụp ảnh bước 1"
    )
    parser.add_argument("--source", help="nhãn nguồn dữ liệu ghi trên ảnh bước 1 (vd đường dẫn S3)")
    parser.add_argument(
        "--data-only", action="store_true", help="chỉ chụp ảnh bước 1 (không chạy/chụp Evidently)"
    )
    args = parser.parse_args(argv)
    if args.html_dir and not args.summary and not args.data_only:
        parser.error("--html-dir cần --summary")
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
