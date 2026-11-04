"""Chạy các bộ kiểm thử của Web API/model và chụp output terminal thành ảnh PNG (+ file .txt giữ nguyên output).

Mỗi mục là một lệnh thật; ảnh là output của chính lần chạy đó (không dựng tay). Chạy từ thư mục gốc repo:
  uv run python -m scripts.evidence.capture_testing_evidence --out docs/evidence --tag 20261104 [--only coverage,epbva,hypothesis]
Locust cần cụm EKS nên không nằm ở đây (xem docs/edai-2/validation_verification.md).
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import html
import subprocess
import time
from pathlib import Path

from scripts.evidence.chrome import chrome_page

SERVING = [
    "uv", "run", "--no-project", "--python", "3.12", "--with-requirements", "serving/requirements-dev.txt",
]  # fmt: skip
EP_BVA = "boundaries or sku_format or bad_body or validation or bad_input or bad_upstream"
CHECKS = {
    "coverage": [
        *SERVING,
        "pytest",
        "serving/tests",
        "-q",
        "--cov=serving",
        "--cov-report=term-missing",
    ],
    "epbva": [
        *SERVING,
        "pytest",
        "serving/tests/test_app.py",
        "-v",
        "-k",
        EP_BVA,
        "-p",
        "no:cacheprovider",
    ],
    "hypothesis": [
        *SERVING,
        "pytest",
        "serving/tests/test_app.py::test_idempotent",
        "-v",
        "--hypothesis-show-statistics",
        "-p",
        "no:cacheprovider",
    ],  # fmt: skip
    "hypothesis-model": [
        "uv",
        "run",
        "pytest",
        "tests/unit/test_model_properties.py",
        "-v",
        "--hypothesis-show-statistics",
        "-p",
        "no:cacheprovider",
    ],  # fmt: skip
}


def terminal_html(command: list[str], output: str, exit_code: int, started: str) -> str:
    """Trang HTML kiểu terminal: dòng lệnh, output, mã thoát và giờ chạy."""
    return (
        "<meta charset=utf-8><style>body{background:#1e1e1e;color:#ddd;margin:0;padding:16px}"
        "pre{font:13px/1.35 ui-monospace,Menlo,Consolas,monospace;white-space:pre-wrap;word-break:break-word}"
        ".c{color:#7ec699}.e{color:#f08d49}</style><pre>"
        f"<span class=c>$ {html.escape(' '.join(command))}</span>\n{html.escape(output)}\n"
        f"<span class=e>[exit {exit_code}] {started}</span></pre>"
    )


def run(command: list[str]) -> tuple[str, int]:
    """Chạy lệnh, gộp stdout+stderr; trả (output, mã thoát)."""
    done = subprocess.run(command, capture_output=True, text=True, check=False)
    return done.stdout + done.stderr, done.returncode


async def shoot(doc: str, png: Path) -> None:
    """Render HTML trong Chrome headless, chụp toàn trang."""
    tmp = png.with_suffix(".html")
    tmp.write_text(doc)
    try:
        async with chrome_page(
            window_size="1400,800", disable_dev_shm_usage=True, max_msg_size=60_000_000
        ) as tab:
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
                        "height": min(size["height"], 9000),
                        "scale": 1,
                    },
                },
            )
            png.write_bytes(base64.b64decode(shot["data"]))
    finally:
        tmp.unlink(missing_ok=True)


def main(argv=None) -> int:
    """CLI: chạy từng mục, ghi .txt + .png; trả 1 nếu có lệnh thất bại."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=Path("docs/evidence"))
    parser.add_argument("--tag", required=True)
    parser.add_argument("--only", default=",".join(CHECKS))
    args = parser.parse_args(argv)
    args.out.mkdir(parents=True, exist_ok=True)
    failed = 0
    for name in args.only.split(","):
        command = CHECKS[name]
        started = time.strftime("%Y-%m-%d %H:%M:%SZ", time.gmtime())
        output, code = run(command)
        failed += code != 0
        (args.out / f"testing-{name}-{args.tag}.txt").write_text(output)
        asyncio.run(
            shoot(
                terminal_html(command, output, code, started),
                args.out / f"testing-{name}-{args.tag}.png",
            )
        )
        print(f"{name}: exit {code}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
