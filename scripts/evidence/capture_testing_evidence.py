"""Run the Web API/model test suites and capture the terminal output as PNG images (+ a .txt file with the raw output).

Each item is a real command; the image is the output of that very run (not hand-made). Run from the repo root:
  uv run python -m scripts.evidence.capture_testing_evidence --out docs/evidence --tag 20261104 [--only coverage,epbva,hypothesis]
Mutation by Git diff: add --mutation-dir out/<output directory> (captures an existing run, does not re-run).
Locust needs the EKS cluster, so it is not here (see docs/ml-llm-platform/validation_verification.md).
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import html
import json
import re
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


MUTATION_COMMAND = [
    *SERVING, "python", "scripts/evidence/run_changed_mutation.py",
    "--base", "89cd44e^", "--head", "89cd44e", "--max-children", "2", "--output", "out/mutation-changed",
]  # fmt: skip
PROGRESS = re.compile(
    r"(\d+/\d+)\s+🎉\s+(\d+)\s+🫥\s+(\d+)\s+⏰\s+(\d+)\s+🤔\s+(\d+)\s+🙁\s+(\d+)\s+🔇\s+(\d+)"
)


def mutation_output(run_dir: Path) -> tuple[str, int]:
    """Readable output of one `run_changed_mutation.py` run: the last progress line of run.txt (spinner removed) and the manifest.

    Returns (text, exit code): code 0 when the manifest records status passed.
    """
    manifest = json.loads((run_dir / "manifest.json").read_text())
    found = PROGRESS.findall((run_dir / "run.txt").read_text())
    if not found:
        raise ValueError(f"run.txt has no mutmut progress line: {run_dir}")
    done, killed, no_tests, timeout, suspicious, survived, skipped = found[-1]
    summary = manifest["summary"]
    lines = [
        f"mutmut {manifest['mutmut_version']}; files selected from Git diff {manifest['base'][:7]}..{manifest['head'][:7]}",
        f"selected_files: {', '.join(manifest['selected_files'])}",
        f"{done}  killed {killed}  no tests {no_tests}  timeout {timeout}  suspicious {suspicious}  survived {survived}  skipped {skipped}",
        f"killed / total = {summary['killed_percent_all']:.1f}%  (excluding no tests: {summary['killed_percent_tested']:.1f}%)",
        f"status: {manifest['status']}  run_exit_code: {manifest['run_exit_code']}  results_exit_code: {manifest['results_exit_code']}",
        f"{manifest['started_at']} → {manifest['finished_at']}",
    ]
    return "\n".join(lines), 0 if manifest["status"] == "passed" else 1


def terminal_html(command: list[str], output: str, exit_code: int, started: str) -> str:
    """Terminal-style HTML page: command line, output, exit code and run time."""
    return (
        "<meta charset=utf-8><style>body{background:#1e1e1e;color:#ddd;margin:0;padding:16px}"
        "pre{font:13px/1.35 ui-monospace,Menlo,Consolas,monospace;white-space:pre-wrap;word-break:break-word}"
        ".c{color:#7ec699}.e{color:#f08d49}</style><pre>"
        f"<span class=c>$ {html.escape(' '.join(command))}</span>\n{html.escape(output)}\n"
        f"<span class=e>[exit {exit_code}] {started}</span></pre>"
    )


def run(command: list[str]) -> tuple[str, int]:
    """Run a command, merging stdout+stderr; returns (output, exit code)."""
    done = subprocess.run(command, capture_output=True, text=True, check=False)
    return done.stdout + done.stderr, done.returncode


async def shoot(doc: str, png: Path) -> None:
    """Render HTML in headless Chrome and capture the full page."""
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
    """CLI: run each item, write .txt + .png; returns 1 if any command failed."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=Path("docs/evidence"))
    parser.add_argument("--tag", required=True)
    parser.add_argument("--only", default=",".join(CHECKS))
    parser.add_argument(
        "--mutation-dir",
        type=Path,
        help="run_changed_mutation.py output directory; captures only this item",
    )
    args = parser.parse_args(argv)
    args.out.mkdir(parents=True, exist_ok=True)
    if args.mutation_dir:
        output, code = mutation_output(args.mutation_dir)
        started = json.loads((args.mutation_dir / "manifest.json").read_text())["started_at"]
        (args.out / f"testing-mutation-changed-{args.tag}.txt").write_text(output)
        asyncio.run(
            shoot(
                terminal_html(MUTATION_COMMAND, output, code, started),
                args.out / f"testing-mutation-changed-{args.tag}.png",
            )
        )
        print(f"mutation-changed: exit {code}")
        return code
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
