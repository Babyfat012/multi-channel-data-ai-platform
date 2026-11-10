"""Capture the Grid of Airflow DAGs with headless Chrome for the Data Platform pipeline.

Reads the standalone password from the container via the subprocess's internal stdout,
without printing it to the terminal or writing it to a file. Uses the Chrome DevTools Protocol
to log in and save real UI screenshots, not images rebuilt from CLI state.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import re
import subprocess
from pathlib import Path

from scripts.evidence.chrome import chrome_page

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "docs" / "images" / "airflow"
BASE_URL = "http://127.0.0.1:8081"


def standalone_password() -> str:
    """Read the Airflow password from the container without exposing it on the command line."""
    env = os.environ.copy()
    env.pop("POSTGRES_PASSWORD", None)
    command = [
        "docker",
        "compose",
        "--profile",
        "airflow",
        "exec",
        "-T",
        "airflow",
        "cat",
        "/opt/airflow/standalone_admin_password.txt",
    ]
    return subprocess.check_output(command, cwd=ROOT, env=env, text=True).strip()


async def capture(password: str, dag_ids: list[str], suffix: str = "") -> None:
    """Log in to Airflow and capture the Grid/Graph of the selected DAGs as PNG."""
    OUTPUT.mkdir(parents=True, exist_ok=True)
    async with chrome_page() as page:
        await page.navigate(f"{BASE_URL}/login/")
        form_present = await page.evaluate(
            "Boolean(document.querySelector('input[name=\"username\"]') "
            "&& document.querySelector('input[name=\"password\"]'))"
        )
        if not form_present:
            raise RuntimeError("Airflow login form not found")
        await page.evaluate(
            "(() => {"
            f"document.querySelector('input[name=\"username\"]').value = {json.dumps('admin')};"
            f"document.querySelector('input[name=\"password\"]').value = {json.dumps(password)};"
            "document.querySelector('input[name=\"password\"]').form.submit();"
            "})()"
        )
        for _ in range(60):
            url = await page.evaluate("location.href")
            if "/login/" not in url:
                break
            await asyncio.sleep(0.5)
        else:
            raise RuntimeError("Airflow did not accept the login")

        for dag_id in dag_ids:
            for view in ("grid", "graph"):
                await page.navigate(f"{BASE_URL}/dags/{dag_id}/{view}")
                await asyncio.sleep(3)
                url = await page.evaluate("location.href")
                if "/login/" in url:
                    raise RuntimeError("Login session expired while capturing the DAG")
                image = await page.call(
                    "Page.captureScreenshot",
                    {
                        "format": "png",
                        "captureBeyondViewport": False,
                    },
                )
                path = OUTPUT / f"{dag_id}-{view}{suffix}.png"
                path.write_bytes(base64.b64decode(image["data"]))
                print(f"Captured {path.relative_to(ROOT)}")
    await asyncio.sleep(0.5)


def main() -> None:
    """Parse the CLI and run the capture session."""
    parser = argparse.ArgumentParser(description="Capture Airflow UI evidence")
    parser.add_argument(
        "--dag-id",
        action="append",
        choices=(
            "dp1_bronze",
            "dp2_silver",
            "dp1_cdc",
            "dp3_features",
            "dp3_return_labels",
            "platform_runtime_config_check",
        ),
        help="DAG to capture; leave empty to capture the default DP1/DP2",
    )
    parser.add_argument("--suffix", default="", help="suffix to keep images from previous runs")
    args = parser.parse_args()
    if args.suffix and not re.fullmatch(r"-[a-z0-9-]+", args.suffix):
        parser.error(
            "--suffix must start with '-' and contain only lowercase letters, digits and '-'"
        )
    dag_ids = args.dag_id or ["dp1_bronze", "dp2_silver"]
    asyncio.run(capture(standalone_password(), dag_ids, args.suffix))


if __name__ == "__main__":
    main()
