"""Capture the completed Silver SQL sub-execution from a Spark History Server.

Defaults reproduce the local 2026-10-14 proof. For AWS (History Server tunnelled to
127.0.0.1:18080) set SPARK_APP_ID, SPARK_EVIDENCE_OUT and SPARK_EVIDENCE_TAG, e.g.
``SPARK_APP_ID=app-... SPARK_EVIDENCE_OUT=docs/evidence/aws SPARK_EVIDENCE_TAG=-aws-20261104``.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
from pathlib import Path

import aiohttp

from scripts.evidence.chrome import chrome_page

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / os.environ.get("SPARK_EVIDENCE_OUT", "docs/evidence/spark-ui")
APP_ID = os.environ.get("SPARK_APP_ID", "app-20261014035356-0002")
BASE_URL = os.environ.get("SPARK_HISTORY_URL", "http://127.0.0.1:18080")
TAG = os.environ.get("SPARK_EVIDENCE_TAG", "")
TOKENS = ("AmazonOrderId", "orderId", "from_json", "Union", "row_number")


async def main() -> None:
    """Find the completed schema-normalization/dedup execution, save REST proof and UI shots."""
    OUTPUT.mkdir(parents=True, exist_ok=True)
    async with aiohttp.ClientSession() as session:
        url = f"{BASE_URL}/api/v1/applications/{APP_ID}/sql?length=1000"
        async with session.get(url) as response:
            response.raise_for_status()
            executions = await response.json()
        # Execution id differs per run (3 locally); the wrapper write has no parse/dedup plan.
        matches = [
            e
            for e in executions
            if e["status"] == "COMPLETED" and all(t in e["planDescription"] for t in TOKENS)
        ]
        if not matches:
            raise ValueError("Expected completed schema-normalization and dedup plan")
        execution = matches[-1]
        exec_id = execution["id"]
        (OUTPUT / f"silver-amazon-execution-{exec_id}{TAG}.json").write_text(
            json.dumps(execution, ensure_ascii=False, indent=2) + "\n"
        )
        async with chrome_page(
            disable_dev_shm_usage=True, window_size="1920,1600", max_msg_size=30_000_000
        ) as page:
            await page.navigate(f"{BASE_URL}/history/{APP_ID}/SQL/execution/?id={exec_id}")
            for _ in range(60):
                body = await page.evaluate("document.body.innerText")
                if "Window" in body and "Union" in body:
                    break
                await asyncio.sleep(0.5)
            else:
                raise RuntimeError(f"SQL execution {exec_id} graph did not load: {body[:500]}")
            await asyncio.sleep(1)
            layout = await page.call("Page.getLayoutMetrics")
            size = layout["cssContentSize"]
            screenshot = await page.call(
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
            (OUTPUT / f"12-silver-amazon-dedup-plan{TAG}.png").write_bytes(
                base64.b64decode(screenshot["data"])
            )
            print(
                json.dumps(
                    {
                        "application": APP_ID,
                        "execution": exec_id,
                        "status": execution["status"],
                        "page_size": size,
                    }
                )
            )
            await page.evaluate("clickPhysicalPlanDetails()")
            await page.evaluate("window.find('(4) Project', false, false, true)")
            await asyncio.sleep(0.5)
            details = await page.call(
                "Page.captureScreenshot",
                {
                    "format": "png",
                    "captureBeyondViewport": False,
                },
            )
            (OUTPUT / f"13-silver-amazon-schema-details{TAG}.png").write_bytes(
                base64.b64decode(details["data"])
            )


if __name__ == "__main__":
    asyncio.run(main())
