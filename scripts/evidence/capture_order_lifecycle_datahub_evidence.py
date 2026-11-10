"""Capture the real Lineage/Quality for the Gold fact and Silver lifecycle of dp_order_lifecycle.

Uses headless Chrome/CDP and logs in like evidence/capture_order_snapshots_datahub_evidence.py.
--print-urls only builds URLs/URNs, without importing the browser or opening connections.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import re
from pathlib import Path
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "docs/images/datahub/v2"
BASE_URL = "http://127.0.0.1:9002"
DATASETS = {
    "dp-order-lifecycle-gold": "lakehouse.gold.lifecycle_fact_order_lines",
    "dp-order-lifecycle-silver": "lakehouse.silver.lifecycle_stg_line_lifecycle",
}


def dataset_urn(name: str) -> str:
    """Iceberg PROD URN with the v2 namespace, matching publisher v2."""
    return f"urn:li:dataset:(urn:li:dataPlatform:iceberg,{name},PROD)"


def dataset_url(name: str, tab: str) -> str:
    """DataHub route with the percent-encoded URN."""
    return f"{BASE_URL}/dataset/{quote(dataset_urn(name), safe='')}/{tab}"


def capture_targets() -> list[dict[str, str]]:
    """List the four images so offline tests and operators can check them beforehand."""
    return [
        {
            "urn": dataset_urn(name),
            "url": dataset_url(name, route),
            "path": str(OUTPUT / f"{prefix}-{suffix}.png"),
            "route": route,
            "name": name,
        }
        for prefix, name in DATASETS.items()
        for route, suffix in (("Lineage", "lineage"), ("Quality", "validation"))
    ]


async def wait_for_quality(page, seconds: int = 90) -> str:
    """Do not assume a single assertion: publisher v2 creates several checks per table."""
    body = ""
    for _ in range(seconds * 2):
        body = await page.evaluate("document.body.innerText || ''") or ""
        if re.search(r"\b[1-9]\d*\s+Passing\b", body):
            if re.search(r"\b[1-9]\d*\s+Failing\b", body):
                raise RuntimeError(f"Quality has failing assertions: {body[:1500]}")
            return body
        await asyncio.sleep(0.5)
    raise TimeoutError(f"No Passing v2 assertion seen yet: {body[:1500]}")


async def capture(inspect_only: bool = False) -> None:
    """Capture the real UI, saving only once the dataset and tab content have appeared."""

    from scripts.evidence.capture_order_snapshots_datahub_evidence import (
        collapse_sidebar,
        dismiss_onboarding,
        login,
        wait_for_text,
    )
    from scripts.evidence.chrome import chrome_page

    if not inspect_only:
        OUTPUT.mkdir(parents=True, exist_ok=True)
    async with chrome_page(disable_dev_shm_usage=True) as page:
        await login(page)
        for target in capture_targets():
            await page.navigate(target["url"])
            await wait_for_text(page, target["name"].split(".")[-1], seconds=90)
            await dismiss_onboarding(page)
            if target["route"] == "Lineage":
                body = await wait_for_text(page, "Airflow | Task", seconds=90)
                await collapse_sidebar(page)
            else:
                body = await wait_for_quality(page)
            await asyncio.sleep(3)
            if inspect_only:
                print(f"{target['url']}\n{body[:1500]}\n")
                continue
            result = await page.call(
                "Page.captureScreenshot",
                {
                    "format": "png",
                    "captureBeyondViewport": False,
                },
            )
            path = Path(target["path"])
            path.write_bytes(base64.b64decode(result["data"]))
            print(f"Captured {path.relative_to(ROOT)}")


def main() -> None:
    """CLI: capture, or check routes without Chrome."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inspect-only", action="store_true")
    parser.add_argument("--print-urls", action="store_true")
    args = parser.parse_args()
    if args.print_urls:
        print(json.dumps(capture_targets(), ensure_ascii=False, indent=2))
    else:
        asyncio.run(capture(args.inspect_only))


if __name__ == "__main__":
    main()
