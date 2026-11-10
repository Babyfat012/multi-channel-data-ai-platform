"""Capture DataHub lineage and assertion pages for DP1, DP2 and DP3.

The screenshots come from the running DataHub UI through Chrome DevTools.
Credentials come from DATAHUB_UI_USERNAME/PASSWORD (quickstart defaults).
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
from pathlib import Path
from urllib.parse import quote

from scripts.evidence.chrome import ChromePage, chrome_page

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "docs" / "images" / "datahub"
BASE_URL = "http://127.0.0.1:9002"
DATASETS = {
    "dp1": "lakehouse.bronze.raw_amazon_orders_dp1",
    "dp2": "lakehouse.gold.fact_order_lines_dp2",
    "dp3": "lakehouse.gold.feat_order_line_return_dp3",
}
# Image set for the two checks re-run with the 2026-10-18 code; written to a separate directory so
# the six historical images in docs/images/datahub/ are not overwritten.
PROFILES = {
    "legacy": (DATASETS, OUTPUT),
    "v2": (
        {
            "dp1-cdc": "lakehouse.bronze.raw_walmart_cdc_v2",
            "dp3": "lakehouse.gold.feat_order_line_return_dp3",
        },
        OUTPUT / "v2",
    ),
    # DataHub on EC2 (via tunnel 9002), published from v1 DAG logs of the 100 GB run on 27–28/10.
    "aws": (DATASETS, ROOT / "docs" / "evidence" / "aws" / "datahub"),
}


def dataset_url(name: str, tab: str) -> str:
    """Build the DataHub route for an Iceberg dataset and tab."""
    urn = f"urn:li:dataset:(urn:li:dataPlatform:iceberg,{name},PROD)"
    return f"{BASE_URL}/dataset/{quote(urn, safe='')}/{tab}"


async def wait_for_text(page: ChromePage, token: str, seconds: int = 45) -> str:
    """Wait until the client-side UI displays a required token."""
    body = ""
    for _ in range(seconds * 2):
        body = await page.evaluate("document.body.innerText || ''") or ""
        if token in body:
            return body
        await asyncio.sleep(0.5)
    raise RuntimeError(f"DataHub UI did not show {token!r}: {body[:500]!r}")


async def login(page: ChromePage) -> None:
    """Sign in with the quickstart account without logging the password."""
    await page.navigate(BASE_URL)
    await wait_for_text(page, "Login")
    username = os.getenv("DATAHUB_UI_USERNAME", "datahub")
    password = os.getenv("DATAHUB_UI_PASSWORD", "datahub")
    result = await page.evaluate(
        "(() => {"
        "const set = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set;"
        "for (const [id, value] of [["
        + json.dumps("username")
        + ", "
        + json.dumps(username)
        + "], ["
        + json.dumps("password")
        + ", "
        + json.dumps(password)
        + "]]) {"
        "const el = document.getElementById(id); if (!el) return 'missing ' + id;"
        "set.call(el, value); el.dispatchEvent(new Event('input', {bubbles:true}));"
        "el.dispatchEvent(new Event('change', {bubbles:true}));"
        "} return 'filled'; })()"
    )
    if result != "filled":
        raise RuntimeError(f"DataHub login form: {result}")
    await asyncio.sleep(0.5)
    await page.evaluate("document.querySelector('[data-testid=\"sign-in\"]').click()")
    for _ in range(60):
        login_visible = await page.evaluate(
            "Boolean(document.querySelector('[data-testid=\"sign-in\"]'))"
        )
        if not login_visible:
            return
        await asyncio.sleep(0.5)
    body = await page.evaluate("document.body.innerText || ''")
    raise RuntimeError(f"DataHub login did not complete: {body[:400]!r}")


async def dismiss_onboarding(page: ChromePage) -> None:
    """Close the first-visit asset sidebar tour before taking evidence."""
    if not await page.evaluate("document.body.innerText.includes('Introducing the Asset Sidebar')"):
        return
    await page.evaluate(
        "(() => { const element = document.elementFromPoint(1110, 143);"
        "(element?.closest('button') || element)?.click(); })()"
    )
    for _ in range(10):
        if not await page.evaluate(
            "document.body.innerText.includes('Introducing the Asset Sidebar')"
        ):
            return
        await asyncio.sleep(0.5)
    raise RuntimeError("Could not dismiss DataHub asset sidebar tour")


async def collapse_sidebar(page: ChromePage) -> None:
    """Give the lineage canvas room to show every node in the screenshot."""
    if not await page.evaluate("document.body.innerText.includes('No owners yet.')"):
        return
    await page.evaluate(
        "(() => { const element = document.elementFromPoint(1628, 101);"
        "(element?.closest('button') || element)?.dispatchEvent("
        "new MouseEvent('click', {bubbles:true})); })()"
    )
    await asyncio.sleep(0.5)


async def capture(inspect_only: bool, profile_name: str = "legacy") -> None:
    """Log in and capture Lineage and Quality screenshots for one dataset profile."""
    datasets, output = PROFILES[profile_name]
    if not inspect_only:
        output.mkdir(parents=True, exist_ok=True)
    async with chrome_page(disable_dev_shm_usage=True) as page:
        await login(page)
        for dp, name in datasets.items():
            for route, suffix in (("Lineage", "lineage"), ("Quality", "validation")):
                await page.navigate(dataset_url(name, route))
                body = await wait_for_text(page, name.split(".")[-1])
                if route == "Lineage":
                    body = await wait_for_text(page, "Airflow | Task")
                else:
                    body = await wait_for_text(page, "1 Passing")
                await dismiss_onboarding(page)
                if route == "Lineage":
                    await collapse_sidebar(page)
                await asyncio.sleep(3)
                url = await page.evaluate("location.href")
                if inspect_only:
                    print(f"{dp} {suffix}: {url}\n{body[:1500]}\n")
                    continue
                image = await page.call(
                    "Page.captureScreenshot",
                    {
                        "format": "png",
                        "captureBeyondViewport": False,
                    },
                )
                path = output / f"{dp}-{suffix}.png"
                path.write_bytes(base64.b64decode(image["data"]))
                print(f"Captured {path.relative_to(ROOT)}")


def main() -> None:
    """Run the browser capture or inspect the UI routes without writing images."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inspect-only", action="store_true")
    parser.add_argument(
        "--profile",
        choices=sorted(PROFILES),
        default="legacy",
        help="legacy = the six historical images; v2 = CDC v2 and DP3 re-run, separate directory",
    )
    args = parser.parse_args()
    asyncio.run(capture(args.inspect_only, args.profile))


if __name__ == "__main__":
    main()
