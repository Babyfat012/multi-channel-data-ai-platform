"""Data contracts demo without Docker: three fixtures, the infer/diff/gate CLI, and evidence with source hashes.

Needs the repo's Python environment (generator + spark_jobs.lib):

    uv run python -m scripts.data.run_data_contracts_demo
"""

import hashlib
import json
import os
import subprocess
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from scripts.common import ROOT

CLI = [sys.executable, "-m", "spark_jobs.lib.data_contracts"]
FIXTURES = [
    ("default", []),
    ("shopee=2", ["--native-schema-for", "shopee=2"]),
    ("tiktok_shop=2", ["--native-schema-for", "tiktok_shop=2"]),
]
TRACKED_SOURCES = [
    "generator/generate_order_lifecycle_fixture.py",
    "spark_jobs/lib/order_lifecycle_lakehouse.py",
    "spark_jobs/bronze/ingest_order_lifecycle.py",
    "spark_jobs/silver/build_order_lifecycle.py",
    "scripts/data/run_data_contracts_demo.py",
    "scripts/data/run_order_lifecycle_spark.py",
    "docker-compose.yml",
    "tests/unit/spark_jobs/lib/test_data_contracts.py",
    "tests/spark/check_order_lifecycle_silver.py",
]


class Recorder:
    """Run commands and keep their full output and exit codes for the evidence file."""

    def __init__(self):
        """Start with no recorded commands."""
        self.commands = []

    def __call__(self, args: list[str]):
        """Run, record, and return parsed JSON stdout; only `diff` may exit 1 (breaking change found)."""
        result = subprocess.run(args, text=True, capture_output=True)
        self.commands.append(
            {
                "command": args,
                "exit_code": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
            }
        )
        if result.returncode not in (0, 1) or (result.returncode and "diff" not in args):
            raise RuntimeError(json.dumps(self.commands[-1], ensure_ascii=False))
        return json.loads(result.stdout) if result.stdout.strip() else None


def main() -> None:
    """Gate three fixtures, diff the shopee schema change, write a receipt and print a summary table."""
    os.chdir(ROOT)
    run = Recorder()
    rows = []
    with tempfile.TemporaryDirectory(prefix="platform-contracts-") as directory:
        root = Path(directory)
        run([*CLI, "check-registry", "--contracts-dir", "contracts"])
        for name, overrides in FIXTURES:
            fixture = root / name
            run(
                [
                    sys.executable,
                    "-m",
                    "generator.generate_order_lifecycle_fixture",
                    "--output",
                    str(fixture),
                    *overrides,
                ]
            )
            report = run([*CLI, "gate", str(fixture), "--contracts-dir", "contracts"])
            rows.append({"fixture": name, "accepted": report["accepted"], **report["quarantine"]})
            if name == "shopee=2":
                manifest = json.loads((fixture / "manifest.json").read_text())
                files = [
                    str(fixture / f["path"])
                    for f in manifest["files"]
                    if f["feed"] == "shopee.orders"
                ]
                inferred = root / "shopee2.schema.json"
                run([*CLI, "infer", *files, "--output", str(inferred)])
                comparison = run(
                    [
                        *CLI,
                        "diff",
                        "contracts/schemas/sim-native-v2/shopee/1.schema.json",
                        str(inferred),
                    ]
                )
    sources = [Path(p) for p in TRACKED_SOURCES]
    sources += sorted(Path("spark_jobs/lib/data_contracts").glob("*.py")) + sorted(
        Path("contracts").rglob("*.json")
    )
    evidence = {
        "description": "Data contracts: Python fixture measurements, not a claim that Spark ran",
        "git_head": subprocess.run(
            ["git", "rev-parse", "HEAD"], check=True, capture_output=True, text=True
        ).stdout.strip(),
        "commands": run.commands,
        "sha256": {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in sources},
        "results": rows,
        "shopee_diff": comparison,
        "spark": {
            "status": "not run",
            "reason": "This demo only runs Python; Spark is checked separately.",
        },
    }
    # Each run writes a new receipt; committed historical evidence is left untouched.
    path = Path("out/data-contracts-evidence") / datetime.now(UTC).strftime(
        "receipt-%Y%m%dT%H%M%S.%fZ.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + "\n")
    print("check-registry: PASS (exit 0)")
    print("shopee/1 -> shopee/2 BACKWARD: breaking (exit 1)")
    for change in comparison["changes"]:
        print(f"  {change['kind']}: {change['path']}")
    print("fixture         accepted  quarantined  reason")
    for row in rows:
        print(
            f"{row['fixture']:<15} {row['accepted']:>8} {row['total']:>12}  {json.dumps(row['by_reason'], sort_keys=True)}"
        )
    print(f"evidence: {path}")


if __name__ == "__main__":
    main()
