#!/usr/bin/env bash
# Data contracts demo without Docker: three fixtures, infer/diff/gate CLI, and evidence with source hashes.
set -euo pipefail
cd "$(dirname "$0")/../.."
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"
exec .venv/bin/python - <<'PY'
"""Run the demo, keep output/exit codes verbatim, and print a table of real measurements."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile

repo = Path.cwd()
commands = []


def run(args):
    """Record commands and full output; a breaking diff is allowed to return 1."""
    result = subprocess.run(args, text=True, capture_output=True)
    commands.append({"command": args, "exit_code": result.returncode,
                     "stdout": result.stdout, "stderr": result.stderr})
    if result.returncode not in (0, 1) or (result.returncode and "diff" not in args):
        raise RuntimeError(json.dumps(commands[-1], ensure_ascii=False))
    return json.loads(result.stdout) if result.stdout.strip() else None


with tempfile.TemporaryDirectory(prefix="platform-contracts-") as directory:
    root = Path(directory)
    cli = [sys.executable, "-m", "spark_jobs.lib.data_contracts"]
    run([*cli, "check-registry", "--contracts-dir", "contracts"])
    rows = []
    for name, overrides in [("default", []), ("shopee=2", ["--native-schema-for", "shopee=2"]),
                            ("tiktok_shop=2", ["--native-schema-for", "tiktok_shop=2"])]:
        fixture = root / name
        run([sys.executable, "-m", "generator.generate_order_lifecycle_fixture", "--output", str(fixture), *overrides])
        report = run([*cli, "gate", str(fixture), "--contracts-dir", "contracts"])
        rows.append({"fixture": name, "accepted": report["accepted"], **report["quarantine"]})
        if name == "shopee=2":
            manifest = json.loads((fixture / "manifest.json").read_text())
            files = [str(fixture / f["path"]) for f in manifest["files"] if f["feed"] == "shopee.orders"]
            inferred = root / "shopee2.schema.json"
            run([*cli, "infer", *files, "--output", str(inferred)])
            comparison = run([*cli, "diff", "contracts/schemas/sim-native-v2/shopee/1.schema.json", str(inferred)])
    tracked_sources = [Path("generator/generate_order_lifecycle_fixture.py"), Path("spark_jobs/lib/order_lifecycle_lakehouse.py"),
                       Path("spark_jobs/bronze/ingest_order_lifecycle.py"), Path("spark_jobs/silver/build_order_lifecycle.py"),
                       Path("scripts/run/run_data_contracts_demo.sh"), Path("scripts/run/run_order_lifecycle_spark.sh"),
                       Path("docker-compose.yml"), Path("tests/unit/spark_jobs/lib/test_data_contracts.py"), Path("tests/spark/check_order_lifecycle_silver.py")]
    tracked_sources += sorted(Path("spark_jobs/lib/data_contracts").glob("*.py")) + sorted(Path("contracts").rglob("*.json"))
    hashes = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in tracked_sources}
    head = subprocess.run(["git", "rev-parse", "HEAD"], check=True, capture_output=True, text=True).stdout.strip()
    evidence = {"description": "Data contracts: Python fixture measurements, not a claim that Spark ran",
                "git_head": head, "commands": commands, "sha256": hashes, "results": rows,
                "shopee_diff": comparison, "spark": {"status": "not run",
                "reason": "This demo only runs Python; Spark is checked separately."}}
    path = Path("out/data-contracts-evidence") / (datetime.now(timezone.utc).strftime("receipt-%Y%m%dT%H%M%S.%fZ.json"))
    # Each run creates a new receipt; committed historical evidence is left untouched.
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + "\n")
    print("check-registry: PASS (exit 0)")
    print("shopee/1 -> shopee/2 BACKWARD: breaking (exit 1)")
    for change in comparison["changes"]:
        print(f"  {change['kind']}: {change['path']}")
    print("fixture         accepted  quarantined  reason")
    for row in rows:
        print(f"{row['fixture']:<15} {row['accepted']:>8} {row['total']:>12}  {json.dumps(row['by_reason'], sort_keys=True)}")
    print(f"evidence: {path}")
PY
