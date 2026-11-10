"""Skew measurement on AWS: spark-submit runs in the airflow container, s3a source and AWS packages from container variables."""

import os
import stat
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]


def test_aws_mode_submits_from_airflow_with_s3_source(tmp_path):
    """skew_b on AWS: docker compose exec airflow, --source from a variable, distribution-mode=none preserved."""
    calls = tmp_path / "calls"
    fake = tmp_path / "bin" / "docker"
    fake.parent.mkdir()
    fake.write_text(f'#!/bin/sh\nprintf "%s\\0" "$@" >> {calls}\n')
    fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
    env = {
        **os.environ,
        "PATH": f"{fake.parent}:{os.environ['PATH']}",
        "PLATFORM_STORAGE": "s3",
        "LOG_DIR": str(tmp_path / "logs"),
    }
    subprocess.run(
        ["bash", "scripts/evidence/measure_order_snapshots_bronze.sh", "skew_b"],
        cwd=ROOT,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    args = calls.read_text().split("\0")[:-1]
    assert args[:4] == ["compose", "exec", "-T", "airflow"]
    script = args[args.index("-c") + 1]
    assert '--source "$AIRFLOW_VAR_PLATFORM_DP1_AMAZON_SOURCE"' in script
    assert '--packages "$AIRFLOW_VAR_PLATFORM_SPARK_PACKAGES"' in script
    assert "write.distribution-mode=none" in script
    assert "raw_amazon_orders_skew_b" in args and "bronze/ingest_amazon_order_snapshots.py" in args
