"""One pipeline: Airflow ignores the path B DAGs, still loads dp1–dp4 and the remaining DAGs."""

from __future__ import annotations

import re
from pathlib import Path

DAGS = Path(__file__).resolve().parents[3] / "dags"


def test_only_order_lifecycle_dags_are_ignored():
    """Airflow 2.10 default regexp syntax: # lines are comments, matching is on relative paths."""
    lines = (DAGS / ".airflowignore").read_text().splitlines()
    patterns = [re.compile(p) for p in lines if p.strip() and not p.startswith("#")]
    ignored = {f.name for f in DAGS.glob("*.py") if any(p.search(f.name) for p in patterns)}
    assert ignored == {
        "dp_order_lifecycle.py",
        "dp_order_lifecycle_cdc.py",
        "dp_order_lifecycle_warehouse.py",
    }
