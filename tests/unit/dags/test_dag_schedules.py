"""Schedules from the system diagram, checked without Airflow in the venv (ast + source).

nightly chain (paused on creation) → dp3_return_labels emits GOLD_LABELS → ml_feature_materialize; dp1_cdc hourly.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

DAGS = Path(__file__).resolve().parents[3] / "dags"


def dag_kwargs(name: str) -> dict[str, str]:
    """Keyword arguments of the `DAG(...)` call in dags/<name>.py, as source text."""
    tree = ast.parse((DAGS / f"{name}.py").read_text())
    call = next(
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.Call) and getattr(n.func, "id", None) == "DAG"
    )
    return {k.arg: ast.unparse(k.value) for k in call.keywords}


def test_nightly_chain_runs_labels_last_and_starts_paused():
    """Labels last: the ML refresh they start must not overlap the other Spark DAGs (shared driver RAM)."""
    text = (DAGS / "dp_nightly.py").read_text()
    chain = ast.literal_eval(re.search(r"^CHAIN = (\[.*\])$", text, re.M).group(1))
    assert chain == [
        "dp1_bronze",
        "dp2_silver",
        "dp3_features",
        "dp4_warehouse",
        "dp3_return_labels",
    ]
    kwargs = dag_kwargs("dp_nightly")
    assert kwargs["schedule"] == "'0 1 * * *'"
    assert "Asia/Ho_Chi_Minh" in kwargs["start_date"]
    assert kwargs["is_paused_upon_creation"] == "True" and kwargs["catchup"] == "False"
    assert kwargs["max_active_runs"] == "1"
    assert "wait_for_completion=True" in text and 'failed_states=["failed"]' in text
    assert "previous >> task" in text


def test_children_stay_manual_so_nothing_runs_twice():
    """The chain triggers them; their own schedule must stay None, or they would also run on their own."""
    for child in ("dp1_bronze", "dp2_silver", "dp3_features", "dp4_warehouse", "dp3_return_labels"):
        assert dag_kwargs(child)["schedule"] == "None", child


def test_ml_refresh_waits_for_the_labels_dataset_from_one_shared_object():
    """Producer and consumer import the same Dataset; no URI typed a second time anywhere in dags/."""
    assert dag_kwargs("ml_feature_materialize")["schedule"] == "[GOLD_LABELS]"
    labels = (DAGS / "dp3_return_labels.py").read_text()
    assert "outlets=[GOLD_LABELS]" in labels
    for name in ("dp3_return_labels", "ml_feature_materialize"):
        assert "from platform_datasets import GOLD_LABELS" in (DAGS / f"{name}.py").read_text()
    uris = [p.name for p in DAGS.glob("*.py") if "iceberg://" in p.read_text()]
    assert uris == ["platform_datasets.py"]


def test_cdc_runs_hourly():
    """Diagram "every hour"; safe to repeat because the ingest MERGEs on the Kafka offset."""
    assert dag_kwargs("dp1_cdc")["schedule"] == "'@hourly'"
    assert (
        "MERGE INTO" in (DAGS.parent / "spark_jobs/cdc/ingest_walmart_order_changes.py").read_text()
    )
