"""The dp1_bronze DAG passes the Amazon source to both ingest and validate (so AWS can point at s3a://)."""

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def test_amazon_tasks_pass_same_source():
    """Both Amazon tasks get --source AMAZON_SOURCE; missing it in validate would count the local mount by mistake."""
    tree = ast.parse((ROOT / "dags/dp1_bronze.py").read_text())
    tasks = {}
    for call in ast.walk(tree):
        if isinstance(call, ast.Call) and getattr(call.func, "id", "") == "SparkSubmitOperator":
            kw = {k.arg: k.value for k in call.keywords}
            if isinstance(kw.get("task_id"), ast.Constant):
                args = [
                    getattr(a, "value", getattr(a, "id", None)) for a in kw["application_args"].elts
                ]
                tasks[kw["task_id"].value] = args
    for task in ("ingest_amazon", "validate_amazon"):
        i = tasks[task].index("--source")
        assert tasks[task][i + 1] == "AMAZON_SOURCE"
