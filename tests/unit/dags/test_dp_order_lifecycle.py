"""Check the v2 DAG contract via AST, without installing Airflow or running Spark."""

import ast
import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
DAG = ROOT / "dags/dp_order_lifecycle.py"
ORDER = ["check_contracts", "ingest_bronze", "build_silver", "build_gold", "validate_gold"]


def literal(node, names):
    """Resolve constants and static args lists to compare against the real CLI."""
    if isinstance(node, ast.Name):
        return literal(names[node.id], names)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        return literal(node.left, names) + literal(node.right, names)
    if isinstance(node, (ast.List, ast.Tuple)):
        return [literal(item, names) for item in node.elts]
    return ast.literal_eval(node)


def test_dag_contract_and_real_job_flags():
    """The paused DAG has all five stages, the right CLI, Variables and run order."""
    source = DAG.read_text()
    tree = ast.parse(source)
    names = {
        n.targets[0].id: n.value
        for n in ast.walk(tree)
        if isinstance(n, ast.Assign) and isinstance(n.targets[0], ast.Name)
    }
    dag = next(
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == "DAG"
    )
    options = {k.arg: k.value for k in dag.keywords}
    for key, want in {
        "dag_id": "dp_order_lifecycle",
        "schedule": None,
        "catchup": False,
        "is_paused_upon_creation": True,
        "max_active_runs": 1,
    }.items():
        assert literal(options[key], names) == want
    tasks = {}
    for name, value in names.items():
        if not isinstance(value, ast.Call):
            continue
        kwargs = {k.arg: k.value for k in value.keywords}
        if "task_id" in kwargs:
            tasks[name] = literal(kwargs["task_id"], names)
        if "application" not in kwargs:
            continue
        application = ROOT / Path(literal(kwargs["application"], names)).relative_to("/opt")
        assert application.is_file()
        job = ast.parse(application.read_text())
        flags = {
            a.value
            for n in ast.walk(job)
            if isinstance(n, ast.Call)
            and isinstance(n.func, ast.Attribute)
            and n.func.attr == "add_argument"
            for a in n.args
            if isinstance(a, ast.Constant) and isinstance(a.value, str)
        }
        args = literal(kwargs["application_args"], names)
        assert {a for a in args if a.startswith("--")} <= flags
        assert literal(kwargs["conn_id"], names) == "spark_default"
        assert literal(kwargs["deploy_mode"], names) == "client"
        assert "platform_spark_packages" in literal(kwargs["packages"], names)
        if tasks[name] == "ingest_bronze":
            assert args[args.index("--receipt") + 1] == literal(kwargs["receipt_path"], names)
        if tasks[name] == "build_silver":
            assert {"--as-of", "--contracts-dir", "--receipt"} <= set(args)
    assert sorted(tasks.values()) == sorted(ORDER)
    chain = next(
        n.value
        for n in tree.body[-1].body
        if isinstance(n, ast.Expr)
        and isinstance(n.value, ast.BinOp)
        and isinstance(n.value.op, ast.RShift)
    )

    def flatten(node):
        """Read the dependency chain written with Airflow's right-shift operator."""
        if isinstance(node, ast.Name):
            return [tasks[node.id]]
        return flatten(node.left) + flatten(node.right)

    assert flatten(chain) == ORDER
    used = set(re.findall(r"var\.value\.(platform_\w+)", source))
    setup = (ROOT / "scripts/setup/setup_airflow.sh").read_text()
    declared = set(re.findall(r"variables set (platform_\w+)", setup))
    assert used <= declared
    assert {
        "platform_order_lifecycle_source_root",
        "platform_order_lifecycle_cutoff",
        "platform_order_lifecycle_receipt_dir",
    } <= used
    assert '"source_root"' in source and '"cutoff"' in source and '"walmart_jdbc"' in source


def test_v2_resource_variables_and_templates():
    """Every v2 Spark task takes its three resources from Variables, including the inherited Bronze."""
    resources = {"driver_memory": "3g", "executor_memory": "4g", "executor_cores": "2"}
    setup = (ROOT / "scripts/setup/setup_airflow.sh").read_text()
    for field, default in resources.items():
        assert f"variables set platform_order_lifecycle_{field} '{default}'" in setup
    for path in (DAG, ROOT / "dags/dp_order_lifecycle_warehouse.py"):
        source = path.read_text()
        tree = ast.parse(source)
        assert not any(isinstance(n, ast.Constant) and n.value == "1g" for n in ast.walk(tree))
        classes = {n.name: n for n in tree.body if isinstance(n, ast.ClassDef)}
        resource_class = classes["OrderLifecycleSparkSubmitOperator"]
        assert [base.id for base in resource_class.bases] == ["PlatformSparkSubmitOperator"]
        assignment = next(
            n
            for n in resource_class.body
            if isinstance(n, ast.Assign) and n.targets[0].id == "template_fields"
        )
        # The provider stores the parameters in `_driver_memory`...; templating the public names would fail.
        assert {f"_{field}" for field in resources} <= {
            n.value
            for n in ast.walk(assignment)
            if isinstance(n, ast.Constant) and isinstance(n.value, str)
        }
        assert any(
            isinstance(n, ast.Attribute)
            and n.attr == "template_fields"
            and isinstance(n.value, ast.Name)
            and n.value.id == "PlatformSparkSubmitOperator"
            for n in ast.walk(assignment)
        )
        count = 0
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            kwargs = {k.arg: k.value for k in node.keywords}
            if "application" not in kwargs:
                continue
            count += 1
            assert node.func.id in {
                "OrderLifecycleSparkSubmitOperator",
                "OrderLifecycleBronzeOperator",
            }
            for field in resources:
                assert (
                    ast.literal_eval(kwargs[field])
                    == "{{ var.value.platform_order_lifecycle_" + field + " }}"
                )
        assert count == (4 if path == DAG else 2)
        if path == DAG:
            bronze = classes["OrderLifecycleBronzeOperator"]
            assert [base.id for base in bronze.bases] == ["OrderLifecycleSparkSubmitOperator"]
            assert any(
                isinstance(n, ast.Attribute)
                and n.attr == "template_fields"
                and isinstance(n.value, ast.Name)
                and n.value.id == "OrderLifecycleSparkSubmitOperator"
                for n in ast.walk(bronze)
            )


@pytest.mark.parametrize("fails", [False, True])
@pytest.mark.parametrize("jdbc", ["", "walmart_lifecycle.orders"])
def test_bronze_preserves_spark_receipt_and_propagates_failure(tmp_path, fails, jdbc):
    """Load the class via AST with a Spark stub: success keeps the schema, failure writes a failure receipt."""
    receipt = tmp_path / "ingest_bronze.json"
    original_args = ["--root", "/fixture", "--receipt", str(receipt)]
    error = RuntimeError("Spark failed")
    spark_receipt = {
        "run_id": "run-test",
        "dataset_version": "dataset",
        "rows": 42,
        "schema_file": "schemas.json",
        "completed_at": "2026-10-19T12:00:00Z",
    }
    encoded = json.dumps(spark_receipt).encode()

    class SparkStub:
        """Simulate only Spark's execute boundary, without importing Airflow."""

        template_fields = ()

        def __init__(self, application_args):
            """Keep the command-line arguments the DAG passes to Spark."""
            self.application_args = application_args

        def execute(self, context):
            """Check the arguments, then write a fake receipt in place of the Spark job."""
            assert context == {"run_id": "run-test"}
            assert self.application_args == original_args + (
                ["--walmart-jdbc", jdbc] if jdbc else []
            )
            receipt.write_bytes(encoded)
            if fails:
                raise error
            return "spark-result"

    tree = ast.parse(DAG.read_text())
    selected = [
        n
        for n in tree.body
        if (isinstance(n, ast.ImportFrom) and n.module in {"datetime", "pathlib"})
        or (isinstance(n, ast.Import) and any(a.name == "json" for a in n.names))
        or (isinstance(n, ast.FunctionDef) and n.name == "write_receipt")
        or (isinstance(n, ast.ClassDef) and n.name == "OrderLifecycleBronzeOperator")
    ]
    namespace = {"OrderLifecycleSparkSubmitOperator": SparkStub}
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(DAG), "exec"), namespace)
    operator = namespace["OrderLifecycleBronzeOperator"](
        receipt_path=receipt, walmart_jdbc=jdbc, application_args=original_args
    )
    if fails:
        with pytest.raises(RuntimeError) as caught:
            operator.execute({"run_id": "run-test"})
        assert caught.value is error
        saved = json.loads(receipt.read_text())
        assert saved["status"] == "failed"
        assert saved["run_id"] == "run-test"
        assert saved["checks_available"] is False
        assert saved["completed_at"]
        assert "schema_file" not in saved
    else:
        assert operator.execute({"run_id": "run-test"}) == "spark-result"
        assert receipt.read_bytes() == encoded
    assert operator.application_args is original_args
