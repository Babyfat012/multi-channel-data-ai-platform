"""Materialize: full_reload reloads everything when Redis was wiped (incremental would load 0 rows and still go green)."""

from __future__ import annotations

import ast
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pytest

from feature_repo.check_online_store import check_incremental_online_store

ROOT = Path(__file__).resolve().parents[3]
DAG = (ROOT / "dags/ml_feature_materialize.py").read_text()


def _rewrite(args, params):
    """Run only run_feast's command-rewrite branch (no Airflow needed)."""
    fn = next(
        n for n in ast.parse(DAG).body if isinstance(n, ast.FunctionDef) and n.name == "run_feast"
    )
    branch = next(n for n in fn.body if isinstance(n, ast.If))
    scope = {"args": args, "params": params, "FULL_RELOAD_START": "S"}
    exec(compile(ast.Module(body=[branch], type_ignores=[]), "run_feast", "exec"), scope)
    return scope["args"]


def test_full_reload_switches_to_ranged_materialize():
    """full_reload on ⇒ `materialize --views v START END`; off or another command ⇒ unchanged."""
    inc = ("materialize-incremental", "--views", "sku_return_daily", "E")
    assert _rewrite(inc, {"full_reload": True}) == (
        "materialize",
        "--views",
        "sku_return_daily",
        "S",
        "E",
    )
    assert _rewrite(inc, {"full_reload": False}) == inc
    assert _rewrite(inc, None) == inc
    assert _rewrite(("apply",), {"full_reload": True}) == ("apply",)
    parameter = next(
        value
        for node in ast.walk(ast.parse(DAG))
        if isinstance(node, ast.Dict)
        for key, value in zip(node.keys, node.values, strict=True)
        if isinstance(key, ast.Constant) and key.value == "full_reload"
    )
    assert ast.literal_eval(parameter.args[0]) is False
    assert {kw.arg: ast.literal_eval(kw.value) for kw in parameter.keywords}["type"] == "boolean"


def test_run_feast_signature_takes_context_only_as_kwargs():
    """`def run_feast(*args, params=None)` ⇒ Airflow 2.10 raises 'params ... reserved' with 4 op_args."""
    assert "def run_feast(*args, **context):" in DAG


@pytest.mark.parametrize(
    ("keys", "last_end", "blocked"),
    [(0, None, False), (2, None, False), (0, "2026-06-30", True), (2, "2026-06-30", False)],
)
def test_guard_distinguishes_bootstrap_from_lost_online_data(keys, last_end, blocked):
    """The first load is allowed; a total loss after materialize must fail clearly, never self-correcting the watermark."""
    store = Mock()
    store.get_feature_view.return_value.most_recent_end_time = last_end
    redis = Mock()
    redis.dbsize.return_value = keys
    if blocked:
        with pytest.raises(RuntimeError, match="full_reload=true"):
            check_incremental_online_store(store, redis)
    else:
        check_incremental_online_store(store, redis)
    store.get_feature_view.assert_called_once_with("sku_return_daily")
    redis.dbsize.assert_called_once_with()


@pytest.mark.parametrize("failure", ["registry", "redis"])
def test_guard_does_not_treat_dependency_failure_as_first_load(failure):
    """If the registry/Redis cannot be read, fail; never interpret it as never loaded."""
    store, redis = Mock(), Mock()
    store.get_feature_view.return_value.most_recent_end_time = None
    if failure == "registry":
        store.get_feature_view.side_effect = ConnectionError("registry unavailable")
    else:
        redis.dbsize.side_effect = ConnectionError("redis unavailable")
    with pytest.raises(ConnectionError, match="unavailable"):
        check_incremental_online_store(store, redis)


def _run_feast_with_fake_runtime():
    """Run the whole DAG function with fake process/credentials, without installing Airflow or Feast."""
    fn = next(
        n for n in ast.parse(DAG).body if isinstance(n, ast.FunctionDef) and n.name == "run_feast"
    )
    run = Mock(return_value=SimpleNamespace(stdout="", stderr="", check_returncode=Mock()))
    directory = MagicMock()
    directory.return_value.__enter__.return_value = "/tmp/guard-test"
    variables = {"platform_feast_root": "s3://test/features", "platform_feast_redis": "redis:6379"}
    scope = {
        "os": SimpleNamespace(environ={"PLATFORM_STORAGE": "s3"}),
        "Variable": SimpleNamespace(get=variables.__getitem__),
        "subprocess": SimpleNamespace(run=run),
        "tempfile": SimpleNamespace(TemporaryDirectory=directory),
        "shutil": SimpleNamespace(copytree=Mock(return_value="/tmp/guard-test/repo")),
        "REPO": "/opt/feature_repo",
        "FEAST": "feast",
        "FEAST_PYTHON": "feast-python",
        "FULL_RELOAD_START": "S",
        "ENSURE_24H": "ensure_24h",
    }
    exec(compile(ast.Module(body=[fn], type_ignores=[]), "run_feast", "exec"), scope)
    return scope["run_feast"], run


@pytest.mark.parametrize("full_reload", [False, True])
def test_dag_guards_incremental_but_allows_explicit_full_reload(full_reload):
    """The guard runs before the CLI; a deliberate full_reload request must reach the ranged materialize."""
    run_feast, run = _run_feast_with_fake_runtime()
    run_feast(
        "materialize-incremental",
        "--views",
        "sku_return_daily",
        "E",
        params={"full_reload": full_reload},
    )
    commands = [call.args[0] for call in run.call_args_list]
    if full_reload:
        assert commands == [["feast", "materialize", "--views", "sku_return_daily", "S", "E"]]
    else:
        assert commands == [
            ["feast-python", "-m", "check_online_store"],
            ["feast", "materialize-incremental", "--views", "sku_return_daily", "E"],
        ]
        assert run.call_args_list[0].kwargs["check"] is True
        assert run.call_args_list[0].kwargs["timeout"] == 120
        assert run.call_args_list[0].kwargs["env"] == run.call_args_list[1].kwargs["env"]


def test_failed_guard_prevents_materialization():
    """A helper reporting an error must stop the task before the materialize CLI is called."""
    run_feast, run = _run_feast_with_fake_runtime()
    run.side_effect = subprocess.CalledProcessError(1, "check_online_store")
    with pytest.raises(subprocess.CalledProcessError):
        run_feast("materialize-incremental", "--views", "sku_return_daily", "E")
    assert [call.args[0] for call in run.call_args_list] == [
        ["feast-python", "-m", "check_online_store"]
    ]


def test_feast_apply_excludes_guard_helper():
    """Feast apply imports only feature definitions, not the check script."""
    assert "check_online_store.py" in (ROOT / "feature_repo/.feastignore").read_text().splitlines()


@pytest.mark.parametrize("full_reload", [False, True])
def test_publication_operator_invokes_isolated_publisher(full_reload):
    """The new DAG path calls the publisher once with the right root and recovery flag."""
    run_feast, run = _run_feast_with_fake_runtime()
    run_feast("publish-batch", "--end", "E", params={"full_reload": full_reload})
    command = ["feast-python", "-m", "publish_batch", "--end", "E"]
    if full_reload:
        command.append("--full-reload")
    run.assert_called_once()
    assert run.call_args.args[0] == command
    assert (
        run.call_args.kwargs["env"]["FEAST_PUBLICATION_ROOT"] == "s3://test/features/publications"
    )
    assert run.call_args.kwargs["check"] is True
