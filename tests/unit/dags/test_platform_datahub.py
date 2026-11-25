"""DataHub reporting of validator results: urns, payloads, and that DataHub never decides the task result."""

from __future__ import annotations

from types import SimpleNamespace

from dags import platform_datahub
from dags.platform_datahub import REPORT, UPSERT, assertion_urn, report, report_task
from spark_jobs.lib.assertion_result import contract_schema_urn, emit, parse


def test_emit_and_parse_round_trip(capsys):
    """The printed line is the record; other output lines are ignored."""
    emit(
        "lakehouse.bronze.raw_amazon_orders_dp1", False, {"invalid_rows": 3}, "rows match", name="x"
    )
    record = parse(capsys.readouterr().out)
    assert record["passed"] is False and record["metrics"] == {"invalid_rows": 3}
    assert parse("INFO SparkContext: Running Spark version 3.5.3") is None


def test_urns_keep_the_old_per_task_name_and_split_named_checks():
    """One check per task keeps platform-<dag>-<task> (history continues); named checks get a suffix."""
    base = {"name": None, "urn": None}
    assert assertion_urn("dp2_silver", "validate_amazon_silver", base) == (
        "urn:li:assertion:platform-dp2_silver-validate_amazon_silver"
    )
    named = {**base, "name": "unique_order_line_key"}
    assert assertion_urn("dp2_silver", "validate_gold_order_lines", named).endswith(
        "validate_gold_order_lines-unique_order_line_key"
    )
    owned = {**base, "urn": contract_schema_urn("amazon")}
    assert assertion_urn("dp1_bronze", "validate_amazon", owned) == contract_schema_urn("amazon")


def test_report_defines_custom_assertions_but_not_contract_owned_ones():
    """Contract schema assertions are defined by the CI publisher; a run only adds its result."""
    calls = []
    records = [
        {
            "dataset": "lakehouse.bronze.raw_amazon_orders_dp1",
            "passed": True,
            "metrics": {"rows": 5},
            "logic": "rows",
            "name": None,
            "urn": None,
        },
        {
            "dataset": "lakehouse.bronze.raw_amazon_orders_dp1",
            "passed": False,
            "metrics": {"contract_rows": 2},
            "logic": "schema",
            "name": None,
            "urn": contract_schema_urn("amazon"),
        },
    ]
    report(
        records,
        "dp1_bronze",
        "validate_amazon",
        "manual__1",
        "http://log",
        lambda q, v: calls.append((q, v)) or {},
    )
    assert [q for q, _ in calls] == [UPSERT, REPORT, REPORT]
    upsert = calls[0][1]
    assert upsert["entityUrn"] == (
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,lakehouse.bronze.raw_amazon_orders_dp1,PROD)"
    )
    assert calls[1][1]["type"] == "SUCCESS" and calls[2][1]["type"] == "FAILURE"
    assert {"key": "run_id", "value": "manual__1"} in calls[2][1]["properties"]
    assert {"key": "contract_rows", "value": "2"} in calls[2][1]["properties"]


def test_report_task_never_raises(monkeypatch, caplog):
    """No DATAHUB_GMS_URL, or DataHub down: a warning, the task result is untouched."""
    context = {"ti": SimpleNamespace(dag_id="d", task_id="t", log_url=None), "run_id": "r"}
    record = {
        "dataset": "lakehouse.x",
        "passed": True,
        "metrics": {},
        "logic": "",
        "name": None,
        "urn": None,
    }
    monkeypatch.delenv("DATAHUB_GMS_URL", raising=False)
    report_task([record], context)
    assert "not set" in caplog.text

    def down(server, token):
        def post(query, variables):
            raise OSError("connection refused")

        return post

    monkeypatch.setenv("DATAHUB_GMS_URL", "http://datahub:8080")
    monkeypatch.setattr(platform_datahub, "graphql_post", down)
    report_task([record], context)
    assert "Could not send" in caplog.text
