"""Test the DataHub evidence parser and publish rules with old and new CDC/feature logs."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[4]
SCRIPTS = Path(__file__).resolve().parents[4] / "scripts"


def load(name: str):
    """Load a script under scripts/ as a module, without installing a package."""
    spec = importlib.util.spec_from_file_location(name, ROOT / (name.replace(".", "/") + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


builder = load("scripts.evidence.build_order_snapshots_datahub_validation")
publisher = load("scripts.run.publish_order_snapshots_datahub")

CDC_V1_LOG = "Kafka=6, Bronze=6, unique offsets=6, missing=0, invalid=0\n"
CDC_V2_LOG = (
    "Kafka=6, Bronze=6, unique offsets=6, missing=0, extra=0, payload mismatch=0, invalid=0\n"
    "Validate CDC Bronze: PASS\n"
)
FEATURE_V1_LOG = "Gold fact=10, feature=10, feature invalid=0, duplicate_order_line_key=0\n"
FEATURE_V2 = dict(
    fact_rows=10,
    feature_rows=10,
    invalid=0,
    duplicate_keys=0,
    source_invalid=0,
    source_duplicates=0,
    missing=0,
    extra=0,
    wrong_event_time=0,
)


def feature_v2_log(**changes) -> str:
    """Log matching the `print(result)` line of spark_jobs/gold/validate_order_snapshot_return_features.py."""
    return f"[2026-10-18] INFO - {FEATURE_V2 | changes}\nValidate feature offline: PASS\n"


def test_cdc_v1_log_keeps_legacy_metrics():
    """Check the CDC v1 parser keeps the legacy metrics."""
    metrics, _, log_format = builder.metrics_for("dp1_cdc", "validate_walmart_cdc", CDC_V1_LOG)
    assert log_format == "cdc-v1"
    assert metrics == {
        "source_rows": 6,
        "dataset_rows": 6,
        "unique_offsets": 6,
        "missing_rows": 0,
        "invalid_rows": 0,
    }


def test_cdc_v2_log_adds_extra_and_payload():
    """Check the CDC v2 parser also reads the extra and payload metrics."""
    metrics, rule, log_format = builder.metrics_for("dp1_cdc", "validate_walmart_cdc", CDC_V2_LOG)
    assert log_format == "cdc-v2"
    assert metrics["extra_rows"] == 0 and metrics["payload_mismatch_rows"] == 0
    assert "payload" in rule


@pytest.mark.parametrize("field", ["extra", "payload mismatch"])
def test_cdc_v2_nonzero_failure_is_rejected(field):
    """Check CDC v2 is rejected when a failure metric is nonzero."""
    log = CDC_V2_LOG.replace(f"{field}=0", f"{field}=1")
    with pytest.raises(ValueError, match="Validation failed"):
        builder.metrics_for("dp1_cdc", "validate_walmart_cdc", log)


def test_cdc_log_format_must_match_table():
    """Check the CDC log format must match the table version."""
    builder.check_cdc_table("cdc-v1", builder.LEGACY_CDC_TABLE)
    builder.check_cdc_table("cdc-v2", "lakehouse.bronze.raw_walmart_cdc_v2")
    with pytest.raises(ValueError):
        builder.check_cdc_table("cdc-v2", builder.LEGACY_CDC_TABLE)
    with pytest.raises(ValueError):
        builder.check_cdc_table("cdc-v1", "lakehouse.bronze.raw_walmart_cdc_v2")


def test_feature_v1_log_still_parses():
    """Check the parser still accepts feature v1 logs."""
    metrics, _, log_format = builder.metrics_for(
        "dp3_features", "validate_return_features", FEATURE_V1_LOG
    )
    assert log_format == "feature-v1"
    assert metrics["source_rows"] == metrics["dataset_rows"] == 10


def test_feature_v2_dict_log_parses_all_checks():
    """Check the parser reads every check in the feature v2 log dictionary."""
    metrics, _, log_format = builder.metrics_for(
        "dp3_features", "validate_return_features", feature_v2_log()
    )
    assert log_format == "feature-v2"
    assert metrics == {
        "source_rows": 10,
        "dataset_rows": 10,
        "invalid_rows": 0,
        "duplicate_order_line_key": 0,
        "source_invalid_rows": 0,
        "source_duplicate_keys": 0,
        "missing_rows": 0,
        "extra_rows": 0,
        "wrong_event_time_rows": 0,
    }


@pytest.mark.parametrize("field", ["missing", "extra", "wrong_event_time", "source_duplicates"])
def test_feature_v2_nonzero_failure_is_rejected(field):
    """Check feature v2 is rejected when a failure metric is nonzero."""
    with pytest.raises(ValueError, match="Validation failed"):
        builder.metrics_for(
            "dp3_features", "validate_return_features", feature_v2_log(**{field: 1})
        )


def test_run_override_replaces_only_named_dag():
    """Check a run override replaces only the named DAG."""
    runs = builder.parse_runs(["dp1_cdc=manual__new"])
    assert runs["dp1_cdc"] == "manual__new"
    assert runs["dp2_silver"] == builder.RUNS["dp2_silver"]
    with pytest.raises(ValueError):
        builder.parse_runs(["unknown_dag=x"])
    with pytest.raises(ValueError):
        builder.parse_runs(["dp1_cdc"])


def test_publisher_maps_cdc_job_to_v2_and_keeps_v1_dataset():
    """Check the publisher picks the CDC v2 job and keeps the v1 dataset."""
    cdc_jobs = publisher.job_map()["dp1_cdc"]
    assert cdc_jobs["ingest_walmart_cdc"][1] == ["lakehouse.bronze.raw_walmart_cdc_v2"]
    assert publisher.CDC_LEGACY in publisher.DATASETS
    assert "legacy" in publisher.description(publisher.CDC_LEGACY, "iceberg")


def test_assertion_urn_separates_v1_and_v2():
    """Check the assertion URN distinguishes v1 and v2."""
    base = {"dag_id": "dp1_cdc", "task_id": "validate_walmart_cdc"}
    v1 = publisher.assertion_urn(base | {"dataset": publisher.CDC_LEGACY})
    v2 = publisher.assertion_urn(base | {"dataset": publisher.CDC})
    assert v1 == "urn:li:assertion:platform-dp1_cdc-validate_walmart_cdc"
    assert v2 == v1 + "-v2"


def test_publisher_rejects_new_failure_metrics():
    """Check the publisher rejects the new failure metrics."""
    item = {
        "dataset": publisher.CDC,
        "state": "success",
        "task_id": "validate_walmart_cdc",
        "metrics": {"dataset_rows": 6, "payload_mismatch_rows": 1},
    }
    with pytest.raises(ValueError, match="did not PASS"):
        publisher.check_passed(item)


def test_legacy_manifest_still_publishable():
    """The historical manifest (CDC v1, feature v1) still passes the publish check; URNs unchanged."""
    import json

    manifest = json.loads((SCRIPTS.parent / "docs/evidence/datahub-validation.json").read_text())
    for item in manifest["checks"]:
        publisher.check_passed(item)
        assert not publisher.assertion_urn(item).endswith("-v2")


def test_publisher_aws_only_draws_dags_that_ran_with_s3_sources():
    """AWS ran 3 DAGs: no CDC/label lineage drawn; file sources are S3, not local /data."""
    root = "s3a://platform-raw/order_snapshots/source-exports"
    jobs = publisher.job_map(root, ("dp1_bronze", "dp2_silver", "dp3_features"))
    names = publisher.datasets_of(jobs)
    assert set(jobs) == {"dp1_bronze", "dp2_silver", "dp3_features"}
    assert publisher.CDC not in names and publisher.LABELS not in names
    assert f"{root}/channel=amazon/dataset=orders" in names
    assert not any(n.startswith("/data/") for n in names)
    assert publisher.platform_of(f"{root}/channel=shopee/dataset=orders") == "s3"
    assert publisher.platform_of(publisher.RAW_WALMART) == "postgres"


def test_publisher_default_keeps_local_lineage():
    """No options passed: all six DAGs (dp1 → dp4) and local sources as in the old evidence."""
    names = publisher.datasets_of(publisher.job_map())
    assert len(publisher.job_map()) == 6
    assert publisher.RAW_AMAZON in names and publisher.CDC in names


def test_builder_dags_only_reads_selected_runs(tmp_path, monkeypatch):
    """--dags dp3_features reads only that DAG's log; does not require logs of DAGs that never ran."""
    task = tmp_path / "dag_id=dp3_features/run_id=r1/task_id=validate_return_features"
    task.mkdir(parents=True)
    log = f"{FEATURE_V2}\nMarking task as SUCCESS. dag_id=dp3_features, end_date=20261028T011708\n"
    (task / "attempt=1.log").write_text(log)
    out = tmp_path / "out.json"
    monkeypatch.setattr(
        "sys.argv",
        [
            "x",
            "--logs-root",
            str(tmp_path),
            "--output",
            str(out),
            "--run",
            "dp3_features=r1",
            "--dags",
            "dp3_features",
        ],
    )
    builder.main()
    checks = __import__("json").loads(out.read_text())["checks"]
    assert [c["task_id"] for c in checks] == ["validate_return_features"]


WAREHOUSE_LOG = (
    "Warehouse warehouse.fact_order_lines_30d_dp2 [2026-05-31 00:00:00 → 2026-06-30 00:00:00): "
    "gold={'amazon': 5, 'walmart': 2, 'tiktok_shop': 1, 'shopify': 1, 'shopee': 1} "
    "postgres={'amazon': 5, 'walmart': 2, 'tiktok_shop': 1, 'shopify': 1, 'shopee': 1}\n"
    "Warehouse written + index seller_sku: warehouse.fact_order_lines_30d_dp2; rows=10\n"
)


def test_warehouse_check_counts_per_channel_and_lineage_on_postgres():
    """dp4: assertion from the reconciliation line; a mismatched Channel blocks; lineage FACT → PostgreSQL table."""
    metrics, _, _ = builder.metrics_for(
        "dp4_warehouse", "export_and_index_warehouse", WAREHOUSE_LOG
    )
    assert metrics == {
        "dataset_rows": 10,
        "source_rows": 10,
        "bad_channel_counts": 0,
        "missing_channels": 0,
    }
    with pytest.raises(ValueError):
        builder.metrics_for(
            "dp4_warehouse",
            "export_and_index_warehouse",
            WAREHOUSE_LOG.replace("postgres={'amazon': 5", "postgres={'amazon': 4"),
        )
    jobs = publisher.job_map(None, ("dp4_warehouse",))
    assert jobs == {
        "dp4_warehouse": {"export_and_index_warehouse": ([publisher.FACT], [publisher.WAREHOUSE])}
    }
    assert publisher.platform_of(publisher.WAREHOUSE) == "postgres"
