"""Check Iceberg maintenance decisions through the pure-Python interface, without Spark."""

from datetime import datetime, timezone

from spark_jobs.lib.maintenance_policy import plan_maintenance

NOW = datetime(2026, 10, 18, tzinfo=timezone.utc)


def test_compact_only_small_crowded_partition():
    """Partitions with few files or large files are not compacted by mistake."""
    metadata = {
        "files": [
            {"partition": {"day": day}, "file_size_in_bytes": size}
            for day, sizes in [(1, [1, 1, 1]), (2, [1]), (3, [100, 100, 100])]
            for size in sizes
        ],
        "snapshots": [],
        "refs": [],
        "properties": {},
    }
    plan = plan_maintenance(metadata, {"target_file_size_bytes": 100, "min_input_files": 3}, NOW)
    compact = next(action for action in plan["actions"] if action["kind"] == "compact")
    assert compact["partitions"] == [{"day": 1}]
    assert compact["reason"]


def metadata_snapshots():
    """Small history with a z-order marker and an old generation."""
    return {
        "files": [],
        "properties": {},
        "refs": [
            {"name": "gen-1", "type": "TAG", "snapshot_id": 1},
            {"name": "main", "type": "BRANCH", "snapshot_id": 4},
        ],
        "current_snapshot_id": 4,
        "snapshots": [
            {
                "snapshot_id": i,
                "parent_id": i - 1,
                "committed_at": "2026-01-01T00:00:00Z",
                "summary": {"added-data-files": "1"},
            }
            for i in range(1, 5)
        ],
    }


def test_zorder_first_run_then_only_after_threshold():
    """The snapshot marker prevents redundant z-order; only enough new files trigger it."""
    metadata = metadata_snapshots()
    config = {"zorder_columns": ["seller_sku"], "zorder_min_new_files": 3}
    assert "zorder" in [a["kind"] for a in plan_maintenance(metadata, config, NOW)["actions"]]
    metadata["properties"]["platform.maintenance.last_zorder_snapshot_id"] = "3"
    assert "zorder" not in [a["kind"] for a in plan_maintenance(metadata, config, NOW)["actions"]]
    metadata["properties"]["platform.maintenance.last_zorder_snapshot_id"] = "1"
    assert "zorder" in [a["kind"] for a in plan_maintenance(metadata, config, NOW)["actions"]]


def test_generation_and_branch_protected_even_when_old():
    """Never put a snapshot referenced by any tag/branch on the expiry list."""
    plan = plan_maintenance(metadata_snapshots(), {"retain_last": 1}, NOW)
    expire = next(a for a in plan["actions"] if a["kind"] == "expire_snapshots")
    assert expire["snapshot_ids"] == [2, 3]
    assert plan["protected_snapshot_ids"] == [1, 4]
    assert plan["generation_tags"] == ["gen-1"]


def test_no_mutation_has_reason_and_orphans_always_dry_run():
    """A mutation no-op still has the orphan inventory, per the always-plan rule."""
    plan = plan_maintenance({}, {}, NOW)
    assert plan["no_op"] and "no-op" in plan["reason"]
    assert len(plan["actions"]) == 1
    assert plan["actions"][0]["dry_run"] is True
    assert plan["actions"][0]["older_than"] == "2026-10-15T00:00:00+00:00"


def test_retry_transient_twice_and_stop_at_fatal():
    """The receipt has 3 attempts; a fatal error stops before the next action."""
    import pytest

    from spark_jobs.lib.maintenance_policy import execute_actions

    attempts, called, delays = [], [], []

    def transient(action):
        """Fake a commit conflict twice, then return the procedure result."""
        called.append(action["kind"])
        if len(called) < 3:
            raise RuntimeError("CommitFailedException: conflicting commit")
        return "ok"

    execute_actions([{"kind": "compact"}], transient, {}, attempts, delays.append)
    assert len(attempts) == 3 and attempts[-1]["result"] == "ok"
    assert delays == [1, 2]
    called.clear()

    def fatal(action):
        """Fake a config error that must not be retried."""
        called.append(action["kind"])
        raise ValueError("bad column")

    with pytest.raises(ValueError):
        execute_actions([{"kind": "compact"}, {"kind": "zorder"}], fatal, {}, [], delays.append)
    assert called == ["compact"]


def test_validation_error_requires_concurrency_and_retry_is_bounded():
    """Do not retry an invalid schema; never try more than max_attempts."""
    import pytest

    from spark_jobs.lib.maintenance_policy import execute_actions, is_transient

    assert not is_transient(RuntimeError("ValidationException: invalid schema"))
    assert is_transient(RuntimeError("ValidationException: Found new conflicting files"))
    attempts = []

    def fail(action):
        """Fake a transient error that never clears."""
        raise RuntimeError("CommitFailedException")

    with pytest.raises(RuntimeError):
        execute_actions(
            [{"kind": "compact"}], fail, {"max_backoff_seconds": 1}, attempts, lambda _: None
        )
    assert len(attempts) == 3
    assert attempts[1]["backoff_seconds"] == 1


def test_project_configuration_valid_and_unsafe_orphan_rejected():
    """The real JSON loads; an orphan-delete cutoff under 72h is blocked."""
    from pathlib import Path

    import pytest

    from spark_jobs.lib.maintenance_policy import load_config, validated_config

    configs = load_config(Path(__file__).resolve().parents[4] / "configs/iceberg_maintenance.json")
    assert configs
    assert all(config["orphans_dry_run"] for config in configs.values())
    with pytest.raises(ValueError):
        validated_config({"orphan_age_hours": 24})


def test_probe_missing_metrics_conservatively_reads_file():
    """The probe counts range intersections; a missing bound must not be treated as skipping the file."""
    from spark_jobs.lib.maintenance_policy import count_probe_files

    files = [
        {"readable_metrics": {"sku": {"lower_bound": 0, "upper_bound": 10}}},
        {"readable_metrics": {"sku": {"lower_bound": 20, "upper_bound": 30}}},
        {"readable_metrics": {}},
    ]
    assert count_probe_files(files, {"sku": [5, 8]}) == 2


def test_probe_timestamp_accepts_iso_range_without_wrong_pruning():
    """Normalize times with a T/time zone before comparing with datetime metrics."""
    from spark_jobs.lib.maintenance_policy import count_probe_files

    files = [
        {
            "readable_metrics": {
                "created_at": {
                    "lower_bound": datetime(2026, 1, 1, 0, 0),
                    "upper_bound": datetime(2026, 1, 1, 2, 0),
                }
            }
        }
    ]
    assert (
        count_probe_files(files, {"created_at": ["2026-01-01T01:00:00Z", "2026-01-01T01:30:00Z"]})
        == 1
    )


def test_sql_where_is_passed_as_one_escaped_argument():
    """The CLI module loads without pyspark; a literal containing quotes keeps its quotes."""
    import importlib.util
    import sys
    from pathlib import Path
    from unittest.mock import patch

    root = Path(__file__).resolve().parents[4]
    with patch.object(sys, "path", [str(root / "spark_jobs"), *sys.path]):
        spec = importlib.util.spec_from_file_location(
            "maintenance_cli", root / "spark_jobs/maintenance/maintain_iceberg_tables.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    assert module.literal("day = '2026-01-01'") == "'day = \\'2026-01-01\\''"
    assert module.identifier("local.demo.orders") == "`local`.`demo`.`orders`"
    orphans = module.action_sql(
        None,
        "lakehouse.bronze.orders",
        {"kind": "remove_orphans", "older_than": "2026-10-25T00:00:00+00:00", "dry_run": True},
    )
    # S3 on AWS: Hadoop FileSystem cannot read s3://, so listing must go through FileIO.
    assert "prefix_listing => true" in orphans and "dry_run => true" in orphans


def test_expiring_reference_retention_is_rejected_before_deletion():
    """Do not let expire_snapshots drop an expired-TTL ref and then delete the old generation."""
    import pytest

    from spark_jobs.lib.maintenance_policy import ensure_refs_survive_expiration

    metadata = metadata_snapshots()
    metadata["refs"][0]["max_reference_age_in_ms"] = 1000
    with pytest.raises(ValueError, match="gen-1"):
        ensure_refs_survive_expiration(metadata, NOW)
    metadata["refs"][0]["max_reference_age_in_ms"] = None
    ensure_refs_survive_expiration(metadata, NOW)
