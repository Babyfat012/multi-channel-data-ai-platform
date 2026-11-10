"""Check config and the receipt contract with stdlib only, without starting or importing Spark."""

import json

import pytest

from scripts.run.run_order_lifecycle_local import empty_receipt, parse_args, save_receipt, stage


def test_default_medium(tmp_path):
    """Check the default config uses the medium profile and a fixed seed."""
    args = parse_args(["--receipt", str(tmp_path / "receipt.json")])
    assert (args.profile, args.orders_per_day, args.days, args.seed) == ("medium", 500, 60, 7)


def test_small_and_overrides(tmp_path):
    """Check the small profile and override values are applied."""
    base = ["--receipt", str(tmp_path / "receipt.json"), "--profile", "small"]
    small = parse_args(base)
    assert (small.orders_per_day, small.days) == (50, 45)
    custom = parse_args(
        base
        + [
            "--orders-per-day",
            "12",
            "--days",
            "55",
            "--seed",
            "19",
            "--source",
            str(tmp_path / "existing"),
        ]
    )
    assert (custom.orders_per_day, custom.days, custom.seed) == (12, 55, 19)
    assert custom.source == tmp_path / "existing"


@pytest.mark.parametrize(
    "flags",
    [
        ["--profile", "large"],
        ["--days", "0"],
        ["--orders-per-day", "0"],
        ["--cutoff", "2026-01-01"],
        ["--sku-count", "1"],
    ],
)
def test_invalid_cli(flags, tmp_path):
    """Check the CLI rejects invalid arguments."""
    with pytest.raises(SystemExit):
        parse_args(["--receipt", str(tmp_path / "r.json"), *flags])


def test_empty_receipt_schema(tmp_path, monkeypatch):
    """Check the initial receipt structure before the pipeline runs."""
    monkeypatch.setenv("WAREHOUSE_TEST_DSN", "secret-value")
    args = parse_args(["--receipt", str(tmp_path / "r.json"), "--run-id", "test"])
    receipt = empty_receipt(args)
    assert receipt["schema_version"] == 1
    assert receipt["status"] == "running"
    assert receipt["dataset_version"] is None
    assert receipt["tables"] == receipt["checks"] == {}
    assert receipt["warehouse"] is receipt["explain"] is None
    assert set(receipt["stages"]) == {
        "source",
        "spark_start",
        "bronze",
        "silver",
        "gold_build",
        "gold_validate",
        "gold_write",
        "warehouse",
        "explain",
    }
    assert all(
        s == {"status": "pending", "seconds": None, "rows": {}} for s in receipt["stages"].values()
    )
    save_receipt(args.receipt, receipt)
    assert "secret-value" not in args.receipt.read_text()
    assert json.loads(args.receipt.read_text())["peak_rss"]["python_driver_kib"] > 0


def test_failed_stage_keeps_measurement(tmp_path):
    """Check a failed stage still records measurements and the failed status."""
    args = parse_args(["--receipt", str(tmp_path / "r.json")])
    receipt = empty_receipt(args)
    with pytest.raises(ValueError), stage(receipt, args.receipt, "bronze") as item:
        item["rows"]["first_table"] = 12
        raise ValueError("secret must not be saved")
    saved = json.loads(args.receipt.read_text())
    assert saved["stages"]["bronze"]["status"] == "failed"
    assert saved["stages"]["bronze"]["seconds"] >= 0
    assert saved["stages"]["bronze"]["rows"] == {"first_table": 12}
    assert "secret must not be saved" not in args.receipt.read_text()
