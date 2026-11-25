"""DataHub publishers: lineage (datasets, jobs) and the Bronze Data Contracts built from contracts/registry.json."""

from __future__ import annotations

from scripts.data import publish_datahub_contracts as contracts_publisher
from scripts.data import publish_order_snapshots_datahub as publisher
from spark_jobs.lib.assertion_result import contract_schema_urn
from spark_jobs.lib.data_contracts.registry import Registry


def test_cdc_lineage_follows_flink_and_the_nightly_gate():
    """dp1_cdc is gone: Flink writes the v2 table, dp_nightly only checks it; the v1 table stays as history."""
    jobs = publisher.job_map()
    assert "dp1_cdc" not in jobs
    assert jobs["lakehouse-bronze-streams"]["walmart_cdc_to_bronze"] == (
        [publisher.KAFKA_WALMART_CDC],
        [publisher.CDC],
    )
    assert jobs["dp_nightly"]["check_walmart_cdc_complete"] == ([publisher.CDC], [])
    assert publisher.FLOW_PLATFORM["lakehouse-bronze-streams"] == "flink"
    assert publisher.CDC_LEGACY in publisher.DATASETS
    assert "legacy" in publisher.description(publisher.CDC_LEGACY, "iceberg")


def test_aws_only_draws_dags_that_ran_with_s3_sources():
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


def test_warehouse_lineage_on_postgres():
    """dp4: FACT → the EC2 PostgreSQL table."""
    assert publisher.job_map(None, ("dp4_warehouse",)) == {
        "dp4_warehouse": {"export_and_index_warehouse": ([publisher.FACT], [publisher.WAREHOUSE])}
    }
    assert publisher.platform_of(publisher.WAREHOUSE) == "postgres"


def test_one_contract_per_channel_on_its_bronze_table():
    """Five Channels; the schema assertion urn is the one the Bronze validator reports against."""
    items = {c["channel"]: c for c in contracts_publisher.contracts(Registry())}
    assert set(items) == {"amazon", "walmart", "tiktok_shop", "shopify", "shopee"}
    shopee = items["shopee"]
    assert shopee["dataset"] == (
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,lakehouse.bronze.raw_shopee_orders_dp1,PROD)"
    )
    assert shopee["schema_assertion"] == contract_schema_urn("shopee")
    assert shopee["quality_assertions"] == ["urn:li:assertion:platform-dp1_bronze-validate_shopee"]
    assert "anyOf" not in shopee["json_schema"]


def test_amazon_contract_accepts_both_active_shapes():
    """Amazon v0 and 2026 are both active: the contract schema is anyOf of the two, like the validator."""
    registry = Registry()
    amazon = next(c for c in contracts_publisher.contracts(registry) if c["channel"] == "amazon")
    assert amazon["json_schema"]["anyOf"] == [
        registry.entries["sim-native-v1/amazon/1"]["schema"],
        registry.entries["sim-native-v1/amazon/2"]["schema"],
    ]
