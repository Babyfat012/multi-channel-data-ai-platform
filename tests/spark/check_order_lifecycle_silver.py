"""Spark regression for Bronze/Silver of the v2 source (package C); runs in the cached Airflow image.

Uses no Kafka/Iceberg/catalog/project volume. Each run generates a fresh v2 fixture
in /tmp with generator.generate_order_lifecycle_fixture, then checks the DataFrame logic of
spark_jobs/lib/order_lifecycle_lakehouse.py:

docker run --rm --network none --read-only --tmpfs /tmp:rw,size=1g \
  --mount type=bind,src=$PWD,dst=/workspace,readonly --entrypoint python \
  platform-airflow:2.10.5-spark3.5.3 /workspace/tests/spark/check_order_lifecycle_silver.py
"""

import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from generator.generate_order_lifecycle_fixture import generate_fixture

os.environ["PLATFORM_CONTRACTS_DIR"] = str(ROOT / "contracts")
import spark_jobs.lib.order_lifecycle_lakehouse as v2

# Day 40 from 2026-01-01: after the ambiguous Refund is observed, before the correction (day 43).
BEFORE_REFUND_CORRECTION = "2026-02-10T00:00:00.000000Z"


class SourceV2LakehouseTests(unittest.TestCase):
    """Check Bronze/Silver v2 logic on DataFrames from a local source fixture."""

    @classmethod
    def setUpClass(cls):
        """Start local Spark and the shared v2 fixture for the suite."""
        cls.tmp = Path(tempfile.mkdtemp(prefix="platform-v2-"))
        cls.root = cls.tmp / "fixture"
        cls.manifest = generate_fixture(cls.root)
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("source-v2-regression")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.warehouse.dir", str(cls.tmp / "warehouse"))
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.bronze = v2.bronze_from_root(cls.spark, str(cls.root), "run-test").cache()
        cls.silver = v2.build_silver(cls.bronze)

    @classmethod
    def tearDownClass(cls):
        """Stop Spark and clean up the temporary fixture after the suite."""
        cls.spark.stop()
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def source_files(self):
        """Get the feed file list from the fixture manifest."""
        return [f for f in self.manifest["files"] if f["role"] == "source"]

    # ---------- Bronze ----------
    def test_bronze_keeps_every_emitted_line(self):
        """Check Bronze keeps every line emitted by the source."""
        expected = sum(f["rows"] for f in self.source_files())
        self.assertEqual(self.bronze.count(), expected)

    def test_bronze_path_columns_match_manifest(self):
        """Check Bronze path columns match the source manifest."""
        got = {
            (r["producer"], r["feed"]): r["n"]
            for r in self.bronze.groupBy("producer", "feed").agg(F.count("*").alias("n")).collect()
        }
        want = {}
        for f in self.source_files():
            key = (f["producer"], f["feed"])
            want[key] = want.get(key, 0) + f["rows"]
        self.assertEqual(got, want)

    def test_bronze_record_hash_matches_file_bytes(self):
        """Check the Bronze record hash matches the source file bytes."""
        row = self.bronze.filter(F.col("feed") == "seller_wms.stock_operations").first()
        import hashlib

        self.assertEqual(row["record_sha256"], hashlib.sha256(row["raw_json"].encode()).hexdigest())

    def test_fact_hash_ignores_execution_recorded_at(self):
        """Check the fact hash does not depend on the execution's recorded_at."""
        row = self.bronze.filter(F.col("feed") == "seller_oms.line_commitments").first()["raw_json"]
        other = json.loads(row)
        other["recorded_at"] = "2099-01-01T00:00:00.000000Z"
        frame = self.spark.createDataFrame(
            [(row,), (json.dumps(other, sort_keys=True, separators=(",", ":")),)], ["raw_json"]
        )
        hashes = {r[0] for r in frame.select(v2.fact_sha256_col("raw_json")).collect()}
        self.assertEqual(len(hashes), 1)

    # ---------- Silver: event identity ----------
    def test_clean_fixture_reconciles(self):
        """Check the clean fixture reconciles successfully."""
        result = v2.reconcile(self.silver)
        failures = {k: v for k, v in result["checks"].items() if v}
        self.assertEqual(failures, {}, result)

    def test_retry_duplicate_is_dropped_not_counted_twice(self):
        """Check a duplicate retry is dropped and not counted twice."""
        dup = self.bronze.filter(F.col("feed") == "seller_oms.line_commitments").limit(1)
        silver = v2.build_silver(self.bronze.unionByName(dup))
        self.assertEqual(silver["events"].count(), self.silver["events"].count())
        self.assertEqual(silver["dedup"]["duplicate_rows"], 1)
        self.assertEqual(v2.reconcile(silver)["checks"]["event_conflicts"], 0)

    def test_same_event_id_with_different_payload_is_conflict(self):
        """Check the same event ID with a different payload is flagged as a conflict."""
        one = self.bronze.filter(F.col("feed") == "seller_wms.stock_operations").limit(1)
        altered = v2.with_envelope(
            one.withColumn(
                "raw_json", F.regexp_replace("raw_json", '"quantity":(\\d+)', '"quantity":999')
            )
        )
        silver = v2.build_silver(self.bronze.unionByName(altered))
        self.assertEqual(v2.reconcile(silver)["checks"]["event_conflicts"], 1)

    def test_missing_dependency_is_reported(self):
        """Check a missing dependency is reported."""
        target = self.bronze.filter(
            (F.col("feed") == "seller_oms.fulfillment_allocations")
            & (F.col("entity_revision") == 1)
        ).limit(1)
        silver = v2.build_silver(self.bronze.subtract(target))
        self.assertGreater(v2.reconcile(silver)["checks"]["missing_dependencies"], 0)

    # ---------- Silver: quantity, money, lifecycle ----------
    def test_headline_is_ordered_minus_canceled_before_ship_refund_separate(self):
        """Check the headline subtracts cancellations before shipping and keeps Refunds separate."""
        lines = self.silver["line_commitments"]
        line_a = lines.filter(F.col("native_line_id") == "1").first()
        self.assertEqual((line_a["ordered_basis_qty"], line_a["canceled_unshipped_qty"]), (3, 1))
        self.assertEqual(line_a["headline_product_net_minor"], 20000)
        total = lines.agg(F.sum("headline_product_net_minor")).first()[0]
        self.assertEqual(total, 15 * (20000 + 30000))
        refunds = self.silver["refunds"].filter(F.col("component") == "product")
        self.assertEqual(refunds.agg(F.sum("amount_minor")).first()[0], 15 * 10000)

    def test_quantity_balance_breaks_when_allocation_missing(self):
        """Check a missing allocation breaks the quantity balance."""
        second = self.bronze.filter(
            (F.get_json_object("raw_json", "$.payload.quantity") == "1")
            & (F.col("feed") == "seller_oms.fulfillment_allocations")
        )
        self.assertGreater(second.count(), 0)
        silver = v2.build_silver(self.bronze.subtract(second))
        self.assertGreater(v2.reconcile(silver)["checks"]["quantity_mismatch_lines"], 0)

    def test_split_shipment_line_has_two_allocations(self):
        """Check a split-shipment line has two separate allocations."""
        allocs = (
            self.silver["allocations"]
            .groupBy("line_id")
            .agg(F.sum("quantity").alias("q"), F.count("*").alias("n"))
        )
        line_b = (
            self.silver["line_commitments"].filter(F.col("native_line_id") == "2").select("line_id")
        )
        rows = allocs.join(line_b, "line_id").collect()
        self.assertEqual({(r["n"], r["q"]) for r in rows}, {(2, 3)})

    def test_no_fanout_between_lines_and_allocations(self):
        """Check joining Order Lines with allocations causes no fan-out."""
        result = v2.reconcile(self.silver)
        self.assertEqual(result["checks"]["fanout_headline_delta_minor"], 0)
        self.assertEqual(result["checks"]["orphan_allocations"], 0)

    # ---------- Silver: corrections, uncertainty, known-time ----------
    def test_refund_before_correction_keeps_unresolved_scope(self):
        """Check a Refund before the correction keeps the unresolved scope."""
        early = v2.build_silver(self.bronze, as_of=BEFORE_REFUND_CORRECTION)
        result = v2.reconcile(early)
        self.assertEqual(result["info"]["refund_unresolved_product_rows"], 15)
        self.assertEqual(result["checks"]["refund_unscoped_rows"], 0)
        unresolved = early["refunds"].filter(
            F.col("line_id").isNull() & (F.col("component") == "product")
        )
        self.assertEqual(unresolved.filter(F.col("unresolved_scope_ref").isNull()).count(), 0)

    def test_refund_after_correction_is_allocated(self):
        """Check a Refund after the correction is allocated correctly."""
        result = v2.reconcile(self.silver)
        self.assertEqual(result["info"]["refund_unresolved_product_rows"], 0)
        refunds = self.silver["refunds"].filter(F.col("component") == "product")
        self.assertEqual(refunds.filter(F.col("allocated_amount_minor") != 10000).count(), 0)

    def test_listing_correction_replaces_wrong_mapping_and_keeps_history(self):
        """Check a listing correction replaces the wrong mapping and keeps history."""
        history = self.silver["listing_versions"]
        amazon_a = history.filter((F.col("channel") == "amazon") & (F.col("revision") > 1))
        latest = history.filter(
            F.col("is_latest")
            & F.col("listing_id").isin(
                [r["listing_id"] for r in amazon_a.select("listing_id").distinct().collect()]
            )
        ).first()
        self.assertEqual(latest["internal_sku_id"], "sku-B")
        self.assertEqual(latest["change_kind"], "source_correction")
        self.assertEqual(amazon_a.count(), 2)

    def test_stock_ledger_matches_snapshot(self):
        """Check the stock ledger matches the snapshot."""
        self.assertEqual(v2.reconcile(self.silver)["checks"]["stock_snapshot_mismatches"], 0)

    # ---------- Silver: native Orders for 5 Channels, cross-reconciled with OMS ----------
    def test_native_lines_parse_for_every_channel(self):
        """Check native lines of all five Channels are parsed."""
        native = self.silver["native_order_lines"]
        per_channel = {
            r["channel"]: r["n"]
            for r in native.groupBy("channel").agg(F.count("*").alias("n")).collect()
        }
        self.assertEqual(
            per_channel, {c: 6 for c in ("amazon", "walmart", "shopify", "tiktok_shop", "shopee")}
        )
        self.assertEqual(native.filter(F.col("quantity") != 3).count(), 0)
        self.assertEqual(native.filter(F.col("net_minor") != 30000).count(), 0)

    def test_native_reconciles_with_oms(self):
        """Check the native source reconciles with OMS."""
        checks = v2.reconcile(self.silver)["checks"]
        for key in (
            "native_unparsed_rows",
            "native_lines_without_commitment",
            "commitments_without_native",
            "native_quantity_mismatches",
            "native_money_mismatches",
        ):
            self.assertEqual(checks[key], 0, key)

    def test_amazon_schema_two_parses_the_same(self):
        """Check the second Amazon schema gives the same logical result."""
        root = self.tmp / "schema2"
        generate_fixture(root, native_schema=2)
        silver = v2.build_silver(v2.bronze_from_root(self.spark, str(root), "run-schema2"))
        amazon = silver["native_order_lines"].filter(F.col("channel") == "amazon")
        self.assertEqual(amazon.count(), 6)
        self.assertEqual(
            set(r[0] for r in amazon.select("native_schema").distinct().collect()), {"amazon/2"}
        )
        checks = v2.reconcile(silver)["checks"]
        self.assertEqual({k: v for k, v in checks.items() if v}, {})

    def test_native_money_drift_is_caught(self):
        """Check money drift from the native source is caught."""
        walmart = self.bronze.filter(F.col("feed") == "walmart.orders").limit(1)
        drifted = v2.with_envelope(
            walmart.withColumn(
                "raw_json", F.regexp_replace("raw_json", '"amount":"300.00"', '"amount":"299.00"')
            )
        )
        silver = v2.build_silver(self.bronze.subtract(walmart).unionByName(drifted))
        self.assertEqual(v2.reconcile(silver)["checks"]["native_money_mismatches"], 2)

    def test_missing_native_order_is_caught(self):
        """Check a missing native Order is caught."""
        one = self.bronze.filter(F.col("feed") == "shopee.orders").limit(1)
        silver = v2.build_silver(self.bronze.subtract(one))
        self.assertEqual(v2.reconcile(silver)["checks"]["commitments_without_native"], 2)

    def test_walmart_via_jdbc_rows_parses_and_reconciles(self):
        """Rows read from PostgreSQL (json_build_object with whitespace) replace the bootstrap file rows."""
        files = self.bronze.filter(F.col("feed") == "walmart.orders")
        jdbc = files.select(
            "dataset_version",
            F.get_json_object("raw_json", "$.purchase_order_id").alias("purchase_order_id"),
            F.concat(
                F.lit('{"fetched_at" : "'),
                F.get_json_object("raw_json", "$.fetched_at"),
                F.lit('", "purchase_order_id" : "'),
                F.get_json_object("raw_json", "$.purchase_order_id"),
                F.lit('", "raw_payload" : '),
                F.get_json_object("raw_json", "$.raw_payload"),
                F.lit("}"),
            ).alias("raw_json"),
        )
        rows = v2.bronze_from_jdbc(
            jdbc,
            "run-jdbc",
            "jdbc:postgresql://postgres:5432/sources#walmart_lifecycle.orders",
            "2026-01-04",
        )
        self.assertEqual(
            set(r[0] for r in rows.select("ingestion_boundary").distinct().collect()), {"jdbc"}
        )
        bronze = self.bronze.filter(F.col("feed") != "walmart.orders").unionByName(rows)
        self.assertEqual(
            bronze.filter(F.col("ingestion_boundary") == "file")
            .filter(F.col("feed") == "walmart.orders")
            .count(),
            0,
        )
        silver = v2.build_silver(bronze)
        self.assertEqual(
            silver["native_order_lines"].filter(F.col("channel") == "walmart").count(), 6
        )
        checks = v2.reconcile(silver)["checks"]
        self.assertEqual({k: v for k, v in checks.items() if v}, {})

    def test_snapshot_scope_compares_order_ids(self):
        """Check the snapshot scope compares the right set of Order IDs."""
        got = v2.snapshot_scope_diff(["a", "b", "b"], ["a", "c"])
        self.assertEqual(got, {"missing": ["c"], "extra": ["b"], "duplicate": ["b"]})

    # ---------- Silver: producer coverage receipt ----------
    def test_coverage_receipts_pass_on_clean_fixture(self):
        """Check the coverage receipt passes on the clean fixture."""
        result = v2.reconcile(self.silver)
        self.assertEqual(result["checks"]["coverage_receipt_failures"], 0)
        self.assertEqual(result["checks"]["feeds_without_complete_coverage"], 0)
        self.assertEqual(result["info"]["coverage_unavailable_scopes"], 6)

    def test_coverage_detects_missing_record(self):
        """Check coverage detects a lost record."""
        op = self.bronze.filter(F.col("raw_json").contains('"operation_type":"reserve"')).limit(1)
        receipts = v2.build_silver(self.bronze.subtract(op))["coverage_receipts"]
        failed = receipts.filter(F.col("covered_feed") == "seller_wms.stock_operations").first()
        self.assertEqual(failed["receipt_status"], "partial")
        self.assertIn("missing_sequence", failed["reasons"])
        self.assertIn("record_count_mismatch", failed["reasons"])

    def test_coverage_detects_silent_content_change(self):
        """Check coverage detects a silent content change."""
        row = self.bronze.filter(F.col("feed") == "carrier.delivery_events").limit(1)
        changed = v2.with_envelope(
            row.withColumn(
                "raw_json",
                F.regexp_replace(
                    "raw_json", '"event_type":"delivered"', '"event_type":"delivered "'
                ),
            )
        )
        receipts = v2.build_silver(self.bronze.subtract(row).unionByName(changed))[
            "coverage_receipts"
        ]
        failed = receipts.filter(F.col("covered_feed") == "carrier.delivery_events").first()
        self.assertEqual(failed["reasons"], ["record_set_digest_mismatch"])

    def test_stock_correction_replaces_not_adds(self):
        """Check a stock correction replaces the old value instead of accumulating."""
        opening = self.bronze.filter(
            F.col("raw_json").contains('"operation_type":"opening"')
        ).limit(1)
        fixed = (
            opening.withColumn(
                "raw_json",
                F.regexp_replace("raw_json", '"entity_revision":1', '"entity_revision":2'),
            )
            .withColumn(
                "raw_json",
                F.regexp_replace("raw_json", '"delta_on_hand":200', '"delta_on_hand":199'),
            )
            .withColumn(
                "raw_json",
                F.regexp_replace(
                    "raw_json", '"source_event_id":"event-', '"source_event_id":"event-fix'
                ),
            )
        )
        fixed = v2.with_envelope(fixed)
        silver = v2.build_silver(self.bronze.unionByName(fixed))
        ops = silver["stock_operations"].filter(F.col("operation_type") == "opening")
        self.assertEqual(ops.count(), 2)
        self.assertEqual(sorted(r["delta_on_hand"] for r in ops.collect()), [199, 200])
        self.assertEqual(v2.reconcile(silver)["checks"]["stock_snapshot_mismatches"], 1)

    def test_contract_default_has_no_quarantine(self):
        """The default fixture is accepted intact."""
        self.assertEqual(self.silver["quarantine_records"].count(), 0)

    def test_contract_unknown_shopee_keeps_other_channels(self):
        """An unknown schema quarantines Shopee; the other four Channels keep all lines and reconcile."""
        root = self.tmp / "unknown-shopee"
        generate_fixture(root, native_schemas={"shopee": 2})
        silver = v2.build_silver(v2.bronze_from_root(self.spark, str(root), "unknown"))
        quarantine = silver["quarantine_records"]
        self.assertEqual(quarantine.count(), 3)
        self.assertEqual(
            {r.reason for r in quarantine.select("reason").collect()}, {"unregistered_schema"}
        )
        for channel in ("amazon", "walmart", "shopify", "tiktok_shop"):
            self.assertEqual(
                silver["native_order_lines"].filter(F.col("channel") == channel).count(),
                self.silver["native_order_lines"].filter(F.col("channel") == channel).count(),
            )
        result = v2.reconcile(silver)
        self.assertEqual({k: v for k, v in result["checks"].items() if v}, {})
        self.assertEqual(result["quarantined_orders"], {})
        self.assertEqual(result["info"]["quarantined_native_records_without_order_identity"], 3)

    def test_contract_compatible_tiktok_uses_existing_parser(self):
        """A new optional version goes through the old parser purely via the registry."""
        root = self.tmp / "compatible-tiktok"
        generate_fixture(root, native_schemas={"tiktok_shop": 2})
        silver = v2.build_silver(v2.bronze_from_root(self.spark, str(root), "compatible"))
        self.assertEqual(silver["quarantine_records"].count(), 0)
        before = (
            self.silver["native_order_lines"]
            .filter(F.col("channel") == "tiktok_shop")
            .drop("dataset_version")
        )
        after = (
            silver["native_order_lines"]
            .filter(F.col("channel") == "tiktok_shop")
            .drop("dataset_version")
        )
        self.assertEqual(before.exceptAll(after).count(), 0)
        self.assertEqual(after.exceptAll(before).count(), 0)

    def test_contract_producer_lies_about_schema(self):
        """A wrong required field under a registered label is quarantined with a clear path."""
        one = self.bronze.filter(F.col("feed") == "shopee.orders").limit(1)
        changed = v2.with_envelope(
            one.withColumn(
                "raw_json", F.regexp_replace("raw_json", '"order_sn":', '"order_number":')
            )
        )
        silver = v2.build_silver(self.bronze.subtract(one).unionByName(changed))
        rows = silver["quarantine_records"].collect()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].reason, "contract_violation")
        self.assertIn("order_sn", rows[0].detail)
        result = v2.reconcile(silver)
        self.assertEqual({k: v for k, v in result["checks"].items() if v}, {})
        self.assertEqual(result["quarantined_orders"], {})
        self.assertEqual(result["info"]["quarantined_native_records_without_order_identity"], 1)

    def test_contract_registered_but_missing_parser(self):
        """The registry knows the schema but Silver has no parser yet: no_parser."""
        from spark_jobs.lib.data_contracts.registry import Registry

        registry = Registry(ROOT / "contracts")
        registry.entries["sim-native-v2/shopee/2"] = {
            **registry.entries.get("sim-native-v2/shopee/1"),
            "parser": "shopee/2",
        }
        bronze = self.bronze.withColumn(
            "contract_schema_id",
            F.when(F.col("feed") == "shopee.orders", F.lit("sim-native-v2/shopee/2")).otherwise(
                F.col("contract_schema_id")
            ),
        )
        silver = v2.build_silver(bronze, registry=registry)
        self.assertEqual(silver["quarantine_records"].count(), 3)
        self.assertEqual(
            {r.reason for r in silver["quarantine_records"].select("reason").collect()},
            {"no_parser"},
        )
        result = v2.reconcile(silver)
        self.assertEqual(result["quarantined_orders"], {"shopee": 3})
        self.assertEqual(result["info"]["quarantined_native_records_without_order_identity"], 0)
        self.assertEqual({k: v for k, v in result["checks"].items() if v}, {})


if __name__ == "__main__":
    unittest.main(verbosity=2)
