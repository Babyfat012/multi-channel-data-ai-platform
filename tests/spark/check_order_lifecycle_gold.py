"""Offline local[1] Gold v2 integration checks; fresh fixture, no catalog or truth inputs.

Run with the Docker command in docs/edai-1/reference/gold_order_lifecycle.md. Positive PIT tests add one observed
Order after fixture follow-up, without generating further Orders in the simulator.
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
os.environ["PLATFORM_CONTRACTS_DIR"] = str(ROOT / "contracts")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from generator.generate_order_lifecycle_fixture import generate_fixture
from spark_jobs.lib.order_lifecycle_gold import build_gold, validate_gold
from spark_jobs.lib.order_lifecycle_lakehouse import (
    bronze_from_root,
    build_silver,
    observed_evidence,
    with_envelope,
)
from spark_jobs.lib.return_label import utc

CUTOFF = "2026-06-01T00:00:00.000000Z"


class GoldV2Checks(unittest.TestCase):
    """Check independent reconciliation and deliberate corruption at public boundaries."""

    @classmethod
    def setUpClass(cls):
        """Generate fresh default fixture and build real Silver/Gold without a catalog."""
        cls.tmp = Path(tempfile.mkdtemp(prefix="gold-v2-"))
        cls.root = cls.tmp / "default"
        generate_fixture(cls.root)
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("gold-v2-checks")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.warehouse.dir", str(cls.tmp / "warehouse"))
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.bronze = bronze_from_root(cls.spark, str(cls.root), "test").cache()
        cls.silver = build_silver(bronze=cls.bronze)
        cls.gold = build_gold(silver=cls.silver, cutoff=CUTOFF, run_id="test")
        cls.result = validate_gold(cls.silver, cls.gold, CUTOFF)

    @classmethod
    def tearDownClass(cls):
        """Release Spark and /tmp fixture only."""
        cls.spark.stop()
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def test_default_all_invariants_zero(self):
        """Includes no fan-out, null equivalence, maturity, PIT and pure policy parity."""
        self.assertEqual({k: v for k, v in self.result["checks"].items() if v}, {}, self.result)

    def test_fixture_official_counts_and_zero_history(self):
        """Observable fixture has 30 lines, 15 approved/product refunds, 15 negatives."""
        labels = self.gold["label_return_v2"]
        self.assertEqual(labels.count(), 30)
        self.assertEqual(labels.filter("label = 1").count(), 15)
        self.assertEqual(labels.filter("label = 0").count(), 15)
        self.assertEqual(
            self.gold["fact_order_lines"].agg(F.sum("headline_product_net_minor")).first()[0],
            750000,
        )
        self.assertEqual(
            self.gold["feat_sku_return_rate_90d"]
            .filter("labeled_count_90d != 0 OR return_or_refund_rate_90d IS NOT NULL")
            .count(),
            0,
        )

    def test_validate_catches_fanout(self):
        """Duplicating a fact row must break both count and money checks."""
        broken = {
            **self.gold,
            "fact_order_lines": self.gold["fact_order_lines"].unionByName(
                self.gold["fact_order_lines"].limit(1)
            ),
        }
        checks = validate_gold(self.silver, broken, CUTOFF)["checks"]
        self.assertGreater(checks["fact_line_count_delta"], 0)
        self.assertGreater(checks["fact_headline_delta_minor"], 0)

    def test_validate_catches_nonnull_unknown_and_nonmature(self):
        """Deliberately corrupt official state and deadline separately."""
        labels = self.gold["label_return_v2"].withColumn("state", F.lit("unknown"))
        checks = validate_gold(self.silver, {**self.gold, "label_return_v2": labels}, CUTOFF)[
            "checks"
        ]
        self.assertEqual(checks["invalid_label_state"], 30)
        labels = self.gold["label_return_v2"].withColumn("maturity_end", F.lit(utc(CUTOFF)))
        checks = validate_gold(self.silver, {**self.gold, "label_return_v2": labels}, CUTOFF)[
            "checks"
        ]
        self.assertEqual(checks["labeled_not_mature"], 30)

    def with_later_order(self):
        """Add one new observed target using same SKU; keep original feed digests intact."""
        lifecycle = self.silver["line_lifecycle"]
        native = self.silver["native_order_lines"]
        base = lifecycle.filter("native_line_id = '1'").first().asDict()
        original = self.bronze.filter(
            (F.col("feed") == "seller_oms.line_commitments")
            & (F.col("entity_revision") == 1)
            & (F.get_json_object("raw_json", "$.payload.line_id") == base["line_id"])
        ).first()
        native_row = (
            native.filter(
                (F.col("dataset_version") == base["dataset_version"])
                & (F.col("channel") == base["channel"])
                & (F.col("native_order_id") == base["native_order_id"])
                & (F.col("native_line_id") == "1")
            )
            .first()
            .asDict()
        )
        base.update(
            line_id="later-line",
            native_order_id="later-order",
            delivered_qty=0,
            allocated_qty=0,
            allocation_count=0,
            approved_return_qty=0,
            product_refund_success_minor=0,
            fulfillment_terminal=False,
            last_delivered_at=None,
        )
        native_row.update(native_order_id="later-order")
        raw = json.loads(original["raw_json"])
        raw.update(
            source_event_id="later-commitment",
            entity_id="later-line",
            producer="test.oms",
            source_epoch="test.epoch",
            source_sequence=1,
            effective_at="2026-04-03T00:00:00.000000Z",
            source_observed_at="2026-04-03T00:00:00.000000Z",
        )
        raw["payload"].update(line_id="later-line", native_order_id="later-order")
        extra = with_envelope(
            self.spark.createDataFrame(
                [
                    (
                        base["dataset_version"],
                        "seller_oms.line_commitments",
                        "test.oms",
                        json.dumps(raw),
                    )
                ],
                "dataset_version string, feed string, producer string, raw_json string",
            )
        )
        evidence = self.silver["observed_evidence"].unionByName(observed_evidence(extra))
        return {
            **self.silver,
            "line_lifecycle": lifecycle.unionByName(
                self.spark.createDataFrame([base], lifecycle.schema)
            ),
            "native_order_lines": native.unionByName(
                self.spark.createDataFrame([native_row], native.schema)
            ),
            "observed_evidence": evidence,
        }

    def test_positive_pit_history_and_injected_late_label(self):
        """A real nonempty PIT feature counts 15 positives; late-known counts and segments are rejected."""
        silver = self.with_later_order()
        gold = build_gold(silver=silver, cutoff=CUTOFF, run_id="later-order")
        feature = gold["feat_sku_return_rate_90d"].filter("line_id = 'later-line'").first()
        self.assertEqual(
            (
                feature["labeled_count_90d"],
                feature["positive_count_90d"],
                feature["return_or_refund_rate_90d"],
            ),
            (15, 15, 1.0),
        )
        self.assertEqual(
            {k: v for k, v in validate_gold(silver, gold, CUTOFF)["checks"].items() if v}, {}
        )
        # Count one more label than was knowable at the April order time: the sampled
        # independent recomputation (label_line at order time) must disagree.
        feat = gold["feat_sku_return_rate_90d"]
        poisoned = feat.withColumn(
            "labeled_count_90d",
            F.when(F.col("line_id") == "later-line", F.col("labeled_count_90d") + 1).otherwise(
                F.col("labeled_count_90d")
            ),
        )
        checks = validate_gold(silver, {**gold, "feat_sku_return_rate_90d": poisoned}, CUTOFF)[
            "checks"
        ]
        self.assertGreater(checks["pit_feature_mismatch"], 0)
        # A segment that opens before its decision was observed would leak a future label.
        segments = gold["label_segments"]
        early = segments.withColumn(
            "start_exclusive", F.col("decision_observed_at") - F.expr("INTERVAL 1 DAY")
        )
        checks = validate_gold(silver, {**gold, "label_segments": early}, CUTOFF)["checks"]
        self.assertGreater(checks["pit_segment_before_known"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
