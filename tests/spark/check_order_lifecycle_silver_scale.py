"""Spark reconciliation at scale, 200 Orders/day × 10 days; runs only in the Airflow/Spark image.

The source is freshly generated in /tmp, no catalog, no network; uses an explicit contracts_dir.
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

import spark_jobs.lib.order_lifecycle_lakehouse as v2
from generator.generate_order_lifecycle import ScaleConfig, generate_scaled


class SourceV2ScaleChecks(unittest.TestCase):
    """Full reconciliation of Silver and the line oracle through the public API."""

    @classmethod
    def setUpClass(cls):
        """Generate the scale source into /tmp and build Silver with keyword arguments."""
        cls.tmp = Path(tempfile.mkdtemp(prefix="platform-v2-scale-"))
        cls.root = cls.tmp / "source"
        cls.manifest = generate_scaled(cls.root, ScaleConfig(orders_per_day=200, days=10))
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("source-v2-scale-regression")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.warehouse.dir", str(cls.tmp / "warehouse"))
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.bronze = v2.bronze_from_root(
            spark=cls.spark, root=str(cls.root), run_id="scale-regression"
        ).cache()
        cls.silver = v2.build_silver(bronze=cls.bronze, contracts_dir="/workspace/contracts")

    @classmethod
    def tearDownClass(cls):
        """Stop Spark and delete exactly the test's temporary directory."""
        cls.spark.stop()
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def test_every_reconcile_check_is_zero(self):
        """Every quantity/money/linkage/coverage invariant must be clean."""
        result = v2.reconcile(silver=self.silver)
        self.assertTrue(result["checks"])
        self.assertEqual({k: v for k, v in result["checks"].items() if v != 0}, {})

    def test_contract_quarantine_is_empty(self):
        """The registry accepts the whole default source."""
        self.assertEqual(self.silver["quarantine_records"].count(), 0)

    def test_silver_lines_equal_truth(self):
        """Compare both the count and the set of line identities, avoiding equal counts caused by fan-out."""
        with (self.root / "_truth/lines.jsonl").open() as handle:
            expected = {json.loads(row)["line_id"] for row in handle}
        lines = self.silver["line_commitments"]
        self.assertEqual(lines.count(), len(expected))
        self.assertEqual({r["line_id"] for r in lines.select("line_id").collect()}, expected)


if __name__ == "__main__":
    unittest.main(verbosity=2)
