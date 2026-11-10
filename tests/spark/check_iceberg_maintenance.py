"""Iceberg maintenance: unittest on a local Iceberg Hadoop catalog, offline; uses only jars cached in ivy.

Runs in Docker. The receipt is written to /tmp; repo evidence is not modified.
Evidence is only exported when the whole suite is green; stdout has the JSON + sha256.
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location(
    "iceberg_maintenance", ROOT / "spark_jobs/maintenance/maintain_iceberg_tables.py"
)
job = importlib.util.module_from_spec(spec)
spec.loader.exec_module(job)

from pyspark.sql import SparkSession

from spark_jobs.lib.maintenance_policy import plan_maintenance, validated_config


class MaintenanceChecks(unittest.TestCase):
    """A table with many appends, a generation mid-history and three independent execution branches."""

    @classmethod
    def setUpClass(cls):
        """Temporary Hadoop catalog with the CALL extension, no packages/network/services."""
        cls.temp = tempfile.TemporaryDirectory(prefix="platform-maintenance-")
        jar = Path("/ivy/.ivy2/jars/org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.11.0.jar")
        if not jar.is_file():
            raise RuntimeError(f"Missing offline jar: {jar}")
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("maintenance-regression")
            .config("spark.jars", str(jar))
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", cls.temp.name + "/warehouse")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.spark.sql("CREATE NAMESPACE local.demo")
        cls.receipts = []

    @classmethod
    def tearDownClass(cls):
        """Clean up the local warehouse, never touching project/service volumes."""
        cls.spark.stop()
        cls.temp.cleanup()

    def demo(self, suffix):
        """32 deterministic appends: each file spans the whole sku domain, two identity partitions."""
        table = f"local.demo.maintenance_{suffix}"
        self.spark.sql(f"""CREATE TABLE {table} (
            id BIGINT, part INT, seller_sku STRING, created_at TIMESTAMP, payload STRING
        ) USING iceberg PARTITIONED BY (part)
        TBLPROPERTIES ('write.target-file-size-bytes'='65536',
                       'write.parquet.row-group-size-bytes'='65536',
                       'write.parquet.compression-codec'='uncompressed',
                       'write.metadata.metrics.default'='full',
                       'write.distribution-mode'='none')""")
        for append in range(32):
            frame = self.spark.sql(f"""SELECT cast({append} * 256 + id AS BIGINT) AS id,
                cast({append} % 2 AS INT) AS part,
                format_string('sku-%04d', cast(id AS INT)) AS seller_sku,
                timestamp '2026-01-01 00:00:00' + id * INTERVAL 1 SECOND AS created_at,
                sha2(cast({append} * 256 + id AS STRING), 256) AS payload
                FROM range(256)""").coalesce(1)
            frame.writeTo(table).append()
            if append == 15:
                snap = job.snapshot_id(self.spark, table)
                self.spark.sql(f"ALTER TABLE {table} CREATE TAG `gen-1` AS OF VERSION {snap}")
        time.sleep(1.1)  # Every append is older than the 1-second cutoff at the first planning run.
        config = validated_config(
            {
                "min_input_files": 12,
                "target_file_size_bytes": 65536,
                "small_file_ratio": 1.0,
                "zorder_columns": ["seller_sku", "created_at"],
                "zorder_min_new_files": 20,
                # Keep the last 3 snapshots: last append + compact + zorder, so the second run is truly a no-op.
                "max_snapshot_age_hours": 1 / 3600,
                "retain_last": 3,
                "backoff_seconds": 0,
                "probe_predicates": [
                    {"name": "sku-0001", "ranges": {"seller_sku": ["sku-0001", "sku-0001"]}}
                ],
            }
        )
        return table, config

    def success_case(self, suffix, transient=0):
        """Prove compact/zorder/expiry, unchanged data/tags and a no-op mutation on the second run."""
        table, config = self.demo(suffix)
        tag_before = job.fingerprint(self.spark, table, "gen-1")
        entry = {}
        job.run_table(self.spark, table, config, entry, execute=True, transient_failures=transient)
        kinds = [action["kind"] for action in entry["plan"]["actions"]]
        self.assertEqual(kinds, ["compact", "zorder", "expire_snapshots", "remove_orphans"])
        before, after = entry["before"], entry["after"]
        self.assertLess(after["data_files"], before["data_files"])
        self.assertLess(after["snapshots"], before["snapshots"])
        self.assertEqual(
            (before["rows"], before["fingerprint"]), (after["rows"], after["fingerprint"])
        )
        self.assertLess(
            after["probes"]["sku-0001"]["candidate_data_files"],
            before["probes"]["sku-0001"]["candidate_data_files"],
        )
        self.assertEqual(job.fingerprint(self.spark, table, "gen-1"), tag_before)
        second = plan_maintenance(job.read_metadata(self.spark, table), config)
        self.assertTrue(second["no_op"], second)
        self.assertEqual([a["kind"] for a in second["actions"]], ["remove_orphans"])
        entry["second_plan"] = second
        if transient:
            first_attempts = [a for a in entry["attempts"] if a["kind"] == "compact"]
            self.assertEqual([a["status"] for a in first_attempts], ["failed", "failed", "success"])
        self.receipts.append(entry)
        receipt = {"status": "success", "tables": [entry]}
        job.validate_receipt(self.spark.newSession(), receipt)
        # JSON roundtrip to check the timestamp/procedure outputs can be written to the receipt.
        path = Path(self.temp.name) / (suffix + ".json")
        job.write_receipt(path, receipt)
        job.validate_receipt(self.spark.newSession(), json.loads(path.read_text()))

    def test_01_maintenance_preserves_generation_and_prunes_files(self):
        """Full success case."""
        self.success_case("normal")

    def test_02_transient_retry_twice(self):
        """Injection happens before CALL; the third attempt succeeds."""
        self.success_case("retry", transient=2)

    def test_03_fatal_preserves_snapshot_and_receipt(self):
        """A fatal error on the first action leaves the snapshot unchanged and does not continue."""
        table, config = self.demo("fatal")
        before = job.snapshot_id(self.spark, table)
        entry = {}
        with self.assertRaisesRegex(ValueError, "Injected fatal"):
            job.run_table(self.spark, table, config, entry, execute=True, fatal_failure=True)
        self.assertEqual(job.snapshot_id(self.spark, table), before)
        self.assertEqual(entry["status"], "failed")
        self.assertEqual(len(entry["attempts"]), 1)
        self.receipts.append(entry)


if __name__ == "__main__":
    result = unittest.TextTestRunner(verbosity=2).run(
        unittest.defaultTestLoader.loadTestsFromTestCase(MaintenanceChecks)
    )
    if result.wasSuccessful():
        print(
            "DE_N2_EVIDENCE_JSON="
            + json.dumps(
                {
                    "suite": "tests.spark.check_iceberg_maintenance",
                    "code_sha256": job.code_hashes(),
                    "receipts": MaintenanceChecks.receipts,
                },
                default=str,
            )
        )
    sys.exit(0 if result.wasSuccessful() else 1)
