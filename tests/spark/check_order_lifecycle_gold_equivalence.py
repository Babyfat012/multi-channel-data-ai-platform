"""The new Gold v2 (reads only Silver, linear) must produce EXACTLY the old Gold (reads Bronze, quadratic).

`tests/spark/reference_order_lifecycle_gold.py` is a snapshot of the Gold module before the refactor, kept only for comparison here.
Two source sets: the default fixture and a small scale source (many SKUs, late, correction, refund
retry). Compares fact, labels (except `input_json`: the new version only holds the line's evidence) and features
with two-way `exceptAll` + fingerprint. Run (Docker):

    docker run --rm --network none --read-only --tmpfs /tmp:rw,size=2g \\
      -e PYSPARK_SUBMIT_ARGS='--driver-memory 3g pyspark-shell' \\
      --mount type=bind,src=$PWD,dst=/workspace,readonly --entrypoint python \\
      platform-airflow:2.10.5-spark3.5.3 /workspace/tests/spark/check_order_lifecycle_gold_equivalence.py
"""

import os
import shutil
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
os.environ["PLATFORM_CONTRACTS_DIR"] = str(ROOT / "contracts")

from pyspark.sql import SparkSession

import spark_jobs.lib.order_lifecycle_gold as new
import tests.spark.reference_order_lifecycle_gold as old
from generator.generate_order_lifecycle import ScaleConfig, generate_scaled
from generator.generate_order_lifecycle_fixture import generate_fixture
from spark_jobs.lib.order_lifecycle_lakehouse import bronze_from_root, build_silver

COMPUTED = datetime(2026, 10, 19, tzinfo=timezone.utc)


def _cutoff(root):
    """Cutoff = observation end of the generated source, so every line is old enough to mature."""
    import json

    horizons = json.loads((root / "manifest.json").read_text())["horizons"]
    return horizons["source_observation_end"]


def _diff(left, right, drop=()):
    """Rows in exactly one side (bag semantics), after dropping columns that may legitimately differ."""
    cols = [c for c in left.columns if c not in drop]
    a, b = left.select(*cols), right.select(*cols)
    return a.exceptAll(b).count() + b.exceptAll(a).count()


class GoldV2Equivalence(unittest.TestCase):
    """Fixture and small scale: new Gold == legacy Gold, table by table."""

    @classmethod
    def setUpClass(cls):
        """One Spark session; sources generated into /tmp only."""
        cls.tmp = Path(tempfile.mkdtemp(prefix="gold-v2-eq-"))
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("gold-v2-equivalence")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        """Release Spark and /tmp sources."""
        cls.spark.stop()
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def _persist(self, root, side, tables):
        """Materialize the three compared tables into /tmp, returning DataFrames read back from the files."""
        out = {}
        for name in ("fact_order_lines", "label_return_v2", "feat_sku_return_rate_90d"):
            path = str(self.tmp / f"{root.name}-{side}-{name}")
            tables[name].write.mode("overwrite").parquet(path)
            out[name] = self.spark.read.parquet(path)
        return out

    def _compare(self, root, cutoff):
        """Build Silver once, Gold both ways, assert identical outputs; return sizes for the log."""
        bronze = bronze_from_root(self.spark, str(root), "eq").cache()
        silver = build_silver(bronze=bronze, as_of=cutoff)
        # Like the real job: Gold reads WRITTEN Silver. An unmaterialized Silver plan grows deeper with the number of days
        # (StackOverflowError during planning on a 90-day source).
        for name in (
            "line_lifecycle",
            "native_order_lines",
            "observed_evidence",
            "coverage_validity",
        ):
            path = str(self.tmp / f"{root.name}-silver-{name}")
            silver[name].write.mode("overwrite").parquet(path)
            silver[name] = self.spark.read.parquet(path)
        fresh = new.build_gold(silver=silver, cutoff=cutoff, run_id="eq", computed_at=COMPUTED)
        legacy = old.build_gold(
            silver={**silver, "source_records": bronze},
            cutoff=cutoff,
            run_id="eq",
            computed_at=COMPUTED,
        )
        # Write to Parquet and read back to cut lineage: exceptAll on an unmaterialized plan OOMs at 6g.
        fresh = {**fresh, **self._persist(root, "fresh", fresh)}
        legacy = {**legacy, **self._persist(root, "legacy", legacy)}
        diffs = {
            "fact_order_lines": _diff(fresh["fact_order_lines"], legacy["fact_order_lines"]),
            "label_return_v2": _diff(
                fresh["label_return_v2"],
                legacy["label_return_v2"],
                drop=("input_json", "positive_kind"),
            ),
            "feat_sku_return_rate_90d": _diff(
                fresh["feat_sku_return_rate_90d"],
                legacy["feat_sku_return_rate_90d"].withColumnRenamed(
                    "return_rate_90d", "return_or_refund_rate_90d"
                ),
            ),
        }
        sizes = {name: fresh[name].count() for name in diffs}
        sizes["labeled"] = fresh["label_return_v2"].filter("state = 'labeled'").count()
        sizes["nonzero_features"] = (
            fresh["feat_sku_return_rate_90d"].filter("labeled_count_90d > 0").count()
        )
        sizes["segments"] = fresh["label_segments"].count()
        # Sample = every line: the independent oracle (label_line at order time) checks ALL features.
        checks = new.validate_gold(silver, fresh, cutoff, feature_sample=100_000)["checks"]
        print(
            "EQUIVALENCE",
            root.name,
            {"diffs": diffs, "sizes": sizes, "violations": {k: v for k, v in checks.items() if v}},
            flush=True,
        )
        self.assertEqual(diffs, {name: 0 for name in diffs})
        self.assertEqual({k: v for k, v in checks.items() if v}, {})
        return sizes

    def test_fixture(self):
        """Default fixture (30 lines, all features zero)."""
        root = self.tmp / "fixture"
        generate_fixture(root)
        self._compare(root, "2026-06-01T00:00:00.000000Z")

    def test_small_scale_has_real_features(self):
        """A seeded legacy-sized scale source runs: the new fact/labels/features must equal legacy exactly."""
        root = self.tmp / "scale"
        generate_scaled(
            root,
            ScaleConfig(
                seed=7,
                days=60,
                orders_per_day=1,
                sku_count=2,
                followup_days=150,
                observation_days=165,
            ),
        )
        sizes = self._compare(root, _cutoff(root))
        # No assert that features are non-zero: the legacy version (replicating coverage per line) OOMs on a source long enough to
        # have non-zero features. Non-zero features are checked in tests/spark/check_order_lifecycle_skew.py and by the independent
        # `pit_feature_mismatch` oracle of validate_gold; here the numbers are only printed for reading.
        print("NONZERO_FEATURES", sizes["nonzero_features"], flush=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
