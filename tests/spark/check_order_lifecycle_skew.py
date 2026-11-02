"""Planner: kiểm tương đương và tải sweep trên nguồn scale nóng; không đọc truth.

Chạy bằng Docker được ghi ở docs/edai-1/reference/spark_order_lifecycle.md. JSON và event log ở /tmp;
stdout in đầy đủ JSON để redirect ra file trên host.
"""

import importlib.util
import json
import os
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
spec = importlib.util.spec_from_file_location(
    "measure_v2_skew", ROOT / "spark_jobs/measure/measure_order_lifecycle_skew.py"
)
measure = importlib.util.module_from_spec(spec)
spec.loader.exec_module(measure)


class SparkV2SkewChecks(unittest.TestCase):
    """Một nguồn chung cho phép so đúng bag và đối chiếu per-task thay vì bấm giờ."""

    def test_handcrafted_boundaries(self):
        """So bag ở biên microsecond/bucket, SKU null, nhiều dataset và line thiếu giờ."""
        from pyspark.sql import SparkSession

        from spark_jobs.lib.order_lifecycle_gold import sku_features
        from spark_jobs.lib.order_lifecycle_skew import sku_features_salted

        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
        spark = (
            SparkSession.builder.master("local[2]")
            .appName("v2-skew-boundaries")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.adaptive.enabled", "false")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        try:
            # Epoch day 20460 là biên bucket 10 ngày. Cửa sổ [edge, edge+10d)
            # có query ngay trước/sau mỗi biên, và query tự trừ chính history.
            edge = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(days=20460)
            micro = timedelta(microseconds=1)
            rows, segments = [], []
            for dataset in ("d1", "d2"):
                for sku in ("hot", None):
                    tag = "null" if sku is None else sku
                    rows.append((dataset, tag + "-history", sku, edge))
                    segments.append(
                        (
                            dataset,
                            tag + "-history",
                            edge - micro,
                            edge + timedelta(days=10) - micro,
                            1,
                            date(2026, 1, 1),
                        )
                    )
                    for i, timestamp in enumerate(
                        (
                            edge - micro,
                            edge,
                            edge + micro,
                            edge + timedelta(days=10) - micro,
                            edge + timedelta(days=10),
                            edge + timedelta(days=40),
                            None,
                        )
                    ):
                        rows.append((dataset, f"{tag}-q{i}", sku, timestamp))
            fact = spark.createDataFrame(
                rows,
                "dataset_version string, line_id string, seller_sku string, created_at timestamp",
            )
            history = spark.createDataFrame(
                segments,
                "dataset_version string, line_id string, start_exclusive timestamp, "
                "end_inclusive timestamp, label int, cohort_date date",
            )
            baseline = sku_features(fact, history).cache()
            salted = sku_features_salted(fact, history, 18).select(*baseline.columns).cache()
            self.assertEqual(baseline.exceptAll(salted).count(), 0)
            self.assertEqual(salted.exceptAll(baseline).count(), 0)
            counts = {(r.dataset_version, r.line_id): r.labeled_count_90d for r in salted.collect()}
            for dataset in ("d1", "d2"):
                for tag in ("hot", "null"):
                    self.assertEqual(
                        [counts[dataset, f"{tag}-q{i}"] for i in range(7)], [0, 1, 1, 1, 0, 0, 0]
                    )
                    self.assertEqual(counts[dataset, tag + "-history"], 0)
        finally:
            spark.stop()

    def test_scale_equivalence_and_sweep_load(self):
        """Seed 7, 90 ngày có history thật; chỉ assert ratio khi tín hiệu phân bố rõ."""
        workdir = Path(tempfile.mkdtemp(prefix="v2-skew-check-")).resolve()
        report = measure.run_measurement(
            workdir, days=90, orders_per_day=4, sku_count=32, seed=7, buckets=18, partitions=16
        )
        print("V2_SKEW_REPORT " + json.dumps(report, ensure_ascii=False), flush=True)
        self.assertEqual(report["differences"], {"baseline_only": 0, "salted_only": 0})
        self.assertGreater(report["nonzero_features"], 0)
        hot = report["top5_partition_keys"]
        self.assertGreater(report["top5_line_skus"][0]["count"], report["fact_rows"] * 0.1)
        candidates = {}
        for mode, role in (("baseline", "sku_sweep_or_prefix"), ("salted", "bucket_sweep")):
            stages = [
                s
                for s in report["stages"]
                if mode in s["groups"]
                and role in s["sweep_exchange_inputs"]
                and not s["failed"]
                and s["shuffle_records"]["max"] > 0
            ]
            self.assertTrue(
                stages,
                "Không định vị được sweep exchange; kiểm sql_plans và event log ở " + str(workdir),
            )
            # Consumer nặng nhất: giữ nguyên mọi task rỗng trong median.
            candidates[mode] = max(stages, key=lambda s: s["shuffle_records"]["max"])
        before, after = (candidates[m]["shuffle_records"] for m in ("baseline", "salted"))
        print(
            "SWEEP_COMPARISON "
            + json.dumps(
                {
                    m: {
                        k: s[k]
                        for k in ("stage_id", "task_count", "shuffle_records", "shuffle_read_bytes")
                    }
                    for m, s in candidates.items()
                }
            ),
            flush=True,
        )
        clear = (
            before["max_median"] is not None
            and after["max_median"] is not None
            and before["max_median"] >= 2
            and candidates["baseline"]["task_count"] >= 8
            and hot["salted"][0]["count"] <= 0.6 * hot["baseline"][0]["count"]
        )
        if clear:
            self.assertLess(after["max_median"], before["max_median"])
        else:
            print(
                "SKEW_INCONCLUSIVE: chưa đủ tín hiệu để assert ratio; không kết luận đã giảm skew.",
                flush=True,
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
