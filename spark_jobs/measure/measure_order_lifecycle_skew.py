"""Measure v2 baseline/skew from the Spark event log, using only the scale source and the real job modules.

No listener callback or network needed. The event log is uncompressed, and Spark is stopped before reading;
the JSON keeps all stages, successful tasks, plans and identifies the data-level exchange for the sweep.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import statistics
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


def distribution(values):
    """Stats over every task including empty ones; with median=0 the ratio is undefined, no fake division by 1."""
    values = sorted(values)
    if not values:
        return dict(max=0, median=0, p95=0, max_median=None)
    median = statistics.median(values)
    return dict(
        max=values[-1],
        median=median,
        p95=values[math.ceil(0.95 * len(values)) - 1],
        max_median=values[-1] / median if median else None,
    )


def _nodes(node):
    """Walk the physical plan tree recorded by SQLExecutionStart."""
    yield node
    for child in node.get("children", []):
        yield from _nodes(child)


def _window_exchanges(plan):
    """Take only Window-level Exchanges through Sort/wrappers; ignore aggregates/joins on the same keys."""

    def inputs(node):
        """Stop at operator boundaries that change data or at the first Exchange."""
        name = node.get("nodeName", "")
        if name == "Exchange":
            yield node
        elif name in (
            "Sort",
            "InputAdapter",
            "Project",
            "AQEShuffleRead",
            "ShuffleQueryStage",
        ) or name.startswith("WholeStageCodegen"):
            for child in node.get("children", []):
                yield from inputs(child)

    for node in _nodes(plan):
        if node.get("nodeName") == "Window":
            for child in node.get("children", []):
                yield from inputs(child)


def read_event_logs(directory):
    """Attach stages to job groups and identify exchange consumers (dataset,SKU[,bucket]).

    records_read = input records + shuffle records; caching does not make this number
    equal output rows. Only sweep consumers with a shuffle are used to compare window load.
    """
    stage_groups, stages, tasks, exchanges, plans = {}, {}, {}, {}, []
    for path in sorted(Path(directory).rglob("*")):
        if not path.is_file() or path.name.startswith("."):
            continue
        with path.open() as stream:
            for line in stream:
                event = json.loads(line)
                kind = event["Event"]
                if kind == "SparkListenerJobStart":
                    group = (event.get("Properties") or {}).get("spark.jobGroup.id", "ungrouped")
                    for sid in event["Stage IDs"]:
                        stage_groups.setdefault(sid, set()).add(group)
                elif kind.endswith(
                    ("SparkListenerSQLExecutionStart", "SparkListenerSQLAdaptiveExecutionUpdate")
                ):
                    plans.append(
                        dict(
                            execution_id=event["executionId"],
                            event=kind,
                            plan=event["physicalPlanDescription"],
                        )
                    )
                    for node in _window_exchanges(event.get("sparkPlanInfo", {})):
                        desc = node.get("simpleString", "")
                        partitioning = re.search(r"hashpartitioning\((.*?), \d+\)", desc)
                        if node.get("nodeName") == "Exchange" and partitioning:
                            keys = partitioning.group(1)
                            key_names = re.sub(r"#\d+L?", "", keys)
                            if key_names in (
                                "dataset_version, seller_sku",
                                "dataset_version, seller_sku, _bucket",
                            ):
                                role = (
                                    "bucket_sweep" if "_bucket#" in keys else "sku_sweep_or_prefix"
                                )
                                for metric in node.get("metrics", []):
                                    exchanges[metric["accumulatorId"]] = role
                elif kind == "SparkListenerStageCompleted":
                    info = event["Stage Info"]
                    stages[(info["Stage ID"], info["Stage Attempt ID"])] = info
                elif (
                    kind == "SparkListenerTaskEnd"
                    and event.get("Task End Reason", {}).get("Reason") == "Success"
                ):
                    info, metrics = event["Task Info"], event.get("Task Metrics", {})
                    shuffle = metrics.get("Shuffle Read Metrics", {})
                    task = dict(
                        partition=info["Index"],
                        task_attempt=info["Attempt"],
                        input_records=metrics.get("Input Metrics", {}).get("Records Read", 0),
                        shuffle_records=shuffle.get("Total Records Read", 0),
                        shuffle_read_bytes=shuffle.get("Remote Bytes Read", 0)
                        + shuffle.get("Local Bytes Read", 0),
                        duration_ms=info["Finish Time"] - info["Launch Time"],
                    )
                    task["records_read"] = task["input_records"] + task["shuffle_records"]
                    key = (event["Stage ID"], event["Stage Attempt ID"], info["Index"])
                    if key not in tasks or tasks[key]["task_attempt"] < task["task_attempt"]:
                        tasks[key] = task
    producers = {}
    for (sid, _), info in stages.items():
        roles = {exchanges[a["ID"]] for a in info.get("Accumulables", []) if a["ID"] in exchanges}
        if roles:
            producers.setdefault(sid, set()).update(roles)
    report = []
    for (sid, attempt), info in sorted(stages.items()):
        rows = [task for (stage, att, _), task in tasks.items() if (stage, att) == (sid, attempt)]
        roles = sorted(
            {role for parent in info.get("Parent IDs", []) for role in producers.get(parent, set())}
        )
        report.append(
            dict(
                stage_id=sid,
                stage_attempt=attempt,
                name=info["Stage Name"],
                groups=sorted(stage_groups.get(sid, [])),
                sweep_exchange_inputs=roles,
                failed=info.get("Failure Reason"),
                scheduled_tasks=info["Number of Tasks"],
                task_count=len(rows),
                records=distribution([r["records_read"] for r in rows]),
                shuffle_records=distribution([r["shuffle_records"] for r in rows]),
                shuffle_read_bytes=distribution([r["shuffle_read_bytes"] for r in rows]),
                duration_ms=distribution([r["duration_ms"] for r in rows]),
                tasks=sorted(rows, key=lambda r: r["partition"]),
            )
        )
    return dict(stages=report, sql_plans=plans)


def run_measurement(
    workdir,
    source=None,
    *,
    days=90,
    orders_per_day=20,
    seed=7,
    sku_count=100,
    buckets=18,
    partitions=16,
    aqe=False,
):
    """Generate/read one source, build real Silver→Gold, pin fact/segments, then measure both sweeps on the same input."""
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    import spark_jobs.lib.order_lifecycle_gold as gold_job
    from generator.generate_order_lifecycle import ScaleConfig, generate_scaled
    from spark_jobs.lib.order_lifecycle_lakehouse import (
        bronze_from_root,
        build_silver,
        read_source_manifest,
    )
    from spark_jobs.lib.order_lifecycle_skew import bucket_width_days, sku_features_salted

    width = bucket_width_days(buckets)
    if type(partitions) is not int or partitions < 1:
        raise ValueError("partitions must be a positive integer")
    workdir = Path(workdir).resolve()
    logdir = workdir / "events"
    logdir.mkdir(parents=True)
    if source is None:
        source = workdir / "source"
        generate_scaled(
            source,
            ScaleConfig(seed=seed, days=days, orders_per_day=orders_per_day, sku_count=sku_count),
        )
    cloud = os.environ.get("PLATFORM_STORAGE") == "s3"
    source = Path(source).resolve() if not cloud else str(source)
    manifest_bytes = None
    if not cloud:
        manifest_bytes = (source / "manifest.json").read_bytes()
        manifest = json.loads(manifest_bytes)
    if not cloud:
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
    os.environ["PLATFORM_CONTRACTS_DIR"] = str(ROOT / "contracts")
    builder = SparkSession.builder
    if cloud:
        # The S3 branch keeps the master from spark-submit; the measurement algorithm is unchanged.
        from spark_jobs.lib.iceberg_session import catalog_conf

        for key, value in catalog_conf(os.environ).items():
            builder = builder.config(key, value)
    else:
        builder = builder.master("local[2]").config("spark.driver.bindAddress", "127.0.0.1")
    spark = (
        builder.appName("order-lifecycle-skew-measurement")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(partitions))
        .config("spark.sql.adaptive.enabled", str(aqe).lower())
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.compress", "false")
        .config("spark.eventLog.rolling.enabled", "false")
        .config("spark.eventLog.dir", logdir.as_uri())
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    if cloud:
        # Take the original manifest bytes so the hash matches the S3 object; do not canonicalize the JSON.
        content = spark.read.text(source.rstrip("/") + "/manifest.json", wholetext=True).collect()
        if len(content) != 1:
            raise ValueError("Expected exactly one manifest.json")
        manifest_bytes = content[0]["value"].encode()
        manifest = read_source_manifest(spark, source)
    cutoff = manifest["horizons"]["source_observation_end"]
    report = dict(
        input_manifest=manifest,
        manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
        source=str(source),
        cutoff=cutoff,
        buckets=buckets,
        bucket_width_days=width,
        runtime=dict(
            spark=spark.version,
            python=sys.version,
            application_id=spark.sparkContext.applicationId,
        ),
        config={
            k: spark.conf.get(k)
            for k in (
                "spark.sql.shuffle.partitions",
                "spark.sql.adaptive.enabled",
                "spark.sql.adaptive.skewJoin.enabled",
                "spark.sql.autoBroadcastJoinThreshold",
                "spark.sql.session.timeZone",
            )
        },
    )
    if cloud:
        report["runtime"]["master"] = spark.sparkContext.master
    try:
        spark.sparkContext.setJobGroup("silver_gold_build", "Silver and Gold from the real modules")
        bronze = bronze_from_root(spark, str(source), "skew").cache()
        silver = build_silver(bronze=bronze, as_of=cutoff, contracts_dir=str(ROOT / "contracts"))
        gold = gold_job.build_gold(
            silver, cutoff, "skew", datetime(2026, 10, 19, tzinfo=timezone.utc)
        )
        fact = gold["fact_order_lines"].localCheckpoint(eager=True)
        segments = gold["label_segments"].localCheckpoint(eager=True)
        report["fact_rows"], report["segment_rows"] = fact.count(), segments.count()
        report["dataset_versions"] = [
            r[0] for r in fact.select("dataset_version").distinct().collect()
        ]
        gold["label_return_v2"].count()
        windows = gold_job._feature_windows(segments, fact.select(*gold_job.KEY, "seller_sku"))
        # Count sweep input with the real endpoints; this is key load, not pretending to be task metrics.
        key_times = windows.select(
            "dataset_version", "seller_sku", F.explode(F.array("a", "b")).alias("time")
        ).unionByName(
            fact.filter("created_at is not null").select(
                "dataset_version", "seller_sku", F.col("created_at").alias("time")
            )
        )
        report["top5_line_skus"] = [
            r.asDict()
            for r in fact.groupBy("dataset_version", "seller_sku")
            .count()
            .orderBy(F.desc("count"), "dataset_version", "seller_sku")
            .limit(5)
            .collect()
        ]
        report["top5_partition_keys"] = {}
        for mode in ("baseline", "salted"):
            keys = ["dataset_version", "seller_sku"]
            source_keys = key_times
            if mode == "salted":
                source_keys = source_keys.withColumn(
                    "_bucket",
                    F.floor(
                        F.datediff(F.to_date("time"), F.lit("1970-01-01"))
                        / F.lit(bucket_width_days(buckets))
                    ),
                )
                keys += ["_bucket"]
            report["top5_partition_keys"][mode] = [
                r.asDict()
                for r in source_keys.groupBy(*keys)
                .count()
                .orderBy(F.desc("count"), *keys)
                .limit(5)
                .collect()
            ]
        outputs = {}
        report["physical_plans"] = {}
        for mode, build in (
            ("baseline", lambda: gold_job.sku_features(fact, segments)),
            ("salted", lambda: sku_features_salted(fact, segments, buckets)),
        ):
            spark.sparkContext.setJobGroup(mode, mode + " sweep on the same fact/segments")
            features = build()
            report["physical_plans"][mode] = (
                features._jdf.queryExecution().executedPlan().toString()
            )
            path = str(workdir / mode)
            features.write.mode("error").parquet(path)
            outputs[mode] = spark.read.parquet(path)
        spark.sparkContext.setJobGroup("correctness", "exceptAll in both directions")
        cols = outputs["baseline"].columns
        left, right = outputs["baseline"].select(*cols), outputs["salted"].select(*cols)
        report["differences"] = dict(
            baseline_only=left.exceptAll(right).count(), salted_only=right.exceptAll(left).count()
        )
        report["nonzero_features"] = left.filter("labeled_count_90d > 0").count()
    finally:
        spark.stop()
    report.update(read_event_logs(logdir))
    for stage in report["stages"]:
        stage["top5_partition_keys"] = {
            mode: report["top5_partition_keys"][mode]
            for mode in ("baseline", "salted")
            if mode in stage["groups"] and stage["sweep_exchange_inputs"]
        }
    return report


def main():
    """CLI needs no catalog/Docker service; output and event logs must be in writable directories."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source")
    parser.add_argument("--workdir", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--orders-per-day", type=int, default=20)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--sku-count", type=int, default=100)
    parser.add_argument("--buckets", type=int, default=18)
    parser.add_argument("--partitions", type=int, default=16)
    parser.add_argument("--aqe", action="store_true")
    args = parser.parse_args()
    workdir = (args.workdir or Path(tempfile.mkdtemp(prefix="v2-skew-"))).resolve()
    workdir.mkdir(parents=True, exist_ok=True)
    report = run_measurement(
        workdir,
        args.source,
        days=args.days,
        orders_per_day=args.orders_per_day,
        seed=args.seed,
        sku_count=args.sku_count,
        buckets=args.buckets,
        partitions=args.partitions,
        aqe=args.aqe,
    )
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(
        json.dumps(
            dict(output=str(args.output), workdir=str(workdir), differences=report["differences"])
        )
    )
    if any(report["differences"].values()):
        raise SystemExit("Salted features differ from baseline")


if __name__ == "__main__":
    main()
