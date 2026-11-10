"""SNAPSHOT BEFORE THE REFACTOR (2026-10-19): only for tests/spark/check_order_lifecycle_gold_equivalence.py to compare against; not used by jobs.

Gold v2 for small fixtures: Silver DataFrames in, Gold DataFrames out.

Policy lives exclusively in return_label; Spark groups raw observed source envelopes per
line and calls that policy in RDD workers. Optional `source_records` is Bronze v2
raw evidence, needed because current Silver catalog tables omit creation clocks,
coverage intervals and revision history. No existing Silver module is modified.
The dataset-sized evidence arrays and line × order-time snapshots are fixture-only.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_jobs.lib.return_label import LABEL_VERSION, label_line, source_line, utc

KEY = ["dataset_version", "line_id"]
IDENTITY = KEY + ["channel", "account_id", "native_order_id", "native_line_id", "seller_sku"]
LABEL_DDL = (
    "dataset_version string, line_id string, cutoff timestamp, generation_id string, computed_at timestamp, "
    "state string, label int, cohort_date date, maturity_end timestamp, qualifying_event_id string, "
    "reasons array<string>, label_version string, decision_observed_at timestamp, source_event_refs array<string>, input_json string"
)
INPUT_DDL = "dataset_version string, line_id string, input_json string, created_at timestamp"


def _input_row(row):
    """Normalize a grouped line's full source observations in a Spark worker."""
    identity = {key: row[key] for key in IDENTITY}
    data = source_line(
        identity, [r["raw_json"] for r in row["raw_records"] if r["raw_json"] is not None]
    )
    return (
        row["dataset_version"],
        row["line_id"],
        json.dumps(data, default=str, sort_keys=True),
        data["created_at"],
    )


def _label_row(row, cutoff, generation, computed):
    """Invoke the single pure label implementation; retain all evidence fields."""
    data = json.loads(row["input_json"])
    result = asdict(label_line(data, cutoff))
    return dict(
        dataset_version=row["dataset_version"],
        line_id=row["line_id"],
        cutoff=utc(cutoff),
        generation_id=generation,
        computed_at=computed,
        input_json=row["input_json"],
        **result,
    )


def label_inputs(inputs: DataFrame, cutoff, run_id: str, computed_at=None) -> DataFrame:
    """Evaluate one label per input line at one observation snapshot."""
    computed = computed_at or datetime.now(timezone.utc)
    return inputs.sparkSession.createDataFrame(
        inputs.rdd.map(lambda row: _label_row(row, cutoff, run_id, computed)), LABEL_DDL
    )


def build_gold(silver: dict, cutoff, run_id: str, computed_at=None) -> dict:
    """Build facts, immutable-snapshot labels and PIT features with auditable lineage.

    `source_records` must contain only observed source envelopes; probes/truth are
    excluded by feed. In-memory build_silver's `events` is a conservative fallback
    (without coverage it yields unknown). No pandas dependency is needed.
    """
    try:
        from spark_jobs.lib.order_lifecycle_lakehouse import OPERATIONAL_FEEDS
    except ImportError:
        from spark_jobs.lib.order_lifecycle_lakehouse import OPERATIONAL_FEEDS
    spark = silver["line_lifecycle"].sparkSession
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    # The snapshot lives in tests/: workers need spark_jobs' return_label and this file itself.
    from spark_jobs.lib.package_distribution import distribute_python_packages

    distribute_python_packages(spark.sparkContext, include_tests=True)
    computed = computed_at or datetime.now(timezone.utc)
    native = silver["native_order_lines"].select(
        "dataset_version", "channel", "native_order_id", "native_line_id", "seller_sku"
    )
    native_key = ["dataset_version", "channel", "native_order_id", "native_line_id"]
    # Fail on ambiguous mapping rather than silently drop/deduplicate source lines.
    if native.groupBy(*native_key).count().filter("count != 1").limit(1).count():
        raise ValueError("Ambiguous native line identity")
    base = silver["line_lifecycle"].join(native, native_key, "left")
    raw = silver.get("source_records", silver.get("events"))
    if raw is None:
        raise ValueError("Gold requires observed source_records or Silver events")
    evidence = (
        raw.filter(
            F.col("feed").isin(
                *OPERATIONAL_FEEDS, "source.coverage_envelopes", "source.uncertainties"
            )
        )
        .select("dataset_version", "raw_json")
        .distinct()
    )
    # Group at the exact line grain. Include the whole dataset feed so the pure
    # adapter can validate the source-wide coverage digest before scope narrowing.
    grouped = (
        base.select(*IDENTITY)
        .join(evidence, "dataset_version", "left")
        .groupBy(*IDENTITY)
        .agg(F.collect_list(F.struct("raw_json")).alias("raw_records"))
    )
    inputs = spark.createDataFrame(grouped.rdd.map(_input_row), INPUT_DDL).cache()
    fact = base.join(inputs.select(*KEY, "created_at"), KEY, "left").withColumn(
        "generation_id", F.lit(run_id)
    )
    labels = label_inputs(inputs, cutoff, run_id, computed).cache()
    # Evaluate source history at each target order time. Final run labels are never
    # reused for old feature timestamps: that would leak future corrections.
    times = (
        fact.select(F.col("created_at").alias("feature_time"))
        .where("feature_time IS NOT NULL")
        .distinct()
    )
    snapshots_input = inputs.crossJoin(times)
    snapshots = spark.createDataFrame(
        snapshots_input.rdd.map(lambda row: _label_row(row, row["feature_time"], run_id, computed)),
        LABEL_DDL,
    ).cache()
    historical = snapshots.join(fact.select(*KEY, "seller_sku"), KEY)
    target = fact.select(*KEY, "seller_sku", "created_at").alias("t")
    history = historical.alias("h")
    condition = (
        (F.col("t.dataset_version") == F.col("h.dataset_version"))
        & (F.col("t.seller_sku") == F.col("h.seller_sku"))
        & (F.col("t.line_id") != F.col("h.line_id"))
        & (F.col("h.cutoff") == F.col("t.created_at"))
        & (F.col("h.state") == "labeled")
        & (F.col("h.decision_observed_at") < F.col("t.created_at"))
        & (F.col("h.maturity_end") < F.col("t.created_at"))
        & (F.col("h.cohort_date") >= F.date_sub(F.to_date("t.created_at"), 90))
        & (F.col("h.cohort_date") < F.to_date("t.created_at"))
    )
    contributions = target.join(history, condition, "inner").select(
        F.col("t.dataset_version").alias("dataset_version"),
        F.col("t.line_id").alias("line_id"),
        F.col("t.created_at").alias("event_timestamp"),
        F.col("h.line_id").alias("history_line_id"),
        F.col("h.cutoff").alias("history_cutoff"),
        F.col("h.label").alias("history_label"),
        F.col("h.decision_observed_at").alias("history_decision_observed_at"),
        F.col("h.maturity_end").alias("history_maturity_end"),
    )
    counts = contributions.groupBy(*KEY).agg(
        F.count("*").alias("labeled_count_90d"), F.sum("history_label").alias("positive_count_90d")
    )
    features = (
        fact.select(*KEY, "seller_sku", "created_at")
        .join(counts, KEY, "left")
        .fillna(0, subset=["labeled_count_90d", "positive_count_90d"])
        .withColumn(
            "return_rate_90d",
            F.when(
                F.col("labeled_count_90d") > 0,
                F.col("positive_count_90d") / F.col("labeled_count_90d"),
            ),
        )
        .withColumn("event_timestamp", F.col("created_at"))
        .withColumn("created", F.lit(computed).cast("timestamp"))
        .withColumn("label_version", F.lit(LABEL_VERSION))
        .withColumn("generation_id", F.lit(run_id))
    )
    return dict(
        fact_order_lines=fact,
        label_return_v2=labels,
        feat_sku_return_rate_90d=features,
        feature_contributions=contributions,
        feature_label_snapshots=snapshots,
        label_inputs=inputs,
    )


def validate_gold(silver: dict, gold: dict, cutoff, sample_limit: int = 200) -> dict:
    """Count violations, independently joining feature lineage to observed label evidence.

    Feature expected counts are reconstructed from snapshot labels, rather than by
    calling build_gold or its feature logic. Retained contributions must equal this
    independent set; an injected late-known row cannot disappear behind aggregate 0.
    """
    fact, labels, feat = (
        gold[name] for name in ("fact_order_lines", "label_return_v2", "feat_sku_return_rate_90d")
    )
    checks = {}
    checks["fact_line_count_delta"] = abs(fact.count() - silver["line_lifecycle"].count())

    def total(frame):
        """Compare minor-unit totals, including the empty-dataset case."""
        return frame.agg(F.coalesce(F.sum("headline_product_net_minor"), F.lit(0))).first()[0]

    checks["fact_headline_delta_minor"] = abs(total(fact) - total(silver["line_lifecycle"]))
    checks["duplicate_fact_lines"] = fact.groupBy(*KEY).count().filter("count != 1").count()
    expected_keys = fact.select(*KEY)
    checks["missing_or_extra_label_lines"] = (
        expected_keys.join(labels.select(*KEY), KEY, "left_anti").count()
        + labels.select(*KEY).join(expected_keys, KEY, "left_anti").count()
    )
    checks["duplicate_labels"] = labels.groupBy(*KEY, "cutoff").count().filter("count != 1").count()
    checks["invalid_label_state"] = labels.filter(
        (~F.col("state").isin("labeled", "unknown", "immature", "ineligible"))
        | F.col("state").isNull()
        | ((F.col("state") == "labeled") & (F.col("label").isNull() | ~F.col("label").isin(0, 1)))
        | ((F.col("state") != "labeled") & F.col("label").isNotNull())
    ).count()
    checks["labeled_not_mature"] = labels.filter(
        (F.col("state") == "labeled")
        & (F.col("maturity_end").isNull() | (F.col("maturity_end") >= F.col("cutoff")))
    ).count()
    generations = [r[0] for r in labels.select("generation_id").distinct().collect()]
    checks["generation_mismatch"] = (
        int(len(generations) != 1 or generations[0] is None) if generations else 1
    )
    if len(generations) == 1 and generations[0] is not None:
        for frame in (fact, feat, gold["feature_label_snapshots"]):
            checks["generation_mismatch"] += frame.filter(
                ~F.col("generation_id").eqNullSafe(F.lit(generations[0]))
            ).count()
    checks["wrong_cutoff"] = labels.filter(
        F.col("cutoff").isNull() | (F.col("cutoff") != F.lit(utc(cutoff)))
    ).count()
    checks["missing_feature_clock_or_sku"] = feat.filter(
        F.col("event_timestamp").isNull()
        | F.col("created").isNull()
        | F.col("seller_sku").isNull()
        | (F.col("event_timestamp") != F.col("created_at"))
    ).count()
    checks["future_created_lines"] = fact.filter(F.col("created_at") >= F.lit(utc(cutoff))).count()
    checks["feature_grain_mismatch"] = (
        abs(feat.count() - fact.count()) + feat.groupBy(*KEY).count().filter("count != 1").count()
    )
    checks["wrong_label_version"] = (
        labels.filter(
            F.col("label_version").isNull() | (F.col("label_version") != LABEL_VERSION)
        ).count()
        + feat.filter(
            F.col("label_version").isNull() | (F.col("label_version") != LABEL_VERSION)
        ).count()
    )
    # Independent reference join (deliberately no helper shared with feature build).
    ref = (
        gold["feature_label_snapshots"]
        .filter("state = 'labeled'")
        .join(fact.select(*KEY, "seller_sku"), KEY)
        .alias("r")
    )
    expected = (
        feat.alias("f")
        .join(
            ref,
            (F.col("f.dataset_version") == F.col("r.dataset_version"))
            & (F.col("f.seller_sku") == F.col("r.seller_sku"))
            & (F.col("f.line_id") != F.col("r.line_id"))
            & (F.col("r.cutoff") == F.col("f.event_timestamp"))
            & (F.col("r.decision_observed_at") < F.col("f.event_timestamp"))
            & (F.col("r.maturity_end") < F.col("f.event_timestamp"))
            & (F.col("r.cohort_date") >= F.date_sub(F.to_date("f.event_timestamp"), 90))
            & (F.col("r.cohort_date") < F.to_date("f.event_timestamp")),
            "inner",
        )
        .select(
            F.col("f.dataset_version").alias("dataset_version"),
            F.col("f.line_id").alias("line_id"),
            F.col("r.line_id").alias("history_line_id"),
            F.col("r.cutoff").alias("history_cutoff"),
            F.col("r.label").alias("history_label"),
        )
    )
    lineage_cols = KEY + ["history_line_id", "history_cutoff", "history_label"]
    actual = gold["feature_contributions"].select(*lineage_cols)
    checks["pit_invalid_contributions"] = (
        actual.exceptAll(expected).count() + expected.exceptAll(actual).count()
    )
    joined = (
        gold["feature_contributions"]
        .alias("c")
        .join(
            gold["feature_label_snapshots"].alias("s"),
            (F.col("c.dataset_version") == F.col("s.dataset_version"))
            & (F.col("c.history_line_id") == F.col("s.line_id"))
            & (F.col("c.history_cutoff") == F.col("s.cutoff")),
            "left",
        )
    )
    checks["pit_late_evidence"] = joined.filter(
        F.col("s.line_id").isNull()
        | (F.col("s.decision_observed_at") >= F.col("c.event_timestamp"))
        | (F.col("c.history_decision_observed_at") >= F.col("c.event_timestamp"))
        | (F.col("s.maturity_end") >= F.col("c.event_timestamp"))
        | (F.col("c.history_cutoff") != F.col("c.event_timestamp"))
    ).count()
    # Join every referenced decision observation back to the preserved input,
    # independently of the derived availability timestamp and feature builder.
    events = (
        gold["label_inputs"]
        .select(
            *KEY,
            F.explode(
                F.from_json(
                    F.get_json_object("input_json", "$.events"),
                    "array<struct<source_event_id:string,source_observed_at:string>>",
                )
            ).alias("e"),
        )
        .select(
            *KEY,
            F.col("e.source_event_id").alias("source_ref"),
            F.to_timestamp("e.source_observed_at").alias("observed_at"),
        )
    )
    refs = (
        gold["feature_label_snapshots"]
        .select(*KEY, "cutoff", F.explode("source_event_refs").alias("source_ref"))
        .join(events, KEY + ["source_ref"], "left")
    )
    decisions = refs.groupBy(*KEY, "cutoff").agg(
        F.max("observed_at").alias("actual_decision_at"),
        F.sum(F.when(F.col("observed_at").isNull(), 1).otherwise(0)).alias("missing_refs"),
    )
    observed = (
        gold["feature_contributions"]
        .alias("c")
        .join(
            decisions.alias("d"),
            (F.col("c.dataset_version") == F.col("d.dataset_version"))
            & (F.col("c.history_line_id") == F.col("d.line_id"))
            & (F.col("c.history_cutoff") == F.col("d.cutoff")),
            "left",
        )
    )
    checks["pit_source_observations"] = observed.filter(
        F.col("d.actual_decision_at").isNull()
        | (F.col("d.missing_refs") > 0)
        | (F.col("d.actual_decision_at") >= F.col("c.event_timestamp"))
    ).count()
    aggregates = expected.groupBy(*KEY).agg(
        F.count("*").alias("n"), F.sum("history_label").alias("p")
    )
    check_feat = feat.join(aggregates, KEY, "left").fillna(0, subset=["n", "p"])
    checks["feature_aggregate_mismatch"] = check_feat.filter(
        F.col("labeled_count_90d").isNull()
        | F.col("positive_count_90d").isNull()
        | (F.col("labeled_count_90d") != F.col("n"))
        | (F.col("positive_count_90d") != F.col("p"))
        | ((F.col("n") == 0) & F.col("return_rate_90d").isNotNull())
        | ((F.col("n") > 0) & (~F.col("return_rate_90d").eqNullSafe(F.col("p") / F.col("n"))))
    ).count()
    sample = gold["label_inputs"].orderBy(*KEY).limit(sample_limit)
    inputs = {tuple(row[k] for k in KEY): row["input_json"] for row in sample.collect()}
    rows = labels.join(sample.select(*KEY), KEY).collect()
    mismatch = 0
    for row in rows:
        pure = asdict(label_line(json.loads(inputs[tuple(row[k] for k in KEY)]), cutoff))
        for name, value in pure.items():
            actual_value = row[name]
            if isinstance(value, datetime):
                actual_value = utc(actual_value) if actual_value else None
            if actual_value != value:
                mismatch += 1
                break
    checks["python_spark_label_mismatch"] = mismatch
    return {"checks": checks, "info": {"fact_rows": fact.count(), "sampled_labels": len(rows)}}
