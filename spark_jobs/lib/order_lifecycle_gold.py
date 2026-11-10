"""Gold v2: Silver DataFrames in, Gold DataFrames out; reads only Silver.

Label rules live entirely in `return_label`; Spark only ships evidence and calls those rules in workers.
Two choices keep the cost linear instead of quadratic:

- Per-line evidence is fetched by key from `silver.lifecycle_stg_observed_evidence` (line, order,
  corrections of events belonging to the line). Only keyless records (coverage, Channel-wide scope) are
  sent once to each worker. The coverage digest is checked once by Silver.
- PIT features use `return_label.label_segments`: a line's label only changes at its own observation
  times or when the wait expires, so counting per SKU is a single sweep over half-open windows.

`tests/spark/check_order_lifecycle_gold_equivalence.py` checks the result matches the old quadratic version.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timedelta, timezone

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from spark_jobs.lib.iceberg_session import CATALOG
from spark_jobs.lib.package_distribution import distribute_python_packages
from spark_jobs.lib.receipts import completed_at_utc
from spark_jobs.lib.return_label import LABEL_VERSION, label_line, label_segments, source_line, utc

SILVER_TABLES = ("line_lifecycle", "native_order_lines", "observed_evidence", "coverage_validity")
GOLD_TABLES = (
    "fact_order_lines",
    "label_return_v2",
    "feat_sku_return_rate_90d",
    "label_segments",
    "label_inputs",
)


def read_silver(spark, cutoff):
    """Read published Silver; reject snapshots not built with `as_of == cutoff`."""
    info = spark.table(f"{CATALOG}.silver.lifecycle_stg_build_info").collect()
    if len(info) != 1 or info[0]["as_of"] is None or utc(info[0]["as_of"]) != utc(cutoff):
        raise ValueError(
            f"Silver as_of={[r['as_of'] for r in info]} != cutoff {utc(cutoff).isoformat()}; "
            "run job 21 --as-of <cutoff> first"
        )
    silver = {name: spark.table(f"{CATALOG}.silver.lifecycle_stg_{name}") for name in SILVER_TABLES}
    late = (
        silver["observed_evidence"]
        .filter(F.col("source_observed_at") >= F.lit(utc(cutoff)))
        .limit(1)
        .count()
    )
    if late:
        raise ValueError(
            "observed_evidence holds observations at/after cutoff; Silver snapshot is not as_of cutoff"
        )
    return silver


KEY = ["dataset_version", "line_id"]
IDENTITY = KEY + ["channel", "account_id", "native_order_id", "native_line_id", "seller_sku"]
LABEL_DDL = (
    "dataset_version string, line_id string, cutoff timestamp, generation_id string, computed_at timestamp, "
    "state string, label int, cohort_date date, maturity_end timestamp, qualifying_event_id string, "
    "reasons array<string>, label_version string, decision_observed_at timestamp, source_event_refs array<string>, input_json string, "
    "positive_kind string"
)
INPUT_DDL = "dataset_version string, line_id string, input_json string, created_at timestamp"
SEGMENT_DDL = (
    "dataset_version string, line_id string, start_exclusive timestamp, end_inclusive timestamp, label int, "
    "cohort_date date, maturity_end timestamp, decision_observed_at timestamp, qualifying_event_id string"
)
# Driver limit: 39 weeks × 9 feeds = 351 complete envelopes/dataset, below 10,000.
# source_line filters the interval in the worker before building input_json; weeks are not multiplied into each line.
MAX_KEYLESS_EVIDENCE = 10_000
# Used only by Silver to check coverage (`coverage_validity`); once `coverage_status` exists, return_label does not
# read it. This field grows with the data (hundreds of KB at scale), so drop it before sending to each line.
VERIFICATION_ONLY_FIELDS = ("source_sequence_ranges",)


def _slim(raw_json: str) -> str:
    """Drop fields used only for coverage checks from a keyless record."""
    record = json.loads(raw_json)
    for name in VERIFICATION_ONLY_FIELDS:
        record.pop(name, None)
    return json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _input_row(row, statuses, keyless=None):
    """Normalize one line's candidate observations in a Spark worker (exact filtering is in return_label).

    `keyless` = {dataset_version: [raw_json]}: records not tied to a line/Order, sent once per
    worker instead of being replicated per line in Spark (cost: line count × record size).
    """
    identity = {key: row[key] for key in IDENTITY}
    raws = [r for r in row["raw_records"] if r is not None]
    raws += (keyless or {}).get(row["dataset_version"], [])
    data = source_line(identity, raws, coverage_status=statuses.get(row["dataset_version"], {}))
    return (
        row["dataset_version"],
        row["line_id"],
        json.dumps(data, default=str, sort_keys=True),
        data["created_at"],
    )


def _label_row(row, cutoff, generation, computed):
    """Call the single pure label rule; keep every evidence column."""
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


def _segment_rows(row):
    """Every labeled cutoff segment of a line (`return_label.label_segments`)."""
    for segment in label_segments(json.loads(row["input_json"])):
        yield (
            row["dataset_version"],
            row["line_id"],
            segment["start_exclusive"],
            segment["end_inclusive"],
            segment["label"],
            segment["cohort_date"],
            segment["maturity_end"],
            segment["decision_observed_at"],
            segment["qualifying_event_id"],
        )


def label_inputs(inputs: DataFrame, cutoff, run_id: str, computed_at=None) -> DataFrame:
    """Compute one label per input line at one observation snapshot."""
    computed = computed_at or datetime.now(timezone.utc)
    return inputs.sparkSession.createDataFrame(
        inputs.rdd.map(lambda row: _label_row(row, cutoff, run_id, computed)), LABEL_DDL
    )


def keyless_evidence(evidence: DataFrame) -> dict:
    """{dataset_version: [raw_json without coverage-check fields]} for records that go to EVERY line.

    Includes records with no line/Order scope, and corrections of those same records (because the
    corrected record sits on every line). Collected to the driver once; count is capped by MAX_KEYLESS_EVIDENCE.
    """
    has_line = F.col("ev_line_id").isNotNull() | F.col("ev_line_ids").isNotNull()
    has_order = F.col("ev_order_id").isNotNull() | F.col("ev_order_ids").isNotNull()
    keyless = evidence.filter(~has_line & ~has_order).select(
        "dataset_version", "record_sha256", "raw_json", "evidence_event_id"
    )
    if keyless.count() > MAX_KEYLESS_EVIDENCE:
        raise ValueError(
            f"More than {MAX_KEYLESS_EVIDENCE} keyless evidence records; scope extraction is wrong"
        )
    held = keyless.select(
        "dataset_version", F.col("evidence_event_id").alias("corrects_source_event_id")
    )
    corrections = evidence.join(
        held, ["dataset_version", "corrects_source_event_id"], "left_semi"
    ).select("dataset_version", "record_sha256", "raw_json")
    rows = (
        keyless.select("dataset_version", "record_sha256", "raw_json")
        .unionByName(corrections)
        .dropDuplicates(["dataset_version", "record_sha256"])
        .collect()
    )
    out = {}
    for row in sorted(rows, key=lambda r: (r["dataset_version"], r["record_sha256"])):
        out.setdefault(row["dataset_version"], []).append(_slim(row["raw_json"]))
    return out


def line_evidence(lines: DataFrame, evidence: DataFrame) -> DataFrame:
    """(line, raw_json) pairs for KEYED records; a superset of what `visible_events` keeps.

    Keys match `scope_matches`: a line-scoped record goes to that line, an order-scoped one to every line of
    the order, and a correction also goes to the line holding the record it corrects. Keyless records go through
    `keyless_evidence`, with no lines × records join.
    """
    ev = evidence.select(
        "dataset_version",
        "record_sha256",
        "raw_json",
        "evidence_event_id",
        "corrects_source_event_id",
        "ev_line_id",
        "ev_line_ids",
        "ev_order_id",
        "ev_order_ids",
    )
    line_ids = lines.select("dataset_version", "line_id")
    has_line = F.col("ev_line_id").isNotNull() | F.col("ev_line_ids").isNotNull()
    has_order = F.col("ev_order_id").isNotNull() | F.col("ev_order_ids").isNotNull()
    by_line = (
        ev.filter(has_line)
        .withColumn(
            "line_id",
            F.explode(
                F.when(F.col("ev_line_id").isNotNull(), F.array("ev_line_id")).otherwise(
                    F.col("ev_line_ids")
                )
            ),
        )
        .join(line_ids, ["dataset_version", "line_id"], "left_semi")
    )
    by_order = (
        ev.filter(~has_line & has_order)
        .withColumn(
            "native_order_id",
            F.explode(
                F.when(F.col("ev_order_id").isNotNull(), F.array("ev_order_id")).otherwise(
                    F.col("ev_order_ids")
                )
            ),
        )
        .join(
            lines.select("dataset_version", "native_order_id", "line_id"),
            ["dataset_version", "native_order_id"],
        )
        .drop("native_order_id")
    )
    columns = ["dataset_version", "line_id", "record_sha256", "raw_json", "evidence_event_id"]
    direct = by_line.select(*columns).unionByName(by_order.select(*columns))
    held = direct.select(
        "dataset_version", "line_id", F.col("evidence_event_id").alias("corrects_source_event_id")
    )
    corrections = (
        ev.filter(F.col("corrects_source_event_id").isNotNull())
        .join(held, ["dataset_version", "corrects_source_event_id"])
        .select(*columns)
    )
    return direct.unionByName(corrections).dropDuplicates(
        ["dataset_version", "line_id", "record_sha256"]
    )


def _feature_windows(segments: DataFrame, skus: DataFrame) -> DataFrame:
    """Order-time ranges [a, b) in which a labeled segment is counted (same as `return_label.feature_window`)."""
    micro = F.expr("INTERVAL 1 MICROSECOND")
    begin = F.greatest(
        F.col("start_exclusive") + micro, F.to_timestamp(F.date_add("cohort_date", 1))
    )
    end = F.least(F.col("end_inclusive") + micro, F.to_timestamp(F.date_add("cohort_date", 91)))
    return (
        segments.join(skus, KEY)
        .select(*KEY, "seller_sku", "label", begin.alias("a"), end.alias("b"))
        .filter(F.col("a") < F.col("b"))
    )


def _sweep(windows: DataFrame, queries: DataFrame) -> DataFrame:
    """Running totals (n, p) of the windows open at each query (dataset, SKU, time, line_id).

    +1/+label when a window opens, −1/−label when it closes. At equal timestamps window changes sort before
    queries (a ≤ t is counted, b ≤ t is not).
    """
    group = ["dataset_version", "seller_sku"]
    opens = windows.select(
        *group,
        F.col("a").alias("time"),
        F.lit(0).alias("kind"),
        F.lit(1).alias("dc"),
        F.col("label").alias("dp"),
        F.lit(None).cast("string").alias("line_id"),
    )
    closes = windows.select(
        *group,
        F.col("b").alias("time"),
        F.lit(0).alias("kind"),
        F.lit(-1).alias("dc"),
        (-F.col("label")).alias("dp"),
        F.lit(None).cast("string").alias("line_id"),
    )
    queries = queries.select(
        *group,
        "time",
        F.lit(1).alias("kind"),
        F.lit(0).alias("dc"),
        F.lit(0).alias("dp"),
        "line_id",
    )
    running = (
        Window.partitionBy(*group).orderBy("time", "kind").rowsBetween(Window.unboundedPreceding, 0)
    )
    return (
        opens.unionByName(closes)
        .unionByName(queries)
        .withColumn("n", F.sum("dc").over(running))
        .withColumn("p", F.sum("dp").over(running))
        .filter(F.col("kind") == 1)
        .select(*group, "time", "line_id", "n", "p")
    )


def _with_rate(frame: DataFrame) -> DataFrame:
    """Rate = positive / labeled; NULL when no line has a label yet."""
    return frame.withColumn(
        "return_or_refund_rate_90d",
        F.when(
            F.col("labeled_count_90d") > 0,
            F.col("positive_count_90d") / F.col("labeled_count_90d"),
        ),
    )


def sku_features(fact: DataFrame, segments: DataFrame) -> DataFrame:
    """Per-line features: labeled history of the same SKU, known at order time, 90-day cohort.

    Reads the `_sweep` running totals at order time, then subtracts the line's own window.
    """
    windows = _feature_windows(segments, fact.select(*KEY, "seller_sku"))
    queries = fact.filter(F.col("created_at").isNotNull()).select(
        "dataset_version", "seller_sku", F.col("created_at").alias("time"), "line_id"
    )
    swept = _sweep(windows, queries).select("dataset_version", "line_id", "n", "p")
    own = (
        windows.join(fact.select(*KEY, "created_at"), KEY)
        .filter((F.col("a") <= F.col("created_at")) & (F.col("created_at") < F.col("b")))
        .groupBy(*KEY)
        .agg(F.count("*").alias("own_n"), F.sum("label").alias("own_p"))
    )
    counts = (
        swept.join(own, KEY, "left")
        .fillna(0, subset=["own_n", "own_p"])
        .select(
            *KEY,
            (F.col("n") - F.col("own_n")).cast("bigint").alias("labeled_count_90d"),
            (F.col("p") - F.col("own_p")).cast("bigint").alias("positive_count_90d"),
        )
    )
    return _with_rate(
        fact.select(*KEY, "seller_sku", "created_at")
        .join(counts, KEY, "left")
        .fillna(0, subset=["labeled_count_90d", "positive_count_90d"])
    )


def sku_daily_features(fact: DataFrame, segments: DataFrame, cutoff) -> DataFrame:
    """SKU feature snapshot at 00:00 UTC each day, from the SKU's first order day to the cutoff.

    Same definition as `sku_features` (same windows, same `_sweep`) but queried per day instead of
    per line, so Feast can use it for both training (PIT join) and serving (latest value). There is no
    "own line" to subtract. `event_timestamp` = snapshot time.
    """
    windows = _feature_windows(segments, fact.select(*KEY, "seller_sku"))
    days = (
        fact.filter(F.col("created_at").isNotNull())
        .groupBy("dataset_version", "seller_sku")
        .agg(F.to_date(F.min("created_at")).alias("first_day"))
        .select(
            "dataset_version",
            "seller_sku",
            F.explode(
                F.sequence("first_day", F.to_date(F.lit(utc(cutoff))), F.expr("INTERVAL 1 DAY"))
            ).alias("day"),
        )
        .select(
            "dataset_version",
            "seller_sku",
            F.to_timestamp("day").alias("time"),
            F.lit(None).cast("string").alias("line_id"),
        )
    )
    return _with_rate(
        _sweep(windows, days).select(
            "dataset_version",
            "seller_sku",
            F.col("time").alias("event_timestamp"),
            F.col("n").cast("bigint").alias("labeled_count_90d"),
            F.col("p").cast("bigint").alias("positive_count_90d"),
        )
    )


def build_gold(silver: dict, cutoff, run_id: str, computed_at=None) -> dict:
    """Build the fact, snapshot labels, label segments and PIT features from Silver only.

    Needs `line_lifecycle`, `native_order_lines`, `observed_evidence`, `coverage_validity` (built
    by `order_lifecycle_lakehouse.build_silver` with `as_of = cutoff`).
    """
    spark = silver["line_lifecycle"].sparkSession
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    distribute_python_packages(spark.sparkContext)
    computed = computed_at or datetime.now(timezone.utc)
    native = silver["native_order_lines"].select(
        "dataset_version", "channel", "native_order_id", "native_line_id", "seller_sku"
    )
    native_key = ["dataset_version", "channel", "native_order_id", "native_line_id"]
    # Ambiguous mappings raise an error instead of silently dropping or merging source lines.
    if native.groupBy(*native_key).count().filter("count != 1").limit(1).count():
        raise ValueError("Ambiguous native line identity")
    base = silver["line_lifecycle"].join(native, native_key, "left")
    statuses = {}
    for row in silver["coverage_validity"].collect():
        statuses.setdefault(row["dataset_version"], {})[row["evidence_event_id"]] = row[
            "receipt_status"
        ]
    pairs = line_evidence(base.select(*IDENTITY), silver["observed_evidence"])
    keyless = keyless_evidence(silver["observed_evidence"])
    grouped = (
        base.select(*IDENTITY)
        .join(pairs.select(*KEY, "raw_json"), KEY, "left")
        .groupBy(*IDENTITY)
        .agg(F.collect_list("raw_json").alias("raw_records"))
    )
    inputs = spark.createDataFrame(
        grouped.rdd.map(lambda row: _input_row(row, statuses, keyless)), INPUT_DDL
    ).cache()
    fact = base.join(inputs.select(*KEY, "created_at"), KEY, "left").withColumn(
        "generation_id", F.lit(run_id)
    )
    labels = label_inputs(inputs, cutoff, run_id, computed).cache()
    segments = (
        spark.createDataFrame(inputs.rdd.flatMap(_segment_rows), SEGMENT_DDL)
        .withColumn("generation_id", F.lit(run_id))
        .cache()
    )
    features = (
        sku_features(fact, segments)
        .withColumn("event_timestamp", F.col("created_at"))
        .withColumn("created", F.lit(computed).cast("timestamp"))
        .withColumn("label_version", F.lit(LABEL_VERSION))
        .withColumn("generation_id", F.lit(run_id))
    )
    return dict(
        fact_order_lines=fact,
        label_return_v2=labels,
        feat_sku_return_rate_90d=features,
        label_segments=segments,
        label_inputs=inputs,
    )


def _recompute_feature(target, history_inputs):
    """Independent oracle per the original definition: `label_line` at the target line's order time."""
    timestamp = utc(target["created_at"])
    n = p = 0
    for candidate in history_inputs:
        if candidate["line_id"] == target["line_id"]:
            continue
        result = label_line(json.loads(candidate["input_json"]), timestamp)
        if (
            result.state == "labeled"
            and result.maturity_end < timestamp
            and result.decision_observed_at < timestamp
            and timestamp.date() - timedelta(days=90) <= result.cohort_date < timestamp.date()
        ):
            n += 1
            p += result.label
    return n, p


def validate_gold(
    silver: dict, gold: dict, cutoff, sample_limit: int = 200, feature_sample: int = 50
) -> dict:
    """Count violations; the PIT feature of a sample is recomputed with the pure `label_line`.

    The sample oracle shares no code with the sweep/segment path, so a feature that counts a label not
    yet known at order time (or misses one) shows up as `pit_feature_mismatch`.
    """
    fact, labels, feat = (
        gold[name] for name in ("fact_order_lines", "label_return_v2", "feat_sku_return_rate_90d")
    )
    spark = fact.sparkSession
    distribute_python_packages(spark.sparkContext)
    checks = {}
    checks["fact_line_count_delta"] = abs(fact.count() - silver["line_lifecycle"].count())

    def total(frame):
        """Compare money totals (minor units), even when the dataset is empty."""
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
        | (
            F.col("label").eqNullSafe(F.lit(1))
            & ~F.coalesce(
                F.col("positive_kind").isin("return_and_refund", "refund_only", "return_only"),
                F.lit(False),
            )
        )
        | (~F.col("label").eqNullSafe(F.lit(1)) & F.col("positive_kind").isNotNull())
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
        for frame in (fact, feat, gold["label_segments"]):
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
    checks["feature_rate_mismatch"] = feat.filter(
        F.col("labeled_count_90d").isNull()
        | F.col("positive_count_90d").isNull()
        | (F.col("positive_count_90d") > F.col("labeled_count_90d"))
        | (F.col("positive_count_90d") < 0)
        | ((F.col("labeled_count_90d") == 0) & F.col("return_or_refund_rate_90d").isNotNull())
        | (
            (F.col("labeled_count_90d") > 0)
            & ~F.col("return_or_refund_rate_90d").eqNullSafe(
                F.col("positive_count_90d") / F.col("labeled_count_90d")
            )
        )
    ).count()
    # PIT: a segment only opens after both the decision time and the wait deadline have passed.
    checks["pit_segment_before_known"] = (
        gold["label_segments"]
        .filter(
            F.col("decision_observed_at").isNull()
            | F.col("maturity_end").isNull()
            | (F.col("start_exclusive") < F.col("decision_observed_at"))
            | (F.col("start_exclusive") < F.col("maturity_end"))
        )
        .count()
    )
    # Recompute PIT features on the sample, independently of the segment/sweep code.
    targets = (
        feat.filter(F.col("created_at").isNotNull())
        .withColumn("h", F.xxhash64(*KEY))
        .orderBy("h", *KEY)
        .limit(feature_sample)
        .select(*KEY, "seller_sku", "created_at", "labeled_count_90d", "positive_count_90d")
    )
    history = (
        gold["label_inputs"]
        .join(fact.select(*KEY, "seller_sku"), KEY)
        .select(
            "dataset_version", "seller_sku", F.col("line_id").alias("history_line_id"), "input_json"
        )
    )
    # Group by (dataset, SKU): one SKU's history is loaded ONCE for all of its targets. Grouping per
    # target copies a hot SKU once for each target (this once caused an OOM).
    history = history.join(
        targets.select("dataset_version", "seller_sku").distinct(),
        ["dataset_version", "seller_sku"],
        "left_semi",
    )

    def sku_key(row):
        """Grouping key of a target or history row."""
        return (row["dataset_version"], row["seller_sku"])

    def oracle(group):
        """Group = (SKU key, (targets, history)); count targets whose feature deviates from the original definition."""
        _, (target_rows, history_rows) = group
        inputs = [
            dict(line_id=r["history_line_id"], input_json=r["input_json"])
            for r in history_rows
            if r["input_json"]
        ]
        return sum(
            int(
                _recompute_feature(dict(line_id=t["line_id"], created_at=t["created_at"]), inputs)
                != (t["labeled_count_90d"], t["positive_count_90d"])
            )
            for t in target_rows
        )

    mismatches = (
        (targets.rdd.keyBy(sku_key).cogroup(history.rdd.keyBy(sku_key)).map(oracle).sum())
        if targets.limit(1).count()
        else 0
    )
    checks["pit_feature_mismatch"] = int(mismatches)
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
    return {
        "checks": checks,
        "info": {
            "fact_rows": fact.count(),
            "sampled_labels": len(rows),
            "sampled_features": min(feature_sample, feat.count()),
        },
    }


def write_receipt(path, receipt):
    """Print check results and, if a path is given, write the receipt for later review."""
    if path:
        receipt.setdefault("completed_at", completed_at_utc())
    text = json.dumps(receipt, ensure_ascii=False, sort_keys=True, indent=2)
    print(text)
    if path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text + "\n")
