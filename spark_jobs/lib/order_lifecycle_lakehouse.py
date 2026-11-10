"""Bronze/Silver logic for the v2 source (package C): DataFrames in, DataFrames out, no catalog I/O.

The jobs `bronze/ingest_order_lifecycle.py` and `silver/build_order_lifecycle.py` call these functions to
write Iceberg; the test `tests/spark/check_order_lifecycle_silver.py` calls them directly on a small fixture.

Design (see `docs/data-platform/reference/source_contract_order_lifecycle.md`):
  * Bronze is ONE shared table for all feeds: each JSONL line becomes one row, keeping
    `raw_json` as is. Adding a feed or changing a schema needs no table change.
  * Silver is split by entity. An event's identity is
    (dataset_version, producer, source_epoch, source_event_id): repeats with the same content
    are dropped; the same id with different content is a conflict and is excluded from Silver.
  * The highest revision of an entity replaces earlier revisions (a correction replaces,
    it does not add). History is kept in the listing/return/inbound tables.
  * `as_of` filters on the time the source observed the record (`source_observed_at`), to rebuild
    exactly what was known at a point in time.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from spark_jobs.lib.data_contracts.registry import SUPPORTED_PARSERS, Registry
from spark_jobs.lib.data_contracts.schema import required_json_paths

OPERATIONAL_FEEDS = (
    "seller_oms.line_commitments",
    "seller_oms.fulfillment_allocations",
    "carrier.delivery_events",
    "seller_postsales.return_events",
    "seller_postsales.refund_events",
    "seller_catalog.listing_versions",
    "seller_wms.stock_operations",
    "seller_wms.inbound_events",
    "seller_wms.stock_snapshots",
)
DIAGNOSTIC_FEEDS = ("fixture.probes",)
PATH_PATTERN = r"dataset_version=([^/]+)/producer=([^/]+)/feed=([^/]+)/export_date=([^/]+)/"
TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"
EVENT_KEY = ["dataset_version", "producer", "source_epoch", "source_event_id"]
ENTITY_KEY = ["dataset_version", "producer", "source_epoch", "feed", "entity_id"]
ALLOCATION_MONEY = "array<struct<allocation_id:string,amount_minor:bigint,evidence_ref:string>>"


def ts(column: str):
    """Contract-format UTC string (microseconds, Z suffix) -> timestamp."""
    return F.to_timestamp(F.col(column), TS_FORMAT)


def fact_sha256_col(column: str = "raw_json"):
    """Hash of the business content: drops `recorded_at` (machine run time, not source truth).

    Source JSON is canonical (sorted keys, no whitespace), so `recorded_at` is never
    the last key of the envelope and the regex also removes the trailing comma.
    """
    return F.sha2(F.regexp_replace(F.col(column), r'"recorded_at":"[^"]*",?', ""), 256)


def read_source_manifest(spark: SparkSession, root: str) -> dict:
    """Read the manifest URI with the Spark driver; local paths keep the pathlib read."""
    root = str(root)
    if re.match(r"^[A-Za-z][A-Za-z0-9+.-]*:", root):
        rows = spark.read.text(f"{root.rstrip('/')}/manifest.json", wholetext=True).collect()
        if len(rows) != 1:
            raise ValueError("Expected exactly one manifest.json")
        return json.loads(rows[0]["value"])
    return json.loads((Path(root) / "manifest.json").read_text())


def bronze_from_root(spark: SparkSession, root: str, run_id: str) -> DataFrame:
    """Read every v2 source file under `root` into one Bronze DataFrame, one row per JSONL line."""
    text = spark.read.text(
        f"{root.rstrip('/')}/dataset_version=*/producer=*/feed=*/export_date=*/*.jsonl"
    )
    df = text.select(F.col("value").alias("raw_json"), F.input_file_name().alias("source_file"))
    for index, name in enumerate(("dataset_version", "producer", "feed", "export_date"), 1):
        df = df.withColumn(name, F.regexp_extract("source_file", PATH_PATTERN, index))
    df = (
        df.filter(~F.col("feed").isin(*DIAGNOSTIC_FEEDS))
        .withColumn("export_date", F.to_date("export_date"))
        .withColumn("ingestion_boundary", F.lit("file"))
    )
    manifest = read_source_manifest(spark, root)
    fallback = F.lit(None).cast("string")
    for channel, source in manifest["native_sources"].items():
        fallback = F.when(
            F.col("feed") == channel + ".orders", F.lit(source["schema_id"])
        ).otherwise(fallback)
    return (
        with_envelope(df)
        .withColumn("contract_schema_id", F.coalesce(F.col("schema_id"), fallback))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("run_id", F.lit(run_id))
    )


def bronze_from_jdbc(
    rows: DataFrame,
    run_id: str,
    source_label: str,
    snapshot_date: str,
    contract_schema_id: str = "sim-native-v2/walmart/1",
) -> DataFrame:
    """Walmart rows read via JDBC -> the same Bronze shape; `rows` has dataset_version, purchase_order_id, raw_json.

    `raw_json` is the JSON string PostgreSQL returns (kept as is, whitespace included). `source_label`
    is the JDBC URL + table, with no password. `export_date` is the snapshot date the manifest declares.
    """
    df = rows.select(
        "raw_json",
        F.lit(source_label).alias("source_file"),
        "dataset_version",
        F.lit("sim.native.walmart").alias("producer"),
        F.lit("walmart.orders").alias("feed"),
        F.to_date(F.lit(snapshot_date)).alias("export_date"),
        F.lit("jdbc").alias("ingestion_boundary"),
    )
    return (
        with_envelope(df)
        .withColumn("contract_schema_id", F.coalesce("schema_id", F.lit(contract_schema_id)))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("run_id", F.lit(run_id))
    )


def snapshot_scope_diff(received: list[str], declared: list[str]) -> dict:
    """Compare Order ids received via JDBC with the manifest's `snapshot_scope.order_ids`."""
    seen, duplicate = set(), set()
    for order_id in received:
        (duplicate if order_id in seen else seen).add(order_id)
    return {
        "missing": sorted(set(declared) - seen),
        "extra": sorted(seen - set(declared)),
        "duplicate": sorted(duplicate),
    }


def with_envelope(df: DataFrame) -> DataFrame:
    """(Re)compute the envelope and hash columns from `raw_json`; path columns are kept."""
    envelope = lambda key: F.get_json_object("raw_json", f"$.{key}")  # noqa: E731
    return (
        df.withColumn("source_event_id", envelope("source_event_id"))
        .withColumn("source_epoch", envelope("source_epoch"))
        .withColumn("entity_id", envelope("entity_id"))
        .withColumn("entity_revision", envelope("entity_revision").cast("int"))
        .withColumn("source_sequence", envelope("source_sequence").cast("bigint"))
        .withColumn("operation", envelope("operation"))
        .withColumn("corrects_source_event_id", envelope("corrects_source_event_id"))
        .withColumn("schema_id", envelope("schema_id"))
        .withColumn("effective_at", envelope("effective_at"))
        .withColumn("source_observed_at", envelope("source_observed_at"))
        .withColumn("record_sha256", F.sha2("raw_json", 256))
        .withColumn("fact_sha256", fact_sha256_col())
    )


def _minor(amount, scale: int = 2):
    """Decimal money string ("330.00") -> integer in minor units (33000); null stays null."""
    return (amount.cast("decimal(20,4)") * (10**scale)).cast("bigint")


def _items(path: str, ddl: str):
    """Item array nested in the Channel's original JSON, exploded into one row each."""
    return F.explode_outer(F.from_json(F.get_json_object("raw_json", path), ddl)).alias("item")


def native_order_lines(known: DataFrame, include_record_hash: bool = False) -> DataFrame:
    """Native Orders for 5 Channels (snapshot at Order creation) -> one row per line, shared columns.

    Each Channel has its own shape (Amazon has two schemas). `net_minor` is the line's goods value after
    discounts; `gross_minor` is null when the Channel sends no list price (Walmart). Orders that fail to
    parse still yield a row with a null `native_order_id` so reconciliation can count them.
    """
    feed = lambda parser: known.filter(F.col("contract_parser") == parser)  # noqa: E731
    money = "struct<CurrencyCode:string,Amount:string>"
    amazon_1 = (
        feed("amazon/1")
        .select(
            "dataset_version",
            "record_sha256",
            F.get_json_object("raw_json", "$.AmazonOrderId").alias("native_order_id"),
            _items(
                "$.OrderItems",
                f"array<struct<OrderItemId:string,SellerSKU:string,QuantityOrdered:int,"
                f"ItemPrice:{money},PromotionDiscount:{money}>>",
            ),
        )
        .select(
            "dataset_version",
            "record_sha256",
            F.lit("amazon").alias("channel"),
            F.lit("amazon/1").alias("native_schema"),
            "native_order_id",
            F.col("item.OrderItemId").alias("native_line_id"),
            F.col("item.SellerSKU").alias("seller_sku"),
            F.col("item.QuantityOrdered").alias("quantity"),
            F.col("item.ItemPrice.CurrencyCode").alias("currency"),
            _minor(F.col("item.ItemPrice.Amount")).alias("gross_minor"),
            (
                _minor(F.col("item.ItemPrice.Amount"))
                - _minor(F.col("item.PromotionDiscount.Amount"))
            ).alias("net_minor"),
        )
    )
    money2 = "struct<currencyCode:string,amount:string>"
    amazon_2 = (
        feed("amazon/2")
        .select(
            "dataset_version",
            "record_sha256",
            F.get_json_object("raw_json", "$.orderId").alias("native_order_id"),
            _items(
                "$.orderItems",
                f"array<struct<orderItemId:string,sellerSku:string,quantityOrdered:int,"
                f"price:{money2},discount:{money2}>>",
            ),
        )
        .select(
            "dataset_version",
            "record_sha256",
            F.lit("amazon").alias("channel"),
            F.lit("amazon/2").alias("native_schema"),
            "native_order_id",
            F.col("item.orderItemId").alias("native_line_id"),
            F.col("item.sellerSku").alias("seller_sku"),
            F.col("item.quantityOrdered").alias("quantity"),
            F.col("item.price.currencyCode").alias("currency"),
            _minor(F.col("item.price.amount")).alias("gross_minor"),
            (_minor(F.col("item.price.amount")) - _minor(F.col("item.discount.amount"))).alias(
                "net_minor"
            ),
        )
    )
    charge = "struct<chargeType:string,chargeAmount:struct<currency:string,amount:string>>"
    walmart = (
        feed("walmart/1")
        .select(
            "dataset_version",
            "record_sha256",
            F.get_json_object("raw_json", "$.purchase_order_id").alias("native_order_id"),
            _items(
                "$.raw_payload.orderLines.orderLine",
                f"array<struct<lineNumber:string,item:struct<sku:string>,"
                f"orderLineQuantity:struct<amount:string>,charges:struct<charge:array<{charge}>>>>",
            ),
        )
        .withColumn(
            "product", F.filter("item.charges.charge", lambda c: c["chargeType"] == "PRODUCT")
        )
        .select(
            "dataset_version",
            "record_sha256",
            F.lit("walmart").alias("channel"),
            F.lit("walmart/1").alias("native_schema"),
            "native_order_id",
            F.col("item.lineNumber").alias("native_line_id"),
            F.col("item.item.sku").alias("seller_sku"),
            F.col("item.orderLineQuantity.amount").cast("int").alias("quantity"),
            F.col("product")[0]["chargeAmount"]["currency"].alias("currency"),
            F.lit(None).cast("bigint").alias("gross_minor"),
            F.aggregate(
                "product",
                F.lit(0).cast("bigint"),
                lambda acc, c: acc + _minor(c["chargeAmount"]["amount"]),
            ).alias("net_minor"),
        )
    )
    shopify = (
        feed("shopify/1")
        .select(
            "dataset_version",
            "record_sha256",
            F.get_json_object("raw_json", "$.order.id").alias("native_order_id"),
            F.get_json_object("raw_json", "$.order.currency").alias("currency"),
            _items(
                "$.order.line_items",
                "array<struct<id:string,sku:string,quantity:int,"
                "price:string,total_discount:string>>",
            ),
        )
        .select(
            "dataset_version",
            "record_sha256",
            F.lit("shopify").alias("channel"),
            F.lit("shopify/1").alias("native_schema"),
            "native_order_id",
            F.col("item.id").alias("native_line_id"),
            F.col("item.sku").alias("seller_sku"),
            F.col("item.quantity").alias("quantity"),
            "currency",
            (_minor(F.col("item.price")) * F.col("item.quantity")).alias("gross_minor"),
            (
                _minor(F.col("item.price")) * F.col("item.quantity")
                - _minor(F.col("item.total_discount"))
            ).alias("net_minor"),
        )
    )
    tiktok = (
        feed("tiktok_shop/1")
        .select(
            "dataset_version",
            "record_sha256",
            F.get_json_object("raw_json", "$.data.orders[0].id").alias("native_order_id"),
            F.get_json_object("raw_json", "$.data.orders[0].currency").alias("currency"),
            _items(
                "$.data.orders[0].line_items",
                "array<struct<id:string,seller_sku:string,quantity:int,"
                "original_price:string,sale_price:string>>",
            ),
        )
        .select(
            "dataset_version",
            "record_sha256",
            F.lit("tiktok_shop").alias("channel"),
            F.lit("tiktok_shop/1").alias("native_schema"),
            "native_order_id",
            F.col("item.id").alias("native_line_id"),
            F.col("item.seller_sku").alias("seller_sku"),
            F.col("item.quantity").alias("quantity"),
            "currency",
            (_minor(F.col("item.original_price")) * F.col("item.quantity")).alias("gross_minor"),
            (_minor(F.col("item.sale_price")) * F.col("item.quantity")).alias("net_minor"),
        )
    )
    shopee = (
        feed("shopee/1")
        .select(
            "dataset_version",
            "record_sha256",
            F.get_json_object("raw_json", "$.response.order_list[0].order_sn").alias(
                "native_order_id"
            ),
            F.get_json_object("raw_json", "$.response.order_list[0].currency").alias("currency"),
            _items(
                "$.response.order_list[0].item_list",
                "array<struct<order_item_id:string,model_sku:string,"
                "model_quantity_purchased:int,model_original_price:string,model_discounted_price:string>>",
            ),
        )
        .select(
            "dataset_version",
            "record_sha256",
            F.lit("shopee").alias("channel"),
            F.lit("shopee/1").alias("native_schema"),
            "native_order_id",
            F.col("item.order_item_id").alias("native_line_id"),
            F.col("item.model_sku").alias("seller_sku"),
            F.col("item.model_quantity_purchased").alias("quantity"),
            "currency",
            (
                _minor(F.col("item.model_original_price")) * F.col("item.model_quantity_purchased")
            ).alias("gross_minor"),
            (
                _minor(F.col("item.model_discounted_price"))
                * F.col("item.model_quantity_purchased")
            ).alias("net_minor"),
        )
    )
    out = amazon_1
    for part in (amazon_2, walmart, shopify, tiktok, shopee):
        out = out.unionByName(part)
    return out if include_record_hash else out.drop("record_sha256")


def _coverage_interval_matches(cov: DataFrame, rows: DataFrame, keys: list[str]) -> DataFrame:
    """Join on the UTC day before checking the interval, avoiding trying every (week, feed row).

    Back-to-back weeks have only one envelope/day/feed; each row joins one envelope.
    Fixtures with long intervals stay compatible by exploding days, without multiplying the whole feed.
    """
    days = F.when(
        F.col("interval_end") > F.col("interval_start"),
        F.sequence(
            F.to_date("interval_start"),
            F.to_date(F.col("interval_end") - F.expr("INTERVAL 1 MICROSECOND")),
        ),
    )
    cov = cov.withColumn("coverage_day", F.explode(days))
    rows = rows.withColumn("coverage_day", F.to_date("effective_ts"))
    return cov.join(rows, keys + ["coverage_day"]).filter(
        (F.col("observed_ts") < F.col("coverage_observed_at"))
        & (F.col("effective_ts") >= F.col("interval_start"))
        & (F.col("effective_ts") < F.col("interval_end"))
    )


def coverage_receipts(known: DataFrame) -> DataFrame:
    """Apply producer coverage to exactly the set of received source rows (like `coverage_receipt` in the generator).

    For each `complete` envelope (latest revision): the rows of that feed, with the same dataset/producer/
    epoch, effective within the interval and observed BEFORE the envelope must match the declared sequence set, row count, fall within the event
    range, and the hash of the whole row set (canonical JSON sorted by id) must equal `record_set_sha256`.
    `unavailable` envelopes (scope without source authority) are not checked, only counted.
    """
    env = lambda key: F.get_json_object("raw_json", f"$.{key}")  # noqa: E731
    window = Window.partitionBy("coverage_id").orderBy(F.col("coverage_revision").desc())
    cov = (
        known.filter(F.col("feed") == "source.coverage_envelopes")
        .select(
            env("coverage_id").alias("coverage_id"),
            env("revision").cast("int").alias("coverage_revision"),
            env("feed").alias("covered_feed"),
            env("producer").alias("covered_producer"),
            env("dataset_version").alias("covered_dataset"),
            env("source_epoch").alias("covered_epoch"),
            env("status").alias("coverage_status"),
            env("source_record_count").cast("bigint").alias("declared_rows"),
            env("record_set_sha256").alias("declared_sha256"),
            F.from_json(
                env("source_sequence_ranges"),
                "array<struct<partition:string,first:bigint,last:bigint>>",
            ).alias("ranges"),
            F.from_json(env("expected_partitions"), "array<string>").alias("expected_partitions"),
            F.from_json(env("completed_partitions"), "array<string>").alias("completed_partitions"),
            F.to_timestamp(env("event_interval.start_inclusive"), TS_FORMAT).alias(
                "interval_start"
            ),
            F.to_timestamp(env("event_interval.end_exclusive"), TS_FORMAT).alias("interval_end"),
            ts("source_observed_at").alias("coverage_observed_at"),
        )
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )
    complete = cov.filter(F.col("coverage_status") == "complete")
    expected = complete.select("coverage_id", F.explode("ranges").alias("r")).select(
        "coverage_id",
        F.explode(F.sequence("r.first", "r.last")).alias("seq"),
        F.col("r.partition").alias("partition"),
    )
    rows = known.filter(F.col("feed").isin(*OPERATIONAL_FEEDS)).select(
        F.col("feed").alias("covered_feed"),
        F.col("producer").alias("covered_producer"),
        F.col("dataset_version").alias("covered_dataset"),
        F.col("source_epoch").alias("covered_epoch"),
        "source_event_id",
        "raw_json",
        "source_sequence",
        F.get_json_object("raw_json", "$.source_partition").alias("partition"),
        ts("effective_at").alias("effective_ts"),
        ts("source_observed_at").alias("observed_ts"),
    )
    matched = _coverage_interval_matches(
        complete, rows, ["covered_feed", "covered_producer", "covered_dataset", "covered_epoch"]
    )
    actual = matched.select("coverage_id", F.col("source_sequence").alias("seq"), "partition")
    missing = (
        expected.join(actual, ["coverage_id", "seq", "partition"], "left_anti")
        .groupBy("coverage_id")
        .agg(F.count("*").alias("missing_seq"))
    )
    unexpected = (
        actual.join(expected, ["coverage_id", "seq", "partition"], "left_anti")
        .groupBy("coverage_id")
        .agg(F.count("*").alias("unexpected_seq"))
    )
    agg = matched.groupBy("coverage_id").agg(
        F.count("*").alias("received_rows"),
        F.countDistinct("source_event_id").alias("distinct_ids"),
        F.sum(
            F.when(
                (F.col("effective_ts") < F.col("interval_start"))
                | (F.col("effective_ts") >= F.col("interval_end")),
                1,
            ).otherwise(0)
        ).alias("outside_interval"),
        F.sha2(
            F.concat(
                F.lit("["),
                F.array_join(
                    F.transform(
                        F.array_sort(F.collect_list(F.struct("source_event_id", "raw_json"))),
                        lambda x: x["raw_json"],
                    ),
                    ",",
                ),
                F.lit("]"),
            ),
            256,
        ).alias("received_sha256"),
    )
    joined = (
        complete.join(agg, "coverage_id", "left")
        .join(missing, "coverage_id", "left")
        .join(unexpected, "coverage_id", "left")
        .fillna(
            0,
            subset=[
                "received_rows",
                "distinct_ids",
                "outside_interval",
                "missing_seq",
                "unexpected_seq",
            ],
        )
    )
    reason = lambda cond, name: F.when(cond, F.lit(name))  # noqa: E731
    reasons = F.array_sort(
        F.filter(
            F.array(
                reason(F.col("missing_seq") > 0, "missing_sequence"),
                reason(F.col("unexpected_seq") > 0, "unexpected_sequence"),
                reason(F.col("distinct_ids") < F.col("received_rows"), "duplicate_source_identity"),
                reason(F.col("outside_interval") > 0, "outside_event_interval"),
                reason(F.col("received_rows") != F.col("declared_rows"), "record_count_mismatch"),
                reason(
                    ~F.coalesce(
                        F.coalesce(F.col("received_sha256"), F.sha2(F.lit("[]"), 256))
                        == F.col("declared_sha256"),
                        F.lit(False),
                    ),
                    "record_set_digest_mismatch",
                ),
                reason(
                    F.array_sort("expected_partitions") != F.array_sort("completed_partitions"),
                    "missing_partition",
                ),
            ),
            lambda x: x.isNotNull(),
        )
    )
    checked = joined.withColumn("reasons", reasons).select(
        "coverage_id",
        "coverage_revision",
        "covered_feed",
        "covered_producer",
        "coverage_status",
        "declared_rows",
        "received_rows",
        "missing_seq",
        "unexpected_seq",
        "reasons",
        F.when(F.size("reasons") == 0, "complete").otherwise("partial").alias("receipt_status"),
        "coverage_observed_at",
    )
    unavailable = cov.filter(F.col("coverage_status") != "complete").select(
        "coverage_id",
        "coverage_revision",
        "covered_feed",
        "covered_producer",
        "coverage_status",
        F.col("declared_rows"),
        F.lit(0).cast("bigint").alias("received_rows"),
        F.lit(0).cast("bigint").alias("missing_seq"),
        F.lit(0).cast("bigint").alias("unexpected_seq"),
        F.array().cast("array<string>").alias("reasons"),
        F.col("coverage_status").alias("receipt_status"),
        "coverage_observed_at",
    )
    return checked.unionByName(unavailable)


# Feeds the v2 label policy reads (matches `return_label.FEED_KINDS`); coverage only spans these feeds.
LABEL_EVIDENCE_FEEDS = (
    "seller_oms.line_commitments",
    "seller_oms.fulfillment_allocations",
    "carrier.delivery_events",
    "seller_postsales.return_events",
    "seller_postsales.refund_events",
)


def observed_evidence(known: DataFrame) -> DataFrame:
    """Observation history for v2 labels: EVERY revision, dropping only identical retries (same `record_sha256`).

    Unlike `events` (conflicts dropped, only the latest revision kept for business tables): the label policy must
    see both conflicts and corrections to return `unknown` / apply a correction at the time it became known. The key
    columns `ev_*` only let Gold group evidence per line without replicating the whole dataset for each line:
    - `ev_line_id` / `ev_line_ids`: scope includes a line;
    - `ev_order_id` / `ev_order_ids`: scope only reaches the Order (e.g. a Refund with no known line);
    - no key (coverage, Channel/account-level scope): sent to every line in the same dataset;
    - `corrects_source_event_id`: the correction is also sent to the line holding the corrected record.
    Exact filtering is still done by `return_label.visible_events`; these keys are only a wider candidate set.
    """
    rows = known.filter(
        F.col("feed").isin(
            *LABEL_EVIDENCE_FEEDS, "source.coverage_envelopes", "source.uncertainties"
        )
    )
    rows = rows.dropDuplicates(["dataset_version", "record_sha256"])
    raw = lambda path: F.get_json_object("raw_json", path)  # noqa: E731
    is_uncertainty = F.col("feed") == "source.uncertainties"
    is_coverage = F.col("feed") == "source.coverage_envelopes"
    # Uncertainty uses the envelope scope; operational feeds build scope from the payload, falling back to affected_scope when empty.
    top_scope = F.coalesce(raw("$.scope"), raw("$.affected_scope"))
    in_scope = lambda key: F.get_json_object(top_scope, f"$.{key}")  # noqa: E731

    def in_affected(key):
        """Get an affected-scope attribute from the envelope or the payload."""
        return F.coalesce(raw(f"$.affected_scope.{key}"), raw(f"$.payload.affected_scope.{key}"))

    def pick(key):
        """Pick a scope attribute by record type: uncertainty, coverage or event."""
        return (
            F.when(is_uncertainty, in_scope(key))
            .when(is_coverage, F.lit(None))
            .otherwise(F.coalesce(raw(f"$.payload.{key}"), in_affected(key)))
        )

    def plural(key):
        """Read a string-array scope attribute by record type."""
        return (
            F.when(is_uncertainty, F.from_json(in_scope(key), "array<string>"))
            .when(is_coverage, F.lit(None).cast("array<string>"))
            .otherwise(F.from_json(in_affected(key), "array<string>"))
        )

    event_id = (
        F.when(is_uncertainty, F.concat_ws(":", raw("$.uncertainty_id"), raw("$.revision")))
        .when(is_coverage, F.concat_ws(":", raw("$.coverage_id"), raw("$.revision")))
        .otherwise(F.coalesce(raw("$.payload.source_event_id"), raw("$.source_event_id")))
    )
    return rows.select(
        "dataset_version",
        "feed",
        "producer",
        "record_sha256",
        "raw_json",
        F.col("source_epoch"),
        ts("source_observed_at").alias("source_observed_at"),
        event_id.alias("evidence_event_id"),
        F.coalesce(
            raw("$.payload.corrects_source_event_id"), raw("$.corrects_source_event_id")
        ).alias("corrects_source_event_id"),
        pick("line_id").alias("ev_line_id"),
        plural("line_ids").alias("ev_line_ids"),
        pick("native_order_id").alias("ev_order_id"),
        plural("native_order_ids").alias("ev_order_ids"),
    )


def coverage_validity(evidence: DataFrame) -> DataFrame:
    """Check each coverage revision ONCE (same rule as `return_label.source_line`), instead of repeating per line.

    A `complete` envelope is valid when the row set of exactly that feed/producer/epoch/dataset, observed BEFORE
    the envelope and effective within the interval, matches the row count, the declared (partition, sequence) set, the canonical hash of the whole set (sorted by
    `source_event_id`), every `effective_at` falls within the event range; all partitions present; capability,
    profile and both range ends present. Result: `complete` / `partial` per `evidence_event_id`.
    """
    env = lambda key: F.get_json_object("raw_json", f"$.{key}")  # noqa: E731
    cov = evidence.filter(F.col("feed") == "source.coverage_envelopes").select(
        "evidence_event_id",
        "dataset_version",
        env("feed").alias("covered_feed"),
        env("producer").alias("covered_producer"),
        env("source_epoch").alias("covered_epoch"),
        env("status").alias("coverage_status"),
        env("source_record_count").cast("bigint").alias("declared_rows"),
        env("record_set_sha256").alias("declared_sha256"),
        env("capability").alias("capability"),
        env("profile_id").alias("profile_id"),
        F.from_json(
            env("source_sequence_ranges"),
            "array<struct<partition:string,first:bigint,last:bigint>>",
        ).alias("ranges"),
        F.from_json(env("expected_partitions"), "array<string>").alias("expected_partitions"),
        F.from_json(env("completed_partitions"), "array<string>").alias("completed_partitions"),
        F.to_timestamp(env("event_interval.start_inclusive"), TS_FORMAT).alias("interval_start"),
        F.to_timestamp(env("event_interval.end_exclusive"), TS_FORMAT).alias("interval_end"),
        F.col("source_observed_at").alias("coverage_observed_at"),
    )
    rows = evidence.filter(F.col("feed").isin(*LABEL_EVIDENCE_FEEDS)).select(
        "dataset_version",
        F.col("feed").alias("covered_feed"),
        F.col("producer").alias("covered_producer"),
        F.col("source_epoch").alias("covered_epoch"),
        F.get_json_object("raw_json", "$.source_event_id").alias("sid"),
        "raw_json",
        F.get_json_object("raw_json", "$.source_partition").alias("partition"),
        F.get_json_object("raw_json", "$.source_sequence").cast("bigint").alias("seq"),
        F.to_timestamp(F.get_json_object("raw_json", "$.effective_at"), TS_FORMAT).alias(
            "effective_ts"
        ),
        F.col("source_observed_at").alias("observed_ts"),
    )
    matched = _coverage_interval_matches(
        cov,
        rows.filter(F.col("sid").isNotNull()),
        ["dataset_version", "covered_feed", "covered_producer", "covered_epoch"],
    )
    agg = matched.groupBy("evidence_event_id").agg(
        F.count("*").alias("received_rows"),
        F.sum(
            F.when(
                F.col("effective_ts").isNull()
                | (F.col("effective_ts") < F.col("interval_start"))
                | (F.col("effective_ts") >= F.col("interval_end")),
                1,
            ).otherwise(0)
        ).alias("outside_interval"),
        F.array_sort(F.collect_set(F.struct(F.col("partition"), F.col("seq")))).alias("actual_seq"),
        F.sha2(
            F.concat(
                F.lit("["),
                F.array_join(
                    F.transform(
                        F.array_sort(F.collect_list(F.struct("sid", "raw_json"))),
                        lambda x: x["raw_json"],
                    ),
                    ",",
                ),
                F.lit("]"),
            ),
            256,
        ).alias("received_sha256"),
    )
    declared = cov.select("evidence_event_id", F.explode("ranges").alias("r")).select(
        "evidence_event_id",
        F.col("r.partition").alias("partition"),
        F.explode(F.sequence("r.first", "r.last")).alias("seq"),
    )
    declared = declared.groupBy("evidence_event_id").agg(
        F.array_sort(F.collect_set(F.struct("partition", "seq"))).alias("declared_seq")
    )
    joined = cov.join(agg, "evidence_event_id", "left").join(declared, "evidence_event_id", "left")
    empty_seq = F.array().cast("array<struct<partition:string,seq:bigint>>")
    valid = (
        (F.col("coverage_status") == "complete")
        & (F.coalesce("received_sha256", F.sha2(F.lit("[]"), 256)) == F.col("declared_sha256"))
        & (F.coalesce("received_rows", F.lit(0)) == F.col("declared_rows"))
        # collect_set dedups by hash while aggregating; array_distinct on a struct array is quadratic
        # (1.816 s / 87% of Silver at e2e medium, see docs/data-platform/reference/silver_order_lifecycle_performance.md).
        & (F.coalesce("declared_seq", empty_seq) == F.coalesce("actual_seq", empty_seq))
        & (F.array_sort("expected_partitions") == F.array_sort("completed_partitions"))
        & (F.coalesce(F.length("capability"), F.lit(0)) > 0)
        & (F.coalesce(F.length("profile_id"), F.lit(0)) > 0)
        & F.col("interval_start").isNotNull()
        & F.col("interval_end").isNotNull()
        & (F.coalesce("outside_interval", F.lit(0)) == 0)
    )
    return joined.select(
        "dataset_version",
        "evidence_event_id",
        F.when(F.coalesce(valid, F.lit(False)), "complete")
        .otherwise("partial")
        .alias("receipt_status"),
    )


def _known(df: DataFrame, as_of: str | None) -> DataFrame:
    """Keep only source rows observed BEFORE `as_of` (exclusive)."""
    if as_of is None:
        return df
    # Native Order snapshots (Channel JSONL, Walmart JDBC) have no `source_observed_at` in the envelope:
    # treat them as known at the END of the export day (export_date + 1 day): conservative, never earlier than
    # reality. Previously nulls were filtered out ⇒ building Silver with as_of dropped every native row.
    known_at = F.coalesce(ts("source_observed_at"), F.to_timestamp(F.date_add("export_date", 1)))
    return df.filter(known_at < F.to_timestamp(F.lit(as_of), TS_FORMAT))


def _dedup(ops: DataFrame) -> tuple[DataFrame, dict]:
    """Drop same-content repeats; split out conflicts (same id with different content, or duplicate revision)."""
    by_event = Window.partitionBy(*EVENT_KEY)
    ranked = ops.withColumn(
        "facts", F.size(F.collect_set("fact_sha256").over(by_event))
    ).withColumn("rn", F.row_number().over(by_event.orderBy("source_file", "record_sha256")))
    unique = ranked.filter(F.col("rn") == 1).drop("rn")
    by_revision = Window.partitionBy(*ENTITY_KEY, "entity_revision")
    unique = unique.withColumn(
        "revision_ids", F.size(F.collect_set("source_event_id").over(by_revision))
    )
    conflicts = unique.filter((F.col("facts") > 1) | (F.col("revision_ids") > 1))
    events = unique.filter((F.col("facts") == 1) & (F.col("revision_ids") == 1)).drop(
        "facts", "revision_ids"
    )
    stats = {
        "input_rows": ops.count(),
        "duplicate_rows": ops.count() - unique.count(),
        "conflict_event_ids": sorted(
            r[0] for r in conflicts.select("source_event_id").distinct().collect()
        ),
    }
    return events, stats


def _latest(df: DataFrame) -> DataFrame:
    """Highest revision of each entity; a correction replaces the corrected record."""
    window = Window.partitionBy(*ENTITY_KEY).orderBy(F.col("entity_revision").desc())
    return df.withColumn("rn", F.row_number().over(window)).filter(F.col("rn") == 1).drop("rn")


def _p(name: str, kind: str = "string"):
    """One field in the envelope payload."""
    value = F.get_json_object("raw_json", f"$.payload.{name}")
    return (value.cast(kind) if kind != "string" else value).alias(name)


def _envelope_cols():
    """Shared envelope columns for the business Silver tables."""
    return [
        "dataset_version",
        "source_event_id",
        "entity_revision",
        "operation",
        ts("effective_at").alias("effective_at"),
        ts("source_observed_at").alias("source_observed_at"),
    ]


def contract_gate(bronze: DataFrame, registry: Registry) -> tuple:
    """Quarantine before parsing with Spark expressions, no UDF, and never fail because of quarantine.

    For arrays only the first item is checked; empty arrays valid under JSON Schema skip the item check.
    Nullable fields may be null. The Spark gate checks required; Python also checks types.
    """
    parser = F.lit(None).cast("string")
    registered = F.lit(False)
    missing = F.array().cast("array<string>")
    for sid, entry in registry.entries.items():
        matches = F.col("contract_schema_id") == sid
        registered = registered | F.coalesce(matches, F.lit(False))
        parser = F.when(matches, F.lit(entry["parser"]).cast("string")).otherwise(parser)
        paths = []
        for path in required_json_paths(entry["schema"]):
            absent = F.get_json_object("raw_json", path).isNull()
            # JSON Schema items does not require non-empty arrays; skip checks under empty arrays.
            for match in re.finditer(r"\[0\]", path):
                prefix = path[: match.start()]
                absent = absent & ~F.coalesce(
                    F.get_json_object("raw_json", prefix) == "[]", F.lit(False)
                )
            paths.append(F.when(absent, F.lit(path)))
        required = F.filter(F.array(*paths).cast("array<string>"), lambda x: x.isNotNull())
        missing = F.when(matches, required).otherwise(missing)
    checked = (
        bronze.withColumn("contract_parser", parser)
        .withColumn("detail", F.concat_ws(", ", missing))
        .withColumn(
            "reason",
            F.when(~registered, "unregistered_schema")
            .when(
                ~F.coalesce(F.col("contract_parser").isin(*SUPPORTED_PARSERS), F.lit(False)),
                "no_parser",
            )
            .when(F.length("detail") > 0, "contract_violation"),
        )
    )
    return checked.filter(F.col("reason").isNull()).drop("reason", "detail"), checked.filter(
        F.col("reason").isNotNull()
    )


def _native_order_identity(frame: DataFrame, registry: Registry) -> DataFrame:
    """Get the Order id from the metadata of exactly the registered schema, without guessing the payload.

    An unknown schema or missing identity_path gives a null Order id; reconciliation excludes the whole Channel scope
    and reports the number of records with unknown Order id, without calling it an Order count.
    """
    identity = F.lit(None).cast("string")
    for sid, entry in registry.entries.items():
        path = entry.get("identity_path")
        if path is not None:
            identity = F.when(
                F.col("contract_schema_id") == sid, F.get_json_object("raw_json", path)
            ).otherwise(identity)
    return frame.select(
        "dataset_version",
        F.regexp_replace("feed", r"\.orders$", "").alias("channel"),
        identity.alias("native_order_id"),
    )


def _feed(df: DataFrame, name: str) -> DataFrame:
    """Rows of exactly one feed."""
    return df.filter(F.col("feed") == name)


def _json(field: str):
    """A field at the root of `raw_json` (auxiliary records such as coverage/uncertainty have no `payload`)."""
    return F.get_json_object("raw_json", f"$.{field}")


def _newest_revision(df: DataFrame, key: str) -> DataFrame:
    """Highest revision per single key (auxiliary records lack the ENTITY_KEY of business feeds)."""
    window = Window.partitionBy(key).orderBy(F.col("revision").desc())
    return df.withColumn("rn", F.row_number().over(window)).filter(F.col("rn") == 1).drop("rn")


def _line_commitments(events: DataFrame) -> DataFrame:
    """OMS sales commitments per line; adds `headline_product_net_minor` = ordered net − cancelled net."""
    return (
        _latest(_feed(events, "seller_oms.line_commitments"))
        .select(
            _p("line_id"),
            _p("channel"),
            _p("account_id"),
            _p("native_order_id"),
            _p("native_line_id"),
            _p("internal_sku_id"),
            _p("currency"),
            _p("minor_unit_scale", "int"),
            _p("ordered_original_qty", "int"),
            _p("ordered_basis_qty", "int"),
            _p("canceled_unshipped_qty", "int"),
            _p("ordered_gross_minor", "bigint"),
            _p("ordered_discount_minor", "bigint"),
            _p("ordered_product_net_minor", "bigint"),
            _p("canceled_product_net_minor", "bigint"),
            _p("fulfillment_terminal", "boolean"),
            _p("listing_binding_id"),
            _p("sale_mapping_version"),
            _p("price_basis_revision"),
            *_envelope_cols(),
        )
        .withColumn(
            "headline_product_net_minor",
            F.col("ordered_product_net_minor") - F.col("canceled_product_net_minor"),
        )
    )


def _allocations(events: DataFrame) -> DataFrame:
    """Goods allocation for a line (latest revision), with the list of physical units."""
    return _latest(_feed(events, "seller_oms.fulfillment_allocations")).select(
        _p("allocation_id"),
        _p("line_id"),
        _p("internal_sku_id"),
        _p("quantity", "int"),
        _p("allocation_state"),
        _p("shipment_id"),
        _p("bound_pool_id"),
        _p("bound_location_id"),
        F.from_json(_json("payload.physical_unit_ids"), "array<string>").alias("physical_unit_ids"),
        *_envelope_cols(),
    )


def _deliveries(events: DataFrame) -> DataFrame:
    """Carrier delivery events (latest revision): actual delivery time and delivered units."""
    return _latest(_feed(events, "carrier.delivery_events")).select(
        _p("delivery_event_id"),
        _p("allocation_id"),
        _p("line_id"),
        _p("delivered_qty", "int"),
        F.to_timestamp(_json("payload.actual_delivered_at"), TS_FORMAT).alias(
            "actual_delivered_at"
        ),
        F.from_json(_json("payload.delivered_unit_ids"), "array<string>").alias(
            "delivered_unit_ids"
        ),
        *_envelope_cols(),
    )


def _return_events(events: DataFrame) -> DataFrame:
    """EVERY Return event (not just the latest): this is history, and the label needs the whole chain."""
    return _feed(events, "seller_postsales.return_events").select(
        _p("return_id"),
        _p("event_type"),
        _p("allocation_id"),
        _p("line_id"),
        _p("quantity", "int"),
        _p("receipt_id"),
        _p("policy_regime_id"),
        *_envelope_cols(),
    )


def _refunds(events: DataFrame) -> DataFrame:
    """Refund (latest revision); sums the amounts allocated down to allocations into one number."""
    allocated = F.aggregate(
        F.from_json(_json("payload.allocation_money_rows"), ALLOCATION_MONEY),
        F.lit(0).cast("bigint"),
        lambda acc, row: acc + row["amount_minor"],
    )
    return _latest(_feed(events, "seller_postsales.refund_events")).select(
        F.col("entity_id").alias("refund_entity_id"),
        _p("refund_transaction_id"),
        _p("component"),
        _p("status"),
        _p("amount_minor", "bigint"),
        _p("currency"),
        _p("line_id"),
        _p("native_line_id"),
        _p("native_order_id"),
        _p("channel"),
        _p("transaction_revision", "int"),
        _p("unmatched_amount_minor", "bigint"),
        _p("unresolved_scope_ref"),
        F.coalesce(allocated, F.lit(0).cast("bigint")).alias("allocated_amount_minor"),
        _json("payload.allocation_money_rows[0].allocation_id").alias("allocation_id"),
        "corrects_source_event_id",
        *_envelope_cols(),
    )


def _listing_versions(events: DataFrame) -> DataFrame:
    """EVERY listing version, flagging `is_latest` instead of dropping old ones (needed for point-in-time lookups)."""
    latest = Window.partitionBy(*ENTITY_KEY).orderBy(F.col("entity_revision").desc())
    return (
        _feed(events, "seller_catalog.listing_versions")
        .withColumn("is_latest", F.row_number().over(latest) == 1)
        .select(
            _p("listing_id"),
            _p("channel"),
            _p("account_id"),
            _p("internal_sku_id"),
            _p("mapping_version"),
            F.to_timestamp(_json("payload.valid_from"), TS_FORMAT).alias("valid_from"),
            _p("listing_state"),
            _p("change_kind"),
            _p("correction_reason"),
            F.from_json(_json("payload.eligible_pool_ids"), "array<string>").alias(
                "eligible_pool_ids"
            ),
            F.col("entity_revision").alias("revision"),
            "corrects_source_event_id",
            "is_latest",
            *_envelope_cols(),
        )
    )


def _stock_operations(events: DataFrame) -> DataFrame:
    """Warehouse operations (latest revision) and the stock delta each operation causes."""
    return _latest(_feed(events, "seller_wms.stock_operations")).select(
        _p("operation_id"),
        _p("operation_type"),
        _p("pool_id"),
        _p("location_id"),
        _p("internal_sku_id"),
        _p("quantity", "int"),
        _p("delta_on_hand", "int"),
        _p("delta_reserved", "int"),
        _p("delta_non_sellable", "int"),
        _p("line_id"),
        _p("allocation_id"),
        *_envelope_cols(),
    )


def _stock_snapshots(events: DataFrame) -> DataFrame:
    """Inventory snapshots per pool/location/SKU (latest revision)."""
    return _latest(_feed(events, "seller_wms.stock_snapshots")).select(
        F.col("entity_id").alias("snapshot_id"),
        _p("pool_id"),
        _p("location_id"),
        _p("internal_sku_id"),
        _p("on_hand", "int"),
        _p("reserved", "int"),
        _p("non_sellable", "int"),
        *_envelope_cols(),
    )


def _inbound_events(events: DataFrame) -> DataFrame:
    """EVERY inbound warehouse event (history, not just the latest)."""
    return _feed(events, "seller_wms.inbound_events").select(
        _p("inbound_line_id"),
        _p("event_type"),
        _p("pool_id"),
        _p("internal_sku_id"),
        _p("quantity", "int"),
        _p("unreceived_qty", "int"),
        *_envelope_cols(),
    )


def _uncertainties(known: DataFrame) -> DataFrame:
    """Points the source itself declares uncertain (latest revision per `uncertainty_id`)."""
    return _newest_revision(
        _feed(known, "source.uncertainties").select(
            _json("uncertainty_id").alias("uncertainty_id"),
            _json("revision").cast("int").alias("revision"),
            _json("reason").alias("reason"),
            _json("resolution_state").alias("resolution_state"),
            _json("unallocated_product_amount_minor")
            .cast("bigint")
            .alias("unallocated_product_amount_minor"),
            ts("source_observed_at").alias("source_observed_at"),
        ),
        "uncertainty_id",
    )


def _coverage(known: DataFrame) -> DataFrame:
    """Producer-declared coverage per feed (latest revision per `coverage_id`)."""
    return _newest_revision(
        _feed(known, "source.coverage_envelopes").select(
            _json("coverage_id").alias("coverage_id"),
            _json("feed").alias("covered_feed"),
            _json("producer").alias("covered_producer"),
            _json("status").alias("status"),
            _json("revision").cast("int").alias("revision"),
            _json("source_record_count").cast("int").alias("source_record_count"),
            F.size(F.from_json(_json("unresolved_scope_refs"), "array<string>")).alias(
                "unresolved_scope_refs"
            ),
            _json("scope").alias("scope_json"),
            ts("source_observed_at").alias("source_observed_at"),
        ),
        "coverage_id",
    )


def build_silver(
    bronze: DataFrame, as_of: str | None = None, contracts_dir=None, registry=None
) -> dict:
    """Build Silver at `as_of`; quarantine counts only go to the receipt and never fail the job.

    Order: filter by `as_of` → contract gate (quarantine) → drop repeats/conflicts in business feeds →
    one `_<table>` function above per Silver table → derived tables (coverage, line_lifecycle).
    """
    registry = registry or Registry(contracts_dir)
    original = _known(bronze, as_of)
    known, quarantine = contract_gate(original, registry)
    native_records = original.filter(F.col("feed").endswith(".orders"))
    accepted_native = known.filter(F.col("feed").endswith(".orders"))
    native_with_hash = native_order_lines(known, include_record_hash=True)
    parsed_hashes = (
        native_with_hash.filter(
            F.col("native_order_id").isNotNull() & F.col("native_line_id").isNotNull()
        )
        .select("record_sha256")
        .distinct()
    )
    parsed_native_records = accepted_native.join(parsed_hashes, "record_sha256", "left_semi")
    events, stats = _dedup(known.filter(F.col("feed").isin(*OPERATIONAL_FEEDS)))
    events = events.cache()

    silver = {
        "registry": registry,
        "events": events,
        "dedup": stats,
        "line_commitments": _line_commitments(events),
        "allocations": _allocations(events),
        "deliveries": _deliveries(events),
        "return_events": _return_events(events),
        "refunds": _refunds(events),
        "listing_versions": _listing_versions(events),
        "stock_operations": _stock_operations(events),
        "stock_snapshots": _stock_snapshots(events),
        "inbound_events": _inbound_events(events),
        "uncertainties": _uncertainties(known),
        "coverage": _coverage(known),
        "native_order_lines": native_with_hash.drop("record_sha256"),
        "quarantine_records": quarantine.select(
            "dataset_version",
            "feed",
            "producer",
            "contract_schema_id",
            "reason",
            "detail",
            "source_file",
            "record_sha256",
            "raw_json",
        ),
        "native_records": native_records,
        "parsed_native_records": parsed_native_records,
        "coverage_receipts": coverage_receipts(known),
        "observed_evidence": observed_evidence(known),
    }
    silver["coverage_validity"] = coverage_validity(silver["observed_evidence"])
    silver["line_lifecycle"] = line_lifecycle(silver)
    return silver


def line_lifecycle(silver: dict) -> DataFrame:
    """One row per Order Line: aggregate each fact to line grain BEFORE joining to avoid fan-out."""
    allocations = (
        silver["allocations"]
        .groupBy("line_id")
        .agg(F.sum("quantity").alias("allocated_qty"), F.count("*").alias("allocation_count"))
    )
    deliveries = (
        silver["deliveries"]
        .groupBy("line_id")
        .agg(
            F.sum("delivered_qty").alias("delivered_qty"),
            F.max("actual_delivered_at").alias("last_delivered_at"),
        )
    )
    approved = (
        silver["return_events"]
        .filter(F.col("event_type") == "approved")
        .dropDuplicates(["return_id"])
        .groupBy("line_id")
        .agg(F.sum("quantity").alias("approved_return_qty"))
    )
    refunds = (
        silver["refunds"]
        .filter(
            (F.col("component") == "product")
            & (F.col("status") == "success")
            & F.col("line_id").isNotNull()
        )
        .groupBy("line_id")
        .agg(F.sum("allocated_amount_minor").alias("product_refund_success_minor"))
    )
    base = silver["line_commitments"].select(
        "line_id",
        "channel",
        "account_id",
        "native_order_id",
        "native_line_id",
        "internal_sku_id",
        "currency",
        "ordered_basis_qty",
        "canceled_unshipped_qty",
        "ordered_product_net_minor",
        "canceled_product_net_minor",
        "headline_product_net_minor",
        "fulfillment_terminal",
        "sale_mapping_version",
        "dataset_version",
    )
    out = base
    for part in (allocations, deliveries, approved, refunds):
        out = out.join(part, "line_id", "left")
    return out.fillna(
        0,
        subset=[
            "allocated_qty",
            "allocation_count",
            "delivered_qty",
            "approved_return_qty",
            "product_refund_success_minor",
        ],
    )


def _count(df: DataFrame) -> int:
    """Count rows for the reconciliation receipt."""
    return int(df.count())


def _native_checks(
    native: DataFrame, lines: DataFrame, eligible_lines: DataFrame | None = None
) -> dict:
    """Native Orders (what the Channel says) must match OMS lines (what the Seller records): present on both sides, same quantity/amount.

    Native is a snapshot at Order creation, so compare with the ORIGINAL ordered quantity/amount, not post-cancellation values.
    """
    key = ["channel", "native_order_id", "native_line_id"]
    parsed = native.filter(
        F.col("native_order_id").isNotNull() & F.col("native_line_id").isNotNull()
    )
    oms = lines.select(
        *key,
        "ordered_original_qty",
        "ordered_product_net_minor",
        F.col("currency").alias("oms_currency"),
    )
    expected = oms if eligible_lines is None else eligible_lines.select(*key)
    both = parsed.join(oms, key)
    return {
        "native_unparsed_rows": _count(native) - _count(parsed),
        "native_lines_without_commitment": _count(parsed.join(oms, key, "left_anti")),
        "commitments_without_native": _count(expected.join(parsed, key, "left_anti")),
        "native_quantity_mismatches": _count(
            both.filter(~F.col("quantity").eqNullSafe(F.col("ordered_original_qty")))
        ),
        "native_money_mismatches": _count(
            both.filter(
                ~F.col("net_minor").eqNullSafe(F.col("ordered_product_net_minor"))
                | ~F.col("currency").eqNullSafe(F.col("oms_currency"))
            )
        ),
    }


def reconcile(silver: dict) -> dict:
    """Invariant reconciliation; every value in `checks` must be 0. `info` is informational only."""
    events = silver["events"]
    lines = silver["line_commitments"]
    allocations = silver["allocations"]
    lifecycle = silver["line_lifecycle"]

    ids = events.select(F.col("source_event_id").alias("ref"))
    refs = events.select(
        "source_event_id",
        F.explode(
            F.from_json(F.get_json_object("raw_json", "$.depends_on_event_ids"), "array<string>")
        ).alias("ref"),
    ).unionByName(
        events.filter(F.col("corrects_source_event_id").isNotNull()).select(
            "source_event_id", F.col("corrects_source_event_id").alias("ref")
        )
    )
    missing_dependencies = _count(
        refs.join(ids, "ref", "left_anti").select("source_event_id").distinct()
    )

    delivered = (
        silver["deliveries"].groupBy("allocation_id").agg(F.sum("delivered_qty").alias("delivered"))
    )
    delivery_mismatch = _count(
        allocations.join(delivered, "allocation_id", "left").filter(
            (
                (F.col("allocation_state") == "delivered")
                & ~F.coalesce(F.col("delivered"), F.lit(0)).eqNullSafe(F.col("quantity"))
            )
            | (F.coalesce(F.col("delivered"), F.lit(0)) > F.col("quantity"))
        )
    )

    snapshots = silver["stock_snapshots"].alias("s")
    ops = silver["stock_operations"].alias("o")
    ledger = (
        snapshots.join(
            ops,
            (F.col("s.pool_id") == F.col("o.pool_id"))
            & (F.col("s.internal_sku_id") == F.col("o.internal_sku_id"))
            & (F.col("o.effective_at") <= F.col("s.effective_at")),
            "left",
        )
        .groupBy("s.snapshot_id", "s.on_hand", "s.reserved", "s.non_sellable")
        .agg(
            F.sum("o.delta_on_hand").alias("ledger_on_hand"),
            F.sum("o.delta_reserved").alias("ledger_reserved"),
            F.sum("o.delta_non_sellable").alias("ledger_non_sellable"),
        )
    )
    stock_mismatch = _count(
        ledger.filter(
            ~F.col("on_hand").eqNullSafe(F.col("ledger_on_hand"))
            | ~F.col("reserved").eqNullSafe(F.col("ledger_reserved"))
            | ~F.col("non_sellable").eqNullSafe(F.col("ledger_non_sellable"))
        )
    )

    mapping_versions = silver["listing_versions"].select(
        F.col("mapping_version").alias("sale_mapping_version")
    )
    headline_lines = lines.agg(F.sum("headline_product_net_minor")).first()[0] or 0
    headline_lifecycle = lifecycle.agg(F.sum("headline_product_net_minor")).first()[0] or 0
    refunds = silver["refunds"]

    checks = {
        "event_conflicts": len(silver["dedup"]["conflict_event_ids"]),
        "missing_dependencies": missing_dependencies,
        "duplicate_lines": _count(lines) - _count(lines.select("line_id").distinct()),
        "orphan_allocations": _count(
            allocations.join(lines.select("line_id"), "line_id", "left_anti")
        ),
        "quantity_mismatch_lines": _count(
            lifecycle.filter(
                F.col("fulfillment_terminal")
                & (
                    F.col("ordered_basis_qty")
                    != F.col("canceled_unshipped_qty") + F.col("allocated_qty")
                )
            )
        ),
        "delivery_mismatch_allocations": delivery_mismatch,
        "fanout_line_rows": _count(lifecycle) - _count(lines),
        "fanout_headline_delta_minor": int(headline_lifecycle - headline_lines),
        "refund_unscoped_rows": _count(
            refunds.filter(
                F.col("line_id").isNull()
                & (F.col("unmatched_amount_minor") > 0)
                & F.col("unresolved_scope_ref").isNull()
            )
        ),
        "refund_exceeds_headline_lines": _count(
            lifecycle.filter(
                F.col("product_refund_success_minor") > F.col("headline_product_net_minor")
            )
        ),
        "stock_snapshot_mismatches": stock_mismatch,
        "listing_binding_orphans": _count(
            lines.join(mapping_versions.distinct(), "sale_mapping_version", "left_anti")
        ),
    }
    quarantine = silver["quarantine_records"]
    quarantined_native = quarantine.filter(F.col("feed").endswith(".orders"))
    quarantined_identities = _native_order_identity(quarantined_native, silver["registry"])
    quarantined_keys = quarantined_identities.distinct()
    unknown_scope = (
        quarantined_keys.filter(F.col("native_order_id").isNull())
        .select("dataset_version", "channel")
        .distinct()
    )
    eligible_lines = lines.join(
        quarantined_keys, ["dataset_version", "channel", "native_order_id"], "left_anti"
    ).join(unknown_scope, ["dataset_version", "channel"], "left_anti")
    scoped_native = silver["native_order_lines"].join(
        unknown_scope, ["dataset_version", "channel"], "left_anti"
    )
    scoped_lines = lines.join(unknown_scope, ["dataset_version", "channel"], "left_anti")
    checks.update(_native_checks(scoped_native, scoped_lines, eligible_lines))
    checks["native_record_balance"] = (
        _count(silver["native_records"])
        - _count(silver["parsed_native_records"])
        - _count(quarantined_native)
    )
    receipts = silver["coverage_receipts"]
    complete_feeds = (
        receipts.filter(F.col("receipt_status") == "complete")
        .select(F.col("covered_feed").alias("feed"))
        .distinct()
    )
    checks["coverage_receipt_failures"] = _count(
        receipts.filter(F.col("receipt_status") == "partial")
    )
    checks["feeds_without_complete_coverage"] = _count(
        events.select("feed").distinct().join(complete_feeds, "feed", "left_anti")
    )
    info = {
        "bronze_input_rows": silver["dedup"]["input_rows"],
        "duplicate_rows_dropped": silver["dedup"]["duplicate_rows"],
        "events": _count(events),
        "lines": _count(lines),
        "allocations": _count(allocations),
        "headline_product_net_minor": int(headline_lines),
        "refund_unresolved_product_rows": _count(
            refunds.filter(F.col("line_id").isNull() & (F.col("component") == "product"))
        ),
        "uncertainties_unresolved": _count(
            silver["uncertainties"].filter(F.col("resolution_state") == "unresolved")
        ),
        "native_lines": _count(silver["native_order_lines"]),
        "coverage_complete": _count(receipts.filter(F.col("receipt_status") == "complete")),
        "coverage_unavailable_scopes": _count(
            receipts.filter(F.col("receipt_status") == "unavailable")
        ),
    }
    quarantine_summary = {
        "total": _count(quarantine),
        "by_reason": {
            r["reason"]: r["count"] for r in quarantine.groupBy("reason").count().collect()
        },
        "by_schema_id": {
            r["contract_schema_id"] or "null": r["count"]
            for r in quarantine.groupBy("contract_schema_id").count().collect()
        },
    }
    quarantined_orders = {
        r["channel"]: r["count"]
        for r in quarantined_keys.filter(F.col("native_order_id").isNotNull())
        .groupBy("channel")
        .count()
        .collect()
    }
    info["quarantined_native_records_without_order_identity"] = _count(
        quarantined_identities.filter(F.col("native_order_id").isNull())
    )
    return {
        "checks": checks,
        "info": info,
        "conflict_event_ids": silver["dedup"]["conflict_event_ids"],
        "quarantine": quarantine_summary,
        "quarantined_orders": quarantined_orders,
    }
