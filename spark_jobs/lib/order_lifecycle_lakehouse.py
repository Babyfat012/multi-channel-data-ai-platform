"""Logic Bronze/Silver cho nguồn v2 (gói C): DataFrame vào, DataFrame ra, không I/O catalog.

Hai job `bronze/ingest_order_lifecycle.py` và `silver/build_order_lifecycle.py` gọi các hàm ở đây để
ghi Iceberg; test `tests/spark/check_order_lifecycle_silver.py` gọi thẳng trên fixture nhỏ.

Thiết kế (xem `docs/edai-1/reference/source_contract_order_lifecycle.md`):
  * Bronze là MỘT bảng chung cho mọi feed: mỗi dòng JSONL thành một dòng, giữ nguyên
    `raw_json`. Thêm feed hay đổi schema không phải sửa bảng.
  * Silver tách theo thực thể. Định danh sự kiện là
    (dataset_version, producer, source_epoch, source_event_id): bản lặp cùng nội dung
    bị bỏ, cùng id mà khác nội dung là xung đột và bị loại khỏi Silver.
  * Revision lớn nhất của một entity thay thế các revision trước (correction thay thế,
    không cộng thêm). Lịch sử giữ ở bảng listing/return/inbound.
  * `as_of` lọc theo thời điểm nguồn quan sát được (`source_observed_at`), để dựng lại
    đúng những gì đã biết tại một thời điểm.
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
    """Chuỗi UTC dạng contract (micro giây, hậu tố Z) -> timestamp."""
    return F.to_timestamp(F.col(column), TS_FORMAT)


def fact_sha256_col(column: str = "raw_json"):
    """Hash nội dung nghiệp vụ: bỏ `recorded_at` (giờ chạy máy, không phải sự thật nguồn).

    JSON nguồn là canonical (khóa sắp xếp, không khoảng trắng), nên `recorded_at` không bao
    giờ là khóa cuối của envelope và regex bỏ được cả dấu phẩy theo sau.
    """
    return F.sha2(F.regexp_replace(F.col(column), r'"recorded_at":"[^"]*",?', ""), 256)


def read_source_manifest(spark: SparkSession, root: str) -> dict:
    """Đọc manifest URI bằng Spark driver; đường dẫn cục bộ giữ cách đọc pathlib."""
    root = str(root)
    if re.match(r"^[A-Za-z][A-Za-z0-9+.-]*:", root):
        rows = spark.read.text(f"{root.rstrip('/')}/manifest.json", wholetext=True).collect()
        if len(rows) != 1:
            raise ValueError("Cần đúng một manifest.json")
        return json.loads(rows[0]["value"])
    return json.loads((Path(root) / "manifest.json").read_text())


def bronze_from_root(spark: SparkSession, root: str, run_id: str) -> DataFrame:
    """Đọc mọi file nguồn v2 dưới `root` thành một DataFrame Bronze, mỗi dòng JSONL một dòng."""
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
    """Dòng Walmart đọc qua JDBC -> cùng dạng Bronze; `rows` có dataset_version, purchase_order_id, raw_json.

    `raw_json` là chuỗi JSON PostgreSQL trả về (giữ nguyên, kể cả khoảng trắng). `source_label`
    là URL JDBC + bảng, không chứa mật khẩu. `export_date` là ngày snapshot manifest khai báo.
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
    """So mã đơn nhận qua JDBC với `snapshot_scope.order_ids` của manifest."""
    seen, duplicate = set(), set()
    for order_id in received:
        (duplicate if order_id in seen else seen).add(order_id)
    return {
        "missing": sorted(set(declared) - seen),
        "extra": sorted(seen - set(declared)),
        "duplicate": sorted(duplicate),
    }


def with_envelope(df: DataFrame) -> DataFrame:
    """(Tính lại) các cột envelope và hash từ `raw_json`; cột đường dẫn giữ nguyên."""
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
    """Chuỗi tiền thập phân ("330.00") -> số nguyên đơn vị nhỏ nhất (33000); null giữ null."""
    return (amount.cast("decimal(20,4)") * (10**scale)).cast("bigint")


def _items(path: str, ddl: str):
    """Mảng item lồng trong JSON gốc của Channel, nổ thành từng dòng."""
    return F.explode_outer(F.from_json(F.get_json_object("raw_json", path), ddl)).alias("item")


def native_order_lines(known: DataFrame, include_record_hash: bool = False) -> DataFrame:
    """Đơn native 5 Channel (ảnh chụp lúc tạo đơn) -> một dòng mỗi line, cột chung.

    Mỗi Channel có dạng riêng (Amazon có hai schema). `net_minor` là giá trị hàng sau giảm
    giá của cả line; `gross_minor` null khi Channel không gửi giá gốc (Walmart). Đơn không
    parse được vẫn ra một dòng với `native_order_id` null để đối soát đếm được.
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
    """Join theo ngày UTC trước khi kiểm interval, tránh thử mọi (tuần, dòng feed).

    Tuần nối tiếp chỉ có một envelope/ngày/feed; mỗi dòng join một envelope.
    Fixture interval dài vẫn tương thích nhờ explode ngày, không nhân lại toàn feed.
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
    """Áp coverage của producer lên đúng tập dòng nguồn đã nhận (giống `coverage_receipt` ở generator).

    Với mỗi envelope `complete` (revision mới nhất): dòng của feed đó, cùng dataset/producer/
    epoch, effective trong interval và quan sát TRƯỚC envelope phải khớp tập số thứ tự khai báo, số dòng, nằm trong khoảng
    sự kiện, và hash cả tập dòng (JSON canonical sắp theo id) phải bằng `record_set_sha256`.
    Envelope `unavailable` (scope không có quyền nguồn) không bị kiểm, chỉ được đếm.
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


# Feed mà chính sách nhãn v2 đọc (khớp `return_label.FEED_KINDS`) — coverage chỉ phủ các feed này.
LABEL_EVIDENCE_FEEDS = (
    "seller_oms.line_commitments",
    "seller_oms.fulfillment_allocations",
    "carrier.delivery_events",
    "seller_postsales.return_events",
    "seller_postsales.refund_events",
)


def observed_evidence(known: DataFrame) -> DataFrame:
    """Lịch sử quan sát cho nhãn v2: MỌI revision, chỉ bỏ retry trùng y hệt (cùng `record_sha256`).

    Khác `events` (đã bỏ xung đột, chỉ giữ revision mới cho bảng nghiệp vụ): chính sách nhãn cần
    thấy cả xung đột và correction để trả `unknown` / áp correction đúng thời điểm biết. Cột khóa
    `ev_*` chỉ để Gold gom bằng chứng theo line mà không phải nhân cả dataset cho từng line:
    - `ev_line_id` / `ev_line_ids`: scope có line;
    - `ev_order_id` / `ev_order_ids`: scope chỉ tới Order (ví dụ refund chưa rõ line);
    - không khóa nào (coverage, scope cấp kênh/tài khoản): phát cho mọi line cùng dataset;
    - `corrects_source_event_id`: correction được gửi thêm cho line có bản bị sửa.
    Lọc chính xác vẫn do `return_label.visible_events` làm; các khóa này chỉ là tập ứng viên rộng hơn.
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
    # Uncertainty dùng scope ở envelope; feed vận hành dựng scope từ payload, rỗng thì rơi về affected_scope.
    top_scope = F.coalesce(raw("$.scope"), raw("$.affected_scope"))
    in_scope = lambda key: F.get_json_object(top_scope, f"$.{key}")  # noqa: E731

    def in_affected(key):
        """Lấy thuộc tính phạm vi bị ảnh hưởng từ envelope hoặc payload."""
        return F.coalesce(raw(f"$.affected_scope.{key}"), raw(f"$.payload.affected_scope.{key}"))

    def pick(key):
        """Chọn thuộc tính phạm vi theo loại bản ghi uncertainty, coverage hoặc sự kiện."""
        return (
            F.when(is_uncertainty, in_scope(key))
            .when(is_coverage, F.lit(None))
            .otherwise(F.coalesce(raw(f"$.payload.{key}"), in_affected(key)))
        )

    def plural(key):
        """Đọc thuộc tính phạm vi dạng mảng chuỗi theo loại bản ghi."""
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
    """Kiểm MỘT lần mỗi revision coverage (đúng luật `return_label.source_line`), thay vì lặp ở từng line.

    Envelope `complete` hợp lệ khi tập dòng của đúng feed/producer/epoch/dataset, quan sát TRƯỚC
    envelope và effective trong interval, khớp số dòng, tập (partition, sequence) khai báo, hash canonical cả tập (sắp theo
    `source_event_id`), mọi `effective_at` nằm trong khoảng sự kiện; đủ partition; có capability,
    profile và hai đầu khoảng. Kết quả: `complete` / `partial` theo `evidence_event_id`.
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
        # collect_set loại trùng bằng hash lúc gom; array_distinct trên mảng struct là bậc hai
        # (1.816 s / 87% Silver ở e2e medium, xem docs/edai-1/reference/silver_order_lifecycle_performance.md).
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
    """Chỉ giữ dòng nguồn đã quan sát được TRƯỚC `as_of` (exclusive)."""
    if as_of is None:
        return df
    # Snapshot Order native (JSONL kênh, JDBC Walmart) không có `source_observed_at` trong envelope:
    # coi là biết được ở CUỐI ngày export (export_date + 1 ngày) — bảo thủ, không bao giờ sớm hơn
    # thực tế. Trước đây null bị lọc mất ⇒ dựng Silver có as_of làm rơi toàn bộ dòng native.
    known_at = F.coalesce(ts("source_observed_at"), F.to_timestamp(F.date_add("export_date", 1)))
    return df.filter(known_at < F.to_timestamp(F.lit(as_of), TS_FORMAT))


def _dedup(ops: DataFrame) -> tuple[DataFrame, dict]:
    """Bỏ bản lặp cùng nội dung; tách xung đột (cùng id khác nội dung, hoặc trùng revision)."""
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
    """Revision lớn nhất của mỗi entity; correction thay thế bản bị sửa."""
    window = Window.partitionBy(*ENTITY_KEY).orderBy(F.col("entity_revision").desc())
    return df.withColumn("rn", F.row_number().over(window)).filter(F.col("rn") == 1).drop("rn")


def _p(name: str, kind: str = "string"):
    """Một trường trong payload của envelope."""
    value = F.get_json_object("raw_json", f"$.payload.{name}")
    return (value.cast(kind) if kind != "string" else value).alias(name)


def _envelope_cols():
    """Cột envelope chung cho các bảng Silver nghiệp vụ."""
    return [
        "dataset_version",
        "source_event_id",
        "entity_revision",
        "operation",
        ts("effective_at").alias("effective_at"),
        ts("source_observed_at").alias("source_observed_at"),
    ]


def contract_gate(bronze: DataFrame, registry: Registry) -> tuple:
    """Cách ly trước parse bằng biểu thức Spark, không UDF và không làm fail vì cách ly.

    Với mảng chỉ kiểm item đầu; mảng rỗng hợp lệ theo JSON Schema được bỏ kiểm item.
    Trường nullable được phép null. Cổng Spark kiểm required, Python kiểm cả kiểu.
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
            # JSON Schema items không bắt mảng có phần tử; bỏ kiểm dưới các mảng rỗng.
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
    """Lấy mã đơn theo metadata của đúng schema đã đăng ký, không đoán payload.

    Schema lạ hoặc thiếu identity_path cho mã đơn null; đối soát loại cả scope kênh
    và báo số bản ghi chưa biết mã đơn, không gọi đó là số đơn.
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
    """Dòng của đúng một feed."""
    return df.filter(F.col("feed") == name)


def _json(field: str):
    """Một trường ở gốc `raw_json` (bản ghi phụ như coverage/uncertainty không có `payload`)."""
    return F.get_json_object("raw_json", f"$.{field}")


def _newest_revision(df: DataFrame, key: str) -> DataFrame:
    """Revision lớn nhất theo một khoá đơn (bản ghi phụ không có ENTITY_KEY như feed nghiệp vụ)."""
    window = Window.partitionBy(key).orderBy(F.col("revision").desc())
    return df.withColumn("rn", F.row_number().over(window)).filter(F.col("rn") == 1).drop("rn")


def _line_commitments(events: DataFrame) -> DataFrame:
    """Cam kết bán của OMS theo line; thêm `headline_product_net_minor` = net đặt − net huỷ."""
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
    """Phân bổ hàng cho line (revision mới nhất), kèm danh sách unit vật lý."""
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
    """Sự kiện giao của hãng vận chuyển (revision mới nhất): giờ giao thật và unit đã giao."""
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
    """MỌI sự kiện Return (không lấy mới nhất): đây là lịch sử, nhãn cần cả chuỗi."""
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
    """Refund (revision mới nhất); cộng tiền đã phân bổ xuống allocation thành một số."""
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
    """MỌI phiên bản listing, đánh dấu `is_latest` thay vì bỏ bản cũ (cần tra theo thời điểm)."""
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
    """Thao tác kho (revision mới nhất) và độ lệch tồn mà mỗi thao tác gây ra."""
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
    """Ảnh chụp tồn kho theo pool/location/SKU (revision mới nhất)."""
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
    """MỌI sự kiện nhập kho (lịch sử, không lấy mới nhất)."""
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
    """Điểm nguồn tự khai là chưa chắc (revision mới nhất mỗi `uncertainty_id`)."""
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
    """Coverage producer tự khai cho từng feed (revision mới nhất mỗi `coverage_id`)."""
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
    """Dựng Silver tại `as_of`; số cách ly chỉ báo receipt, không làm fail job.

    Thứ tự: lọc theo `as_of` → cổng hợp đồng (cách ly) → bỏ lặp/xung đột feed nghiệp vụ →
    mỗi bảng Silver một hàm `_<bảng>` ở trên → các bảng suy ra (coverage, line_lifecycle).
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
    """Một dòng mỗi Order Line: gộp từng fact về grain line TRƯỚC khi join để không fan-out."""
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
    """Đếm dòng cho receipt đối soát."""
    return int(df.count())


def _native_checks(
    native: DataFrame, lines: DataFrame, eligible_lines: DataFrame | None = None
) -> dict:
    """Đơn native (Channel nói) phải khớp line OMS (Seller ghi): có mặt hai bên, cùng số lượng/tiền.

    Native là ảnh chụp lúc tạo đơn nên so với số lượng/tiền ĐẶT BAN ĐẦU, không với số sau hủy.
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
    """Đối soát bất biến; mọi giá trị trong `checks` phải bằng 0. `info` chỉ để đọc."""
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
