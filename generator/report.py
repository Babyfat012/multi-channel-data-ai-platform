"""Báo cáo chất lượng dữ liệu của một lần chạy Batch, đo lại từ chính dữ liệu đã ghi.

Báo cáo chất lượng dữ liệu đọc
dữ liệu y như pipeline phía sau sẽ đọc (file gốc của Channel trên local hoặc MinIO,
bảng nguồn trong PostgreSQL), đo lại bằng DuckDB, rồi đặt từng con số cạnh đáp án
trong `run_manifest.json`. Khớp nhau nghĩa là generator chèn đúng lượng lỗi đã cấu hình.

Các bảng đầu ra:
    storefront_skew, city_skew, category_skew, top_skus, daily_orders   skew
    cardinality                                                        high cardinality
    schema_evolution, schema_evolution_daily                           schema evolution
    duplicates                                                         duplicate trước và sau dedup
    volume                                                             dung lượng và định dạng

Nguồn đọc:
    - Amazon: `source-exports/channel=amazon/...` (JSON Lines). Hai schema (Orders v0 và
      Orders 2026-01-01) được đọc như JSON thô rồi lấy field bằng `coalesce` tên cũ và tên mới,
      giống cách job Silver sẽ phải làm.
    - Walmart và danh mục SKU: bảng PostgreSQL nếu có `dsn`, ngược lại là file `pg-load/`.
    - Trên MinIO: thông tin đăng nhập lấy từ biến môi trường `AWS_ACCESS_KEY_ID`,
      `AWS_SECRET_ACCESS_KEY` và `FSSPEC_S3_ENDPOINT_URL` (cùng biến mà s3fs dùng).

Dedup theo quy tắc ở docs/data-platform/reference/generator_design.md mục 5: cùng khóa thì giữ bản có thời
điểm cập nhật mới nhất (Amazon `LastUpdateDate`/`lastUpdatedTime`, Walmart `fetched_at`).
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import duckdb

from generator.channels.amazon import MARKETPLACES
from generator.core.storefronts import DEFAULT_STOREFRONT_WEIGHTS
from generator.storage import OutputStore

AMAZON_GLOB = "source-exports/channel=amazon/dataset=orders/export_date=*/*.jsonl"
WALMART_GLOB = "pg-load/walmart.orders/export_date=*/*.jsonl"
SKUS_GLOB = "pg-load/seller.skus/*.jsonl"

# Nhóm file để báo cáo dung lượng: (tên, tiền tố, nơi lưu, định dạng).
DATASET_PREFIXES = (
    ("amazon.orders", "source-exports/channel=amazon/dataset=orders", "object store", "JSON Lines"),
    ("walmart.orders", "pg-load/walmart.orders", "PostgreSQL (trung chuyển)", "JSON Lines"),
    ("seller.skus", "pg-load/seller.skus", "PostgreSQL (trung chuyển)", "JSON Lines"),
    ("labels", "ground-truth/labels", "object store", "Parquet"),
    ("order_line_keys", "ground-truth/order_line_keys", "object store", "Parquet"),
)


@dataclass(frozen=True)
class Table:
    """Một bảng kết quả: tiêu đề, giải thích ngắn, tên cột và các dòng."""

    title: str
    note: str
    columns: tuple[str, ...]
    rows: list[tuple[Any, ...]]

    def as_dicts(self) -> list[dict[str, Any]]:
        """Các dòng dưới dạng dict theo tên cột, tiện cho test và notebook."""
        return [dict(zip(self.columns, row)) for row in self.rows]


class QualityReport:
    """Đo chất lượng dữ liệu của một lần chạy, gốc tại `source_root`."""

    def __init__(self, source_root: str, dsn: str | None = None) -> None:
        """Mở DuckDB, khai báo view cho từng nguồn và đọc manifest."""
        self.store = OutputStore(source_root)
        self.base = source_root.rstrip("/")
        self.dsn = dsn
        self.manifest = json.loads(self.store.fs.cat_file(self.store.path("run_manifest.json")))
        self.con = duckdb.connect()
        if source_root.startswith("s3://"):
            self._configure_s3()
        if dsn:
            self.con.execute("INSTALL postgres; LOAD postgres;")
            self.con.execute(f"ATTACH {_quote(dsn)} AS pg (TYPE postgres, READ_ONLY)")
        self._create_views()

    # --- Chuẩn bị -----------------------------------------------------------

    def _configure_s3(self) -> None:
        """Cho DuckDB đọc MinIO bằng cùng biến môi trường mà s3fs dùng."""
        endpoint = urlparse(os.environ.get("FSSPEC_S3_ENDPOINT_URL", "https://s3.amazonaws.com"))
        self.con.execute("INSTALL httpfs; LOAD httpfs;")
        self.con.execute(f"""
            CREATE SECRET minio (
                TYPE s3,
                KEY_ID {_quote(os.environ.get("AWS_ACCESS_KEY_ID", ""))},
                SECRET {_quote(os.environ.get("AWS_SECRET_ACCESS_KEY", ""))},
                ENDPOINT {_quote(endpoint.netloc)},
                URL_STYLE 'path',
                USE_SSL {str(endpoint.scheme == "https").lower()}
            )""")

    def _glob(self, pattern: str) -> str:
        """Đường dẫn glob đầy đủ, dạng DuckDB hiểu (thư mục local hoặc s3://)."""
        return _quote(f"{self.base}/{pattern}")

    def _create_views(self) -> None:
        """Khai báo view chuẩn hóa: đơn và dòng đơn của Amazon và Walmart, danh mục SKU."""
        marketplace_map = " ".join(
            f"WHEN {_quote(mid)} THEN {_quote(sf)}" for sf, (mid, _) in MARKETPLACES.items()
        )
        self.con.execute(f"""
            CREATE VIEW amazon_raw AS
            SELECT json, regexp_extract(filename, 'export_date=([0-9-]+)', 1)::DATE AS export_date
            FROM read_ndjson_objects({self._glob(AMAZON_GLOB)}, filename = true);

            CREATE VIEW amazon_orders AS
            SELECT
                coalesce(json->>'AmazonOrderId', json->>'orderId') AS order_id,
                CASE coalesce(json->>'MarketplaceId', json->'salesChannel'->>'marketplaceId')
                    {marketplace_map} END AS storefront_id,
                coalesce(json->'ShippingAddress'->>'City', json->'shippingAddress'->>'city') AS city,
                coalesce(json->>'PurchaseDate', json->>'createdTime')::TIMESTAMP AS created_at,
                coalesce(json->>'LastUpdateDate', json->>'lastUpdatedTime')::TIMESTAMP AS updated_at,
                export_date,
                (json->'proceeds') IS NULL AS proceeds_null,
                (json->'IsPrime') IS NULL AS is_prime_null,
                (json->>'AmazonOrderId') IS NULL AS order_id_v0_null,
                json
            FROM amazon_raw;

            CREATE VIEW amazon_latest AS
            SELECT * FROM amazon_orders
            QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY updated_at DESC) = 1;

            CREATE VIEW amazon_lines AS
            SELECT order_id, storefront_id,
                   coalesce(item->>'OrderItemId', item->>'orderItemId') AS line_id,
                   coalesce(item->>'SellerSKU', item->'product'->>'sellerSku') AS sku,
                   coalesce(item->>'ASIN', item->'product'->>'asin') AS asin
            FROM (SELECT order_id, storefront_id,
                         unnest(json_extract(coalesce(json->'OrderItems', json->'orderItems'), '$[*]')) AS item
                  FROM amazon_latest);
        """)

        if self.dsn:
            walmart_source = (
                "SELECT purchase_order_id, fetched_at::TIMESTAMP AS fetched_at, export_date, "
                "raw_payload::JSON AS payload FROM pg.walmart.orders"
            )
            skus_source = "SELECT sku, category FROM pg.seller.skus"
        else:
            walmart_source = (
                "SELECT json->>'purchase_order_id' AS purchase_order_id, "
                "(json->>'fetched_at')::TIMESTAMP AS fetched_at, "
                "regexp_extract(filename, 'export_date=([0-9-]+)', 1)::DATE AS export_date, "
                "json->'raw_payload' AS payload "
                f"FROM read_ndjson_objects({self._glob(WALMART_GLOB)}, filename = true)"
            )
            skus_source = (
                f"SELECT json->>'sku' AS sku, json->>'category' AS category "
                f"FROM read_ndjson_objects({self._glob(SKUS_GLOB)})"
            )

        self.con.execute(f"""
            CREATE VIEW walmart_orders AS
            SELECT purchase_order_id AS order_id, 'walmart_us' AS storefront_id,
                   payload->'shippingInfo'->'postalAddress'->>'city' AS city,
                   make_timestamp((payload->>'orderDate')::BIGINT * 1000) AS created_at,
                   fetched_at AS updated_at, export_date, payload
            FROM ({walmart_source});

            CREATE VIEW walmart_latest AS
            SELECT * FROM walmart_orders
            QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY updated_at DESC) = 1;

            CREATE VIEW walmart_lines AS
            SELECT order_id, storefront_id,
                   order_id || '-' || (line->>'lineNumber') AS line_id,
                   line->'item'->>'sku' AS sku,
                   line->'orderLineStatuses'->'orderLineStatus'->0->'trackingInfo'->>'trackingNumber' AS tracking_number
            FROM (SELECT order_id, storefront_id,
                         unnest(json_extract(payload->'orderLines'->'orderLine', '$[*]')) AS line
                  FROM walmart_latest);

            CREATE VIEW skus AS {skus_source};

            CREATE VIEW all_orders AS
            SELECT order_id, storefront_id, city, created_at FROM amazon_latest
            UNION ALL SELECT order_id, storefront_id, city, created_at FROM walmart_latest;

            CREATE VIEW all_lines AS
            SELECT line_id, sku FROM amazon_lines UNION ALL SELECT line_id, sku FROM walmart_lines;
        """)

    def _query(self, sql: str) -> tuple[tuple[str, ...], list[tuple[Any, ...]]]:
        """Chạy `sql`, trả về tên cột và các dòng."""
        rel = self.con.sql(sql)
        return tuple(rel.columns), rel.fetchall()

    # --- Skew ---------------------------------------------------------------

    def storefront_skew(self) -> Table:
        """Tỷ trọng Order theo Storefront: cấu hình, đáp án, và đo lại từ dữ liệu đã xuất."""
        truth = self.manifest["orders_by_storefront"]
        total = sum(truth.values())
        weights = self.manifest["config"].get("storefront_weights", DEFAULT_STOREFRONT_WEIGHTS)
        _, measured = self._query("SELECT storefront_id, count(*) FROM all_orders GROUP BY ALL")
        measured_by_sf = dict(measured)
        rows = [
            (sf, weights.get(sf), n, round(100 * n / total, 2), measured_by_sf.get(sf))
            for sf, n in sorted(truth.items(), key=lambda kv: -kv[1])
        ]
        return Table(
            "storefront_skew",
            "Order theo Storefront. `orders_exported` trống nghĩa là Channel chưa có renderer.",
            ("storefront_id", "config_weight", "orders_truth", "pct_truth", "orders_exported"),
            rows,
        )

    def city_skew(self) -> Table:
        """Tỷ trọng Order theo thành phố trong từng Storefront (skew địa lý)."""
        columns, rows = self._query("""
            SELECT storefront_id, city, count(*) AS orders,
                   round(100 * count(*) / sum(count(*)) OVER (PARTITION BY storefront_id), 2) AS pct
            FROM all_orders GROUP BY storefront_id, city ORDER BY storefront_id, orders DESC""")
        return Table(
            "city_skew", "Vài thành phố lớn chiếm phần lớn Order của mỗi Storefront.", columns, rows
        )

    def category_skew(self) -> Table:
        """Tỷ trọng Order Line theo danh mục (join `seller.skus`), kèm tỷ lệ trả thật."""
        columns, rows = self._query("""
            SELECT s.category, count(*) AS order_lines, round(100 * count(*) / sum(count(*)) OVER (), 2) AS pct
            FROM all_lines l JOIN skus s USING (sku) GROUP BY s.category ORDER BY order_lines DESC""")
        return_rates = self.manifest["labels"]["positive_rate_by_category"]
        rows = [(*row, _pct(return_rates.get(row[0]))) for row in rows]
        return Table(
            "category_skew",
            "Order Line theo danh mục. `return_pct_truth` là tỷ lệ trả thật từ manifest.",
            (*columns, "return_pct_truth"),
            rows,
        )

    def top_skus(self, n: int = 10) -> Table:
        """Các SKU bán chạy nhất và phần trăm Order Line của chúng (phân phối Zipf)."""
        columns, rows = self._query(f"""
            SELECT sku, count(*) AS order_lines, round(100 * count(*) / sum(count(*)) OVER (), 2) AS pct
            FROM all_lines GROUP BY sku ORDER BY order_lines DESC LIMIT {int(n)}""")
        return Table(
            "top_skus",
            "SKU bán chạy nhất nên chiếm khoảng 5% Order Line: tạo task chậm cho Spark.",
            columns,
            rows,
        )

    def daily_orders(self, n: int = 5) -> Table:
        """Các ngày nhiều Order nhất so với trung vị (Sale Event làm lệch theo ngày)."""
        columns, rows = self._query(f"""
            WITH daily AS (SELECT created_at::DATE AS day, count(*) AS orders FROM all_orders GROUP BY ALL)
            SELECT day, orders, round(orders / (SELECT median(orders) FROM daily), 2) AS x_median
            FROM daily ORDER BY orders DESC LIMIT {int(n)}""")
        sale = (self.manifest.get("drift") or {}).get("prime_day") or {}
        return Table(
            "daily_orders",
            f"Ngày nhiều Order nhất (Amazon và Walmart). Ngày Sale Event trong manifest: {sale.get('days')}.",
            columns,
            rows,
        )

    # --- High cardinality ---------------------------------------------------

    def cardinality(self) -> Table:
        """`approx_count_distinct` của các mã có cardinality cao, cạnh số đếm chính xác."""
        checks = (
            ("amazon.orders", "order_id", "amazon_orders"),
            ("amazon.orders", "line_id", "amazon_lines"),
            ("amazon.orders", "asin", "amazon_lines"),
            ("amazon.orders", "sku", "amazon_lines"),
            ("walmart.orders", "order_id", "walmart_orders"),
            ("walmart.orders", "payload->>'buyerId'", "walmart_orders"),
            ("walmart.orders", "line_id", "walmart_lines"),
            ("walmart.orders", "tracking_number", "walmart_lines"),
        )
        rows = []
        for dataset, column, view in checks:
            ((approx, exact, n),) = self.con.sql(
                f"SELECT approx_count_distinct({column}), count(DISTINCT {column}), count(*) FROM {view}"
            ).fetchall()
            rows.append(
                (
                    dataset,
                    column.replace("payload->>", "").strip("'"),
                    approx,
                    exact,
                    _pct(approx / exact - 1) if exact else None,
                    n,
                )
            )
        return Table(
            "cardinality",
            "Số giá trị khác nhau: ước lượng HyperLogLog của DuckDB và số đếm chính xác. "
            "HyperLogLog của DuckDB dùng ít bộ đếm nên có thể lệch hơn 10%.",
            ("dataset", "column", "approx_distinct", "exact_distinct", "approx_err_pct", "rows"),
            rows,
        )

    # --- Schema evolution ---------------------------------------------------

    def schema_evolution(self) -> Table:
        """Tỷ lệ null của field cũ và mới, trước và sau ngày Amazon đổi schema."""
        switch = self.manifest["schema_evolution"]["amazon.orders"]["first_export_date"]
        columns, rows = self._query(f"""
            SELECT CASE WHEN export_date < DATE '{switch}' THEN 'before (Orders v0)' ELSE 'after (Orders 2026)' END
                       AS period,
                   count(DISTINCT export_date) AS export_dates, count(*) AS rows,
                   round(100 * avg(proceeds_null::INT), 2) AS pct_null_proceeds,
                   round(100 * avg(is_prime_null::INT), 2) AS pct_null_IsPrime,
                   round(100 * avg(order_id_v0_null::INT), 2) AS pct_null_AmazonOrderId
            FROM amazon_orders GROUP BY ALL ORDER BY period DESC""")
        return Table(
            "schema_evolution",
            f"amazon.orders đổi schema từ export_date {switch}: field mới null ở partition cũ và ngược lại.",
            columns,
            rows,
        )

    def schema_evolution_daily(self, days: int = 3) -> Table:
        """Tỷ lệ null theo từng `export_date` quanh ngày đổi schema."""
        switch = self.manifest["schema_evolution"]["amazon.orders"]["first_export_date"]
        columns, rows = self._query(f"""
            SELECT export_date, count(*) AS rows,
                   round(100 * avg(proceeds_null::INT), 2) AS pct_null_proceeds,
                   round(100 * avg(is_prime_null::INT), 2) AS pct_null_IsPrime
            FROM amazon_orders
            WHERE export_date BETWEEN DATE '{switch}' - INTERVAL {int(days)} DAY
                                  AND DATE '{switch}' + INTERVAL {int(days) - 1} DAY
            GROUP BY ALL ORDER BY export_date""")
        return Table(
            "schema_evolution_daily",
            f"Tỷ lệ null theo export_date quanh ngày đổi schema {switch}.",
            columns,
            rows,
        )

    # --- Duplicate ----------------------------------------------------------

    def duplicates(self) -> Table:
        """Tỷ lệ trùng theo khóa trước và sau dedup, tách bản y hệt và bản cũ hơn."""
        rows = []
        for dataset, raw, latest in (
            ("amazon.orders", "amazon_orders", "amazon_latest"),
            ("walmart.orders", "walmart_orders", "walmart_latest"),
        ):
            ((n, keys, older),) = self.con.sql(f"""
                SELECT count(*), count(DISTINCT order_id),
                       count(*) FILTER (WHERE updated_at < max_updated)
                FROM (SELECT order_id, updated_at, max(updated_at) OVER (PARTITION BY order_id) AS max_updated
                      FROM {raw})""").fetchall()
            ((n_after, keys_after),) = self.con.sql(
                f"SELECT count(*), count(DISTINCT order_id) FROM {latest}"
            ).fetchall()
            truth = self.manifest["datasets"][dataset]["duplicate_rate"]
            rows.append(
                (
                    dataset,
                    n,
                    keys,
                    _pct(1 - keys / n),
                    n - keys - older,
                    older,
                    n_after,
                    _pct(1 - keys_after / n_after),
                    _pct(truth),
                )
            )
        return Table(
            "duplicates",
            (
                "Tỷ lệ trùng = 1 - count(distinct khóa) / count(*). Dedup giữ bản cập nhật mới nhất theo khóa. "
                "Walmart: mỗi lần kéo có `fetched_at` riêng, nên bản trùng nào cũng thành bản cũ hơn."
            ),
            (
                "dataset",
                "rows_before",
                "distinct_keys",
                "dup_pct_before",
                "identical_copies",
                "older_versions",
                "rows_after",
                "dup_pct_after",
                "dup_pct_truth",
            ),
            rows,
        )

    # --- Dung lượng ---------------------------------------------------------

    def volume(self) -> Table:
        """Dung lượng, số file và định dạng theo từng dataset, cộng dung lượng bảng PostgreSQL."""
        rows = []
        for name, prefix, where, fmt in DATASET_PREFIXES:
            full = self.store.path(prefix)
            sizes = (
                [info["size"] for info in self.store.fs.find(full, detail=True).values()]
                if self.store.fs.exists(full)
                else []
            )
            rows.append((name, where, fmt, len(sizes), _mb(sum(sizes))))
        if self.dsn:
            for table, fmt in (("walmart.orders", "heap, cột JSONB"), ("seller.skus", "heap")):
                ((size,),) = self.con.execute(
                    "SELECT * FROM postgres_query('pg', ?)",
                    [f"SELECT pg_total_relation_size('{table}')"],
                ).fetchall()
                rows.append((table, "PostgreSQL (bảng, gồm index)", fmt, None, _mb(size)))
        return Table(
            "volume",
            "Dung lượng sau khi lưu, theo nơi lưu và định dạng.",
            ("dataset", "storage", "format", "files", "size_mb"),
            rows,
        )

    def all_tables(self) -> list[Table]:
        """Mọi bảng của báo cáo, theo thứ tự in."""
        return [
            self.storefront_skew(),
            self.city_skew(),
            self.category_skew(),
            self.top_skus(),
            self.daily_orders(),
            self.cardinality(),
            self.schema_evolution(),
            self.schema_evolution_daily(),
            self.duplicates(),
            self.volume(),
        ]


def render_text(table: Table) -> str:
    """Định dạng bảng thành chữ canh cột, để in ra terminal và chụp màn hình."""
    cells = [tuple("" if v is None else str(v) for v in row) for row in table.rows]
    widths = [
        max(len(c), *(len(r[i]) for r in cells)) if cells else len(c)
        for i, c in enumerate(table.columns)
    ]
    line = "  ".join("-" * w for w in widths)
    out = [
        f"== {table.title} ==",
        table.note,
        "  ".join(c.ljust(w) for c, w in zip(table.columns, widths)),
        line,
    ]
    out += ["  ".join(v.ljust(w) for v, w in zip(row, widths)) for row in cells]
    return "\n".join(out)


def render_markdown(table: Table) -> str:
    """Định dạng bảng thành Markdown, để dán vào tài liệu trong `docs/`."""
    out = [
        f"#### {table.title}",
        "",
        table.note,
        "",
        "| " + " | ".join(table.columns) + " |",
        "|" + "---|" * len(table.columns),
    ]
    out += [
        "| " + " | ".join("" if v is None else str(v) for v in row) + " |" for row in table.rows
    ]
    return "\n".join(out)


def _quote(value: str) -> str:
    """Đặt chuỗi trong nháy đơn kiểu SQL."""
    return "'" + value.replace("'", "''") + "'"


def _pct(fraction: float | None) -> float | None:
    """Đổi tỷ lệ 0..1 thành phần trăm làm tròn 2 chữ số."""
    return None if fraction is None else round(100 * fraction, 2)


def _mb(n_bytes: int) -> float:
    """Byte sang MB, làm tròn 2 chữ số."""
    return round(n_bytes / 1024**2, 2)
