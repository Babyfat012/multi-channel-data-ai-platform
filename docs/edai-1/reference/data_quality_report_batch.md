# Báo cáo chất lượng dữ liệu của Data Generator (Batch)

Báo cáo này ghi số đo thực tế của nguồn batch và các vấn đề dữ liệu được mô phỏng. Phần Stream nằm ở [data-quality-report-stream.md](data_quality_report_stream.md).

## Phạm vi 100 GB

Lần chạy chính dùng `configs/scale-100gb.yaml`: giống `default.yaml` (seed 42, 2.000 SKU, 180 ngày từ 2026-01-01) nhưng `orders_per_day = 851.506`, ghi ra `/data/raw`. Lý do chọn 851.506 và đơn vị 100,0 GB thập phân nằm trong chú thích đầu file cấu hình.

Ở phạm vi này chỉ ba nguồn có renderer: `amazon.orders`, `walmart.orders` và `seller.skus`. TikTok Shop, Shopify, Shopee nằm trong `not_rendered_yet` của manifest và được sinh ở nguồn nhỏ (xem mục cuối).

`/data/raw/run_manifest.json` ghi:

| Dataset | Dòng | Khóa khác nhau | Tỷ lệ trùng | File | Byte |
|---|---:|---:|---:|---:|---:|
| `amazon.orders` | 108.248.563 | 106.109.650 | 1,98% | 3.316 | 86.728.943.600 |
| `walmart.orders` | 10.949.058 | 10.732.935 | 1,97% | 3.255 | 13.283.621.342 |
| `seller.skus` | 2.000 | 2.000 | 0% | 1 | 225.470 |
| **Tổng** | | | | | **100.012.790.412** |

Tổng **100.012.790.412 byte = 100,01 GB** thập phân (93,14 GiB). Đây là byte thô trong manifest, đã được xác nhận khớp byte thật trên đĩa (`tests/unit/generator/test_generate_order_snapshots.py`), không phải số `df` (vốn hiển thị GiB và lệch 7,4%).

## Lần chạy nhỏ để đo chi tiết

Các bảng phân tích sâu (skew theo thành phố/danh mục, cardinality, schema evolution theo ngày, danh sách SKU) được đo trên lần chạy `configs/default.yaml` (4.000 Order/ngày, Amazon ở MinIO, Walmart ở PostgreSQL) bằng lệnh `report`, vì lệnh này đọc lại toàn bộ dữ liệu và join với `seller.skus`. Phương pháp đo giống hệt ở quy mô 100 GB, chỉ con số tuyệt đối khác. Mỗi mục dưới ghi rõ nguồn số.

- Cấu hình nhỏ: `configs/default.yaml` (seed 42, 2.000 SKU, 180 ngày từ 2026-01-01, 4.000 Order mỗi ngày).
- Nơi lưu: Amazon ở MinIO (bucket `platform`), Walmart và `seller.skus` ở PostgreSQL (database `sources`). Cách dựng xem [Docker.md](../docker.md).
- Lệnh đo:

```bash
set -a; . ./.env; set +a
export AWS_ACCESS_KEY_ID=$MINIO_ROOT_USER AWS_SECRET_ACCESS_KEY=$MINIO_ROOT_PASSWORD \
       FSSPEC_S3_ENDPOINT_URL=http://localhost:$MINIO_API_PORT
uv run python -m generator report --source s3://platform \
    --dsn "postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@localhost:$POSTGRES_PORT/$POSTGRES_DB"
```

Cột `_truth` là đáp án generator ghi trong `run_manifest.json`. Số đo đọc lại từ dữ liệu đã lưu, không lấy từ bộ nhớ của generator. Bảng đầy đủ của lệnh `report` nằm ở [generator-design.md, mục Kết quả chạy](generator_design.md#kết-quả-chạy).

## Kết quả đo

| Đặc tính | Ghi nhận | Số đo chính (100 GB) |
|---|---|---|
| Simulate skew | Đã đo | Prime Day 2026-04-30/05-01: 2.281.202 Order Amazon/ngày so với 570.490 ngày thường (×4,0), tỷ lệ trả 13,7% so với 11,4%. Chi tiết thành phố/danh mục đo ở lần chạy nhỏ |
| Simulate high cardinality | Đã đo | `amazon.orders`: 106.109.650 khóa Order khác nhau trên 108.248.563 dòng; `walmart.orders`: 10.732.935 khóa trên 10.949.058 dòng |
| Simulate schema evolution | Đã đo | `amazon.orders` đổi schema đúng ngày export 2026-04-10 (`proceeds` null 100% → 0%, `IsPrime` ngược lại) |
| Simulate another offline problem (duplicate 2%) | Đã đo | Trùng 1,98% (Amazon) và 1,97% (Walmart); phần lớn là bản cũ hơn cùng khóa, phải giữ bản mới nhất chứ không chỉ bỏ bản y hệt |
| Using generator configuration | Đã đo | Mọi tham số nằm trong `configs/*.yaml`, xem [mục 15](generator_design.md#các-khóa-cấu-hình-đang-dùng) |
| Store data (MinIO, PostgreSQL) | Đã đo | 100,01 GB thô: `amazon.orders` 86,73 GB trong 3.316 file, `walmart.orders` 13,28 GB trong 3.255 file, `seller.skus` 0,23 MB |

## Chi tiết từng vấn đề (đo trên lần chạy nhỏ)

### Skew

```text
== storefront_skew ==
storefront_id   config_weight  orders_truth  pct_truth  orders_exported
--------------  -------------  ------------  ---------  ---------------
amazon_us       0.45           334046        45.38      334046
tiktok_shop_us  0.15           108042        14.68
amazon_uk       0.12           89332         12.14      89332
amazon_de       0.1            74778         10.16      74778
shopify         0.08           57673         7.83
walmart_us      0.07           50499         6.86       50499
shopee_vn       0.03           21759         2.96

== city_skew == (thành phố lớn nhất mỗi Storefront)
storefront_id  city      orders  pct
-------------  --------  ------  -----
amazon_de      Berlin    36723   49.11
amazon_uk      London    43798   49.03
amazon_us      New York  154019  46.11
walmart_us     Dallas    24843   49.2

== category_skew ==
category     order_lines  pct    return_pct_truth
-----------  -----------  -----  ----------------
apparel      140711       17.71  23.32
toys         115003       14.48  6.69
electronics  112797       14.2   8.1
accessories  105104       13.23  10.16
sports       90499        11.39  8.3
home         85100        10.71  4.57
footwear     84430        10.63  16.08
beauty       60717        7.64   5.83

== top_skus == (3 SKU đầu)
sku        order_lines  pct
---------  -----------  ----
ELE-01103  43073        5.42
TOY-01453  24485        3.08
SPT-01304  18065        2.27

== daily_orders == (Sale Event: 2026-04-30, 2026-05-01)
day         orders  x_median
----------  ------  --------
2026-05-01  10950   3.7
2026-04-30  10887   3.68
2026-03-13  3119    1.05
```

- **Storefront**: số Order đo được khớp đáp án ở cả 4 Storefront đã có renderer (ví dụ `amazon_us` 334.046). TikTok Shop, Shopify và Shopee chưa có renderer nên để trống.
- **Thành phố**: Berlin 49,1%, London 49,0%, New York 46,1%, Dallas 49,2%. Job group theo thành phố sẽ có một partition lớn gấp đôi các partition còn lại cộng lại.
- **Danh mục**: apparel 17,7% và tỷ lệ trả 23,3%, home 10,7% và tỷ lệ trả 4,6%. Danh mục chỉ có trong `seller.skus`, nên phải join với PostgreSQL mới đo được.
- **SKU**: `ELE-01103` chiếm 5,4% Order Line (43.073 dòng), gấp gần 2 lần SKU thứ hai.
- **Ngày**: 2026-04-30 và 2026-05-01 (Prime Day) có khoảng 10.900 Order, gấp 3,7 lần trung vị.

### High cardinality

```text
== cardinality ==
dataset         column           approx_distinct  exact_distinct  approx_err_pct  rows
--------------  ---------------  ---------------  --------------  --------------  ------
amazon.orders   order_id         385608           498156          -22.59          508227
amazon.orders   line_id          818537           721099          13.51           721099
amazon.orders   asin             1844             2000            -7.8            721099
amazon.orders   sku              1526             2000            -23.7           721099
walmart.orders  order_id         50400            50499           -0.2            51523
walmart.orders  buyerId          21549            21719           -0.78           51523
walmart.orders  line_id          68348            73262           -6.71           73262
walmart.orders  tracking_number  52956            48629           8.9             73262
```

| Cột | Số đếm chính xác | HyperLogLog của DuckDB | Lệch |
|---|---|---|---|
| `amazon.orders.order_id` | 498.156 | 385.608 | -22,6% |
| `amazon.orders.line_id` | 721.099 | 818.537 | +13,5% |
| `walmart.orders.order_id` | 50.499 | 50.400 | -0,2% |

`approx_count_distinct` của DuckDB lệch tới hơn 20% ở cột nhiều giá trị, nên báo cáo in thêm số đếm chính xác. Job Spark ở bước sau sẽ so với số chính xác này.

### Schema evolution

```text
== schema_evolution_daily ==
export_date  rows  pct_null_proceeds  pct_null_IsPrime
-----------  ----  -----------------  ----------------
2026-04-07   2786  100.0              0.0
2026-04-08   2632  100.0              0.0
2026-04-09   2819  100.0              0.0
2026-04-10   2688  0.0                100.0
2026-04-11   2676  0.0                100.0
2026-04-12   2707  0.0                100.0
```

| Giai đoạn | Số ngày export | Dòng | `proceeds` null | `IsPrime` null |
|---|---|---|---|---|
| Trước, Orders v0 | 99 | 247.213 | 100% | 0% |
| Sau, Orders 2026-01-01 | 81 | 261.014 | 0% | 100% |

Ranh giới rơi đúng ngày export 2026-04-10: ngày 2026-04-09 còn schema cũ, ngày 2026-04-10 đã là schema mới. `AmazonOrderId` cũng đổi tên, nên job Bronze phải gộp hai schema theo tên cột mới.

### Duplicate 2%

```text
== duplicates ==
dataset         rows_before  distinct_keys  dup_pct_before  identical_copies  older_versions  rows_after  dup_pct_after  dup_pct_truth
--------------  -----------  -------------  --------------  ----------------  --------------  ----------  -------------  -------------
amazon.orders   508227       498156         1.98            5108              4963            498156      0.0            1.98
walmart.orders  51523        50499          1.99            0                 1024            50499       0.0            1.99
```

| Dataset | Dòng | Khóa khác nhau | Trùng | Bản y hệt | Bản cũ hơn | Sau dedup | Đáp án |
|---|---|---|---|---|---|---|---|
| `amazon.orders` | 508.227 | 498.156 | 1,98% | 5.108 | 4.963 | 0% | 1,98% |
| `walmart.orders` | 51.523 | 50.499 | 1,99% | 0 | 1.024 | 0% | 1,99% |

Khoảng một nửa bản trùng của Amazon là bản cũ hơn, mang Order Status cũ. Vì vậy chỉ bỏ các dòng giống hệt nhau là không đủ, mà phải giữ bản cập nhật mới nhất theo khóa.

### Nơi lưu (lần chạy nhỏ)

![MinIO console](../../images/batch/minio-bucket.png)

![DBeaver](../../images/batch/dbeaver-walmart-orders.png)

![DBeaver seller.skus](../../images/batch/dbeaver-seller-skus.png)

| Dataset | Nơi lưu | Định dạng | File | Dung lượng |
|---|---|---|---|---|
| `amazon.orders` | MinIO | JSON Lines | 180 | 388 MB |
| `labels` | MinIO | Parquet | 1 | 13 MB |
| `walmart.orders` | PostgreSQL | bảng, cột JSONB | | 74 MB |
| `seller.skus` | PostgreSQL | bảng | | 0,3 MB |

Ở phạm vi 100 GB, `/data/raw` giữ cùng định dạng nhưng chia nhiều file export hơn (3.316 file Amazon, 3.255 file Walmart) vì mỗi Export Date sinh một lô; `seller.skus` vẫn một file. Bảng `walmart.orders` trong PostgreSQL giữ nguồn của lần chạy nhỏ; bản 100 GB nằm ở `/data/raw` dưới dạng export để không phải nạp 13 GB vào database.

## Ba Channel nguồn nhỏ

`configs/scale-100gb.yaml` chưa render TikTok Shop, Shopify, Shopee (xem `not_rendered_yet`). Để ba Channel này có đủ Bronze/Silver/Gold mà không tốn 100 GB, dự án chạy thêm một nguồn nhỏ `configs/small.yaml` ghi ra `out/channels-small`: 50 SKU, 60 ngày lịch sử, 200 Order/ngày, seed 42. Lần này **cả năm Channel** đều có renderer (`not_rendered_yet` rỗng):

| Dataset | Dòng | Khóa khác nhau | Tỷ lệ trùng | File |
|---|---:|---:|---:|---:|
| `amazon.orders` | 8.999 | 8.823 | 1,96% | 666 |
| `tiktok_shop.orders` | 1.810 | 1.780 | 1,66% | 506 |
| `shopify.orders` | 910 | 891 | 2,09% | 388 |
| `walmart.orders` | 827 | 814 | 1,57% | 393 |
| `shopee.orders` | 354 | 347 | 1,98% | 226 |
| `seller.skus` | 50 | 50 | 0% | 1 |

Ba dòng `tiktok_shop.orders` / `shopify.orders` / `shopee.orders` (1.810 / 910 / 354) khớp đúng số dòng mà các job validate Bronze/Silver đếm được ở DP1/DP2, nên đường ống cho ba Channel mới được kiểm bằng số của chính nguồn nhỏ này. Đây là nguồn **nhỏ có chủ đích** để kiểm cấu trúc và đường ống, không phải bằng chứng đạt ngưỡng 100 GB; ngưỡng 100 GB chỉ dựa trên Amazon + Walmart ở `/data/raw`.

## Trên AWS (2026-10-27)

Nguồn batch nằm trên S3 và PostgreSQL EC2; [receipt AWS](../../evidence/aws/data-platform-aws-20261027.json)
ghi `raw_s3` 17.393 object, 141.774.479.087 byte, khớp byte trên đĩa EC2.
Phạm vi v1 Amazon + Walmart + SKU là 100.012.790.412 byte, khớp manifest local;
PostgreSQL có 10.949.058 dòng Walmart và 2.000 SKU.

![DataHub lọc AWS S3: bốn dataset nguồn Amazon, Shopee, Shopify và TikTok Shop ở source-exports](../../evidence/aws/datahub/s3-sources.png)

Ảnh DataHub chỉ cho thấy các dataset nguồn S3 đã được khai metadata, không hiển thị số byte hay histogram.
Số byte và số dòng trên lấy từ receipt; tỷ lệ skew, unique/total, duplicate và schema trước/sau ở các mục trước
thuộc báo cáo local. Mục dưới đo lại bốn đặc tính đó **trên chính dữ liệu AWS**.
[Code generator](../../../generator/__main__.py), [cấu hình 100 GB](../../../configs/scale-100gb.yaml),
[manifest local để đối chiếu](../../evidence/batch-100gb-manifest.json).

### Đặc tính dữ liệu đo trên AWS (2026-10-31)

Đo bằng Trino trên EC2 (catalog `lakehouse` = Iceberg S3) trên bảng Amazon 100 GB đã nạp: Bronze
`raw_amazon_orders_dp1` (payload gốc) và Silver `stg_amazon_order_lines_dp2` (đã tách Order Line, chuẩn hoá schema).
SQL: [`sql/profile_amazon_aws.sql`](../../../sql/profile_amazon_aws.sql); output đầy đủ:
[`amazon-data-profile-trino-20261031.txt`](../../evidence/aws/amazon-data-profile-trino-20261031.txt) (chạy 2 phút 13 giây).
Tỷ lệ trùng và dung lượng lấy từ `run_manifest.json` mà generator ghi lên S3 ngay trên EC2
(`s3://…-raw/order_snapshots/run_manifest.json`, `dataset_version` `38a1f7b3…`).

| Đặc tính | Số đo trên AWS |
|---|---|
| **Skew SKU** (Zipf 0,8) | 2.000 SKU; 1% SKU bán chạy nhất (20) chiếm **25,58%** Order Line, 10% (200) chiếm **54,27%**; SKU nhiều nhất 8.316.158 dòng, ít nhất 18.738 (gấp 444 lần) |
| **Skew marketplace** | US `ATVPDKIKX0DER` 67,17%, UK `A1F83G8C2ARO7P` 17,90%, DE `A1PA6795UKMFR9` 14,93% |
| **Cardinality** (unique / tổng, 153.849.901 Order Line, `approx_distinct`) | `order_id` ≈ 102 triệu (0,66 — cao), `postal_code` 12.354 (0,00008), `city` 20, `seller_sku` ≈ 2.000 |
| **Schema evolution** | Khoá payload ngày 2026-04-08: `AmazonOrderId, FulfillmentChannel, IsBusinessOrder, IsPrime, …` (Orders API v0); ngày 2026-04-12: `orderId, createdTime, lastUpdatedTime, salesChannel, programs, fulfillment, proceeds, shippingAddress, orderItems` (API 2026). Silver gộp được: `schema_form` `v0` 2026-01-01→04-09 = 74.821.347 dòng, `2026` 04-10→06-29 = 79.028.554 dòng |
| **Duplicate** (manifest S3) | `amazon.orders` 108.248.563 dòng / 106.109.650 khoá = **1,98%** (1.062.953 bản cũ + 1.075.960 bản sao y hệt); Walmart 1,97%, Shopify 1,98%, Shopee 1,97% |
| **Volume** | Amazon 86.728.943.600 B (3.316 file), Walmart 13.283.621.342 B; tổng nguồn v1 100.012.790.412 B |

![Trino EC2: khoá payload trước/sau đổi API, schema_form v0/2026, top 1% SKU 25,58%, marketplace 67/18/15%, cardinality order_id/postal/city/sku](../../evidence/aws/amazon-data-profile-trino-20261031.png)

Đọc số: phần đầu Zipf rất dày (1% SKU giữ một phần tư số dòng); skew mà Spark xử lý trong [spark.md](spark.md) là skew dung lượng theo ngày, khác skew SKU này;
`order_id` gần như duy nhất mỗi Order, còn `city` chỉ 20 giá trị; schema đổi đúng ngày 100 (2026-04-11) theo cấu hình
`amazon_orders_api_switch_day: 100`.
