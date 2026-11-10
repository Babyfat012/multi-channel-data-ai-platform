# Data Generator trên AWS

<a id="trên-aws-2026-10-27-đến-2026-10-28"></a>

Trang này ứng với EDAI-1, dòng 5–10 của rubric: sinh batch ≥100 GB, có skew (dữ liệu lệch), schema evolution
(schema đổi theo thời gian), duplicate (bản ghi trùng), late arrival (tin đến muộn) và lưu lại nguồn. Generator chạy
trên EC2. Dữ liệu batch được lưu vào S3 và PostgreSQL; dữ liệu stream được phát qua Kafka trên EC2. Cấu hình:
[`scale-100gb.yaml`](../../configs/scale-100gb.yaml), [`default.yaml`](../../configs/default.yaml). Code và cách sinh
dữ liệu: [thiết kế generator](reference/generator_design.md).

## Trên AWS

Nguồn batch nằm trên S3 và PostgreSQL của EC2. Theo [receipt AWS](../evidence/aws/data-platform-aws-20261027.json):

- `raw_s3` có 17.393 object, tổng 141.774.479.087 byte, khớp với số byte trên đĩa EC2.
- Riêng nguồn Amazon, Walmart và SKU là 100.012.790.412 byte, khớp với manifest sinh ở local.
- PostgreSQL có 10.949.058 dòng Walmart và 2.000 SKU.

![DataHub lọc AWS S3: bốn dataset nguồn Amazon, Shopee, Shopify và TikTok Shop ở source-exports](../evidence/aws/datahub/s3-sources.png)

Ảnh DataHub chỉ cho thấy các dataset nguồn trên S3 đã được khai metadata; ảnh không hiển thị số byte hay histogram.
Số byte và số dòng ở trên lấy từ receipt. Các tỷ lệ skew, unique/total, duplicate và schema trước/sau ở các mục
trước đây là số của báo cáo local. Mục dưới đo lại bốn đặc tính đó **trên chính dữ liệu AWS**.
[Code generator](../../generator/__main__.py), [cấu hình 100 GB](../../configs/scale-100gb.yaml),
[manifest local để đối chiếu](../evidence/batch-100gb-manifest.json).

### Đặc tính dữ liệu đo trên AWS

Cách đo: chạy Trino trên EC2 (catalog `lakehouse` là Iceberg trên S3) với bảng Amazon 100 GB đã nạp. Có hai bảng
được đọc:

- Bronze `raw_amazon_orders_dp1`: payload gốc.
- Silver `stg_amazon_order_lines_dp2`: đã tách thành từng Order Line (dòng hàng trong đơn) và chuẩn hoá schema.

SQL: [`sql/profile_amazon_aws.sql`](../../sql/profile_amazon_aws.sql). Output đầy đủ:
[`amazon-data-profile-trino-20261031.txt`](../evidence/aws/amazon-data-profile-trino-20261031.txt) (chạy hết 2 phút
13 giây). Tỷ lệ trùng và dung lượng lấy từ `run_manifest.json`, file mà generator ghi lên S3 ngay khi chạy trên EC2
(`s3://…-raw/order_snapshots/run_manifest.json`, `dataset_version` `38a1f7b3…`).

| Đặc tính | Số đo trên AWS |
|---|---|
| **Skew SKU** (phân phối Zipf 0,8) | Có 2.000 SKU. 1% SKU bán chạy nhất (20 SKU) chiếm **25,58%** số Order Line; 10% (200 SKU) chiếm **54,27%**. SKU nhiều nhất có 8.316.158 dòng, ít nhất có 18.738 dòng (chênh 444 lần) |
| **Skew marketplace** | US `ATVPDKIKX0DER` 67,17%, UK `A1F83G8C2ARO7P` 17,90%, DE `A1PA6795UKMFR9` 14,93% |
| **Cardinality** (số giá trị khác nhau / tổng số dòng, trên 153.849.901 Order Line, đếm gần đúng bằng `approx_distinct`) | `order_id` ≈ 102 triệu (tỷ lệ 0,66, cao), `postal_code` 12.354 (0,00008), `city` 20, `seller_sku` ≈ 2.000 |
| **Schema evolution** | Ngày 2026-04-08, payload có các khoá `AmazonOrderId, FulfillmentChannel, IsBusinessOrder, IsPrime, …` (Orders API v0). Ngày 2026-04-12, payload chuyển thành `orderId, createdTime, lastUpdatedTime, salesChannel, programs, fulfillment, proceeds, shippingAddress, orderItems` (API 2026). Silver đọc được cả hai dạng: `schema_form` `v0` từ 2026-01-01 đến 04-09 có 74.821.347 dòng; `2026` từ 04-10 đến 06-29 có 79.028.554 dòng |
| **Duplicate** (theo manifest trên S3) | `amazon.orders` có 108.248.563 dòng trên 106.109.650 khoá, tức **1,98%** là trùng (1.062.953 bản cũ của cùng đơn và 1.075.960 bản sao y hệt). Walmart 1,97%, Shopify 1,98%, Shopee 1,97% |
| **Volume** | Amazon 86.728.943.600 B (3.316 file), Walmart 13.283.621.342 B. Tổng nguồn Amazon, Walmart và SKU là 100.012.790.412 B |

![Trino EC2: khoá payload trước/sau đổi API, schema_form v0/2026, top 1% SKU 25,58%, marketplace 67/18/15%, cardinality order_id/postal/city/sku](../evidence/aws/amazon-data-profile-trino-20261031.png)

Cách đọc các số trên:

- Đầu phân phối Zipf rất dày: chỉ 1% SKU đã giữ một phần tư số dòng.
- Skew mà Spark xử lý trong [spark.md](reference/spark.md) là skew dung lượng theo ngày, khác với skew SKU ở đây.
- `order_id` gần như duy nhất cho mỗi Order, còn `city` chỉ có 20 giá trị.
- Schema đổi đúng ngày thứ 100 (2026-04-11), đúng như cấu hình `amazon_orders_api_switch_day: 100`.

## Trên AWS

Lần chạy stream này: generator phát vào Kafka (chạy bằng Compose trên EC2) với `configs/default.yaml`, seed 42,
`speed=0` (phát nhanh nhất có thể), hết 41 giây. Số trong bảng dưới lấy từ
[manifest AWS](../evidence/aws/stream-manifest-20261028.json). Đây là một lần chạy riêng trên cloud, không dùng nhịp
`STREAM_SPEED=60` của lần chạy local ở trên.

| Đặc tính | Amazon notification | TikTok webhook |
|---|---|---|
| Số dòng / số khoá duy nhất | 399.410 / 399.410 | 93.554 / 92.157 |
| Duplicate | 0 | 1.397, tỷ lệ 0,014933 ≈ 1,49% |
| Độ trễ trung vị | 5,27 phút | 0,06 phút |
| Tỷ lệ tin trễ hơn 1 giờ | 0,049763 ≈ 4,98% | 0 |
| Tin sai thứ tự (out-of-order) quá 1 giờ | 19.729 | 0 |

Độ trễ của Amazon tính bằng khoảng cách giữa `EventTime` và thời điểm nhận tin. Tin trùng của TikTok giữ nguyên
`tts_notification_id` như tin gốc. Burst livestream (đợt bán dồn khi livestream): 148,5 Order/giờ so với 25,2
Order/giờ lúc thường, gấp 5,89 lần theo manifest.
[Code generator](../../generator/__main__.py), [cấu hình](../../configs/default.yaml),
[script đo](../../scripts/evidence/measure_order_snapshots_stream.py).

![Flink 1.20.3 Completed Jobs: baseline Amazon, late Amazon, hai job dedup và job cửa sổ đều FINISHED](../evidence/aws/flink-completed-jobs-20261028.png)

![Flink dedup_tiktok_state_proof: cạnh HASH tới Deduplicate, source gửi 93.554 record, job FINISHED](../evidence/aws/flink-dedup-graph-20261028.png)

Ảnh cho thấy các consumer Flink đã chạy xong và job dedup nhận 93.554 record. Các đặc tính trễ và trùng ở bảng lấy
từ manifest, không lấy từ ảnh. Flink tách được 10.166 tin muộn, ít hơn số generator đếm, vì watermark (mốc thời gian
Flink coi là "đã nhận đủ") tiến theo chu kỳ và theo từng partition; xem
[xử lý và job_id](reference/flink.md#trên-aws). Ngoài ra còn có đầu ra AWS của job late chạy liên tục: 6.674/6.674
dòng thỏa `event_time <= watermark`, kèm ví dụ tin muộn, xem
[Processing Jobs](processing_jobs.md#flink-baseline-late-arrival-dedup-và-window). Chưa có ảnh UI Flink AWS mở
operator watermark.

[Mục lục phần này](README.md) · [Mục lục chung](../README.md)
