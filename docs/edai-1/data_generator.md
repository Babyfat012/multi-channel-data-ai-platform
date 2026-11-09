# Data Generator trên AWS

<a id="trên-aws-2026-10-27-đến-2026-10-28"></a>

EDAI-1, dòng 5–10: batch ≥100 GB, skew, schema evolution, duplicate, late arrival và lưu nguồn. Generator chạy EC2, batch lưu S3/PostgreSQL, stream qua Kafka EC2. Cấu hình: [`scale-100gb.yaml`](../../configs/scale-100gb.yaml), [`default.yaml`](../../configs/default.yaml). Code và cách sinh: [thiết kế generator](reference/generator_design.md).

## Trên AWS

Nguồn batch nằm trên S3 và PostgreSQL EC2; [receipt AWS](../evidence/aws/data-platform-aws-20261027.json)
ghi `raw_s3` 17.393 object, 141.774.479.087 byte, khớp byte trên đĩa EC2.
Nguồn Amazon + Walmart + SKU là 100.012.790.412 byte, khớp manifest local;
PostgreSQL có 10.949.058 dòng Walmart và 2.000 SKU.

![DataHub lọc AWS S3: bốn dataset nguồn Amazon, Shopee, Shopify và TikTok Shop ở source-exports](../evidence/aws/datahub/s3-sources.png)

Ảnh DataHub chỉ cho thấy các dataset nguồn S3 đã được khai metadata, không hiển thị số byte hay histogram.
Số byte và số dòng trên lấy từ receipt; tỷ lệ skew, unique/total, duplicate và schema trước/sau ở các mục trước
thuộc báo cáo local. Mục dưới đo lại bốn đặc tính đó **trên chính dữ liệu AWS**.
[Code generator](../../generator/__main__.py), [cấu hình 100 GB](../../configs/scale-100gb.yaml),
[manifest local để đối chiếu](../evidence/batch-100gb-manifest.json).

### Đặc tính dữ liệu đo trên AWS

Đo bằng Trino trên EC2 (catalog `lakehouse` = Iceberg S3) trên bảng Amazon 100 GB đã nạp: Bronze
`raw_amazon_orders_dp1` (payload gốc) và Silver `stg_amazon_order_lines_dp2` (đã tách Order Line, chuẩn hoá schema).
SQL: [`sql/profile_amazon_aws.sql`](../../sql/profile_amazon_aws.sql); output đầy đủ:
[`amazon-data-profile-trino-20261031.txt`](../evidence/aws/amazon-data-profile-trino-20261031.txt) (chạy 2 phút 13 giây).
Tỷ lệ trùng và dung lượng lấy từ `run_manifest.json` mà generator ghi lên S3 ngay trên EC2
(`s3://…-raw/order_snapshots/run_manifest.json`, `dataset_version` `38a1f7b3…`).

| Đặc tính | Số đo trên AWS |
|---|---|
| **Skew SKU** (Zipf 0,8) | 2.000 SKU; 1% SKU bán chạy nhất (20) chiếm **25,58%** Order Line, 10% (200) chiếm **54,27%**; SKU nhiều nhất 8.316.158 dòng, ít nhất 18.738 (gấp 444 lần) |
| **Skew marketplace** | US `ATVPDKIKX0DER` 67,17%, UK `A1F83G8C2ARO7P` 17,90%, DE `A1PA6795UKMFR9` 14,93% |
| **Cardinality** (unique / tổng, 153.849.901 Order Line, `approx_distinct`) | `order_id` ≈ 102 triệu (0,66 — cao), `postal_code` 12.354 (0,00008), `city` 20, `seller_sku` ≈ 2.000 |
| **Schema evolution** | Khoá payload ngày 2026-04-08: `AmazonOrderId, FulfillmentChannel, IsBusinessOrder, IsPrime, …` (Orders API v0); ngày 2026-04-12: `orderId, createdTime, lastUpdatedTime, salesChannel, programs, fulfillment, proceeds, shippingAddress, orderItems` (API 2026). Silver gộp được: `schema_form` `v0` 2026-01-01→04-09 = 74.821.347 dòng, `2026` 04-10→06-29 = 79.028.554 dòng |
| **Duplicate** (manifest S3) | `amazon.orders` 108.248.563 dòng / 106.109.650 khoá = **1,98%** (1.062.953 bản cũ + 1.075.960 bản sao y hệt); Walmart 1,97%, Shopify 1,98%, Shopee 1,97% |
| **Volume** | Amazon 86.728.943.600 B (3.316 file), Walmart 13.283.621.342 B; tổng nguồn Amazon + Walmart + SKU 100.012.790.412 B |

![Trino EC2: khoá payload trước/sau đổi API, schema_form v0/2026, top 1% SKU 25,58%, marketplace 67/18/15%, cardinality order_id/postal/city/sku](../evidence/aws/amazon-data-profile-trino-20261031.png)

Đọc số: phần đầu Zipf rất dày (1% SKU giữ một phần tư số dòng); skew mà Spark xử lý trong [spark.md](reference/spark.md) là skew dung lượng theo ngày, khác skew SKU này;
`order_id` gần như duy nhất mỗi Order, còn `city` chỉ 20 giá trị; schema đổi đúng ngày 100 (2026-04-11) theo cấu hình
`amazon_orders_api_switch_day: 100`.

## Trên AWS

Generator phát vào Kafka Compose trên EC2, `configs/default.yaml`, seed 42, `speed=0`, hết 41 giây.
[Manifest AWS](../evidence/aws/stream-manifest-20261028.json) là nguồn số của bảng dưới;
đây là lần cloud riêng, không dùng nhịp `STREAM_SPEED=60` của lần local ở trên.

| Đặc tính | Amazon notification | TikTok webhook |
|---|---|---|
| Dòng / unique key | 399.410 / 399.410 | 93.554 / 92.157 |
| Duplicate | 0 | 1.397, tỷ lệ 0,014933 ≈ 1,49% |
| Trễ trung vị | 5,27 phút | 0,06 phút |
| Tỷ lệ trễ hơn 1 giờ | 0,049763 ≈ 4,98% | 0 |
| Out-of-order quá 1 giờ | 19.729 | 0 |

Tin late là Amazon `EventTime` so thời điểm nhận; duplicate TikTok giữ nguyên `tts_notification_id`.
Burst livestream: 148,5 Order/giờ so 25,2 giờ thường, tỷ số 5,89 theo manifest.
[Code generator](../../generator/__main__.py), [cấu hình](../../configs/default.yaml),
[script đo](../../scripts/evidence/measure_order_snapshots_stream.py).

![Flink 1.20.3 Completed Jobs: baseline Amazon, late Amazon, hai job dedup và job cửa sổ đều FINISHED](../evidence/aws/flink-completed-jobs-20261028.png)

![Flink dedup_tiktok_state_proof: cạnh HASH tới Deduplicate, source gửi 93.554 record, job FINISHED](../evidence/aws/flink-dedup-graph-20261028.png)

Ảnh cho thấy consumer Flink hoàn tất và dedup nhận 93.554 record; đặc tính trễ/trùng ở bảng lấy từ manifest.
Flink tách 10.166 tin muộn, thấp hơn phép đếm generator vì watermark theo chu kỳ/partition;
[xử lý và job_id](reference/flink.md#trên-aws). Có thêm đầu ra AWS của job late chạy liên tục (6.674/6.674 dòng thỏa `event_time <= watermark`, kèm ví dụ tin muộn, xem [Processing Jobs](processing_jobs.md#flink-baseline-late-arrival-dedup-và-window)); chưa có UI Flink AWS mở operator watermark.

[Mục lục phần này](README.md) · [Mục lục chung](../README.md)
