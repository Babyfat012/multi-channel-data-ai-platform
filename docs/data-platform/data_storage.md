# Data Storage trên AWS

Trang này ứng với EDAI-1, dòng 20–21 của rubric: tối ưu lưu trữ ở lakehouse (Iceberg trên S3) và warehouse
(PostgreSQL).

Code / cấu hình: [Partition rewrite](../../spark_jobs/maintenance/repartition_small_channel.py), [export warehouse](../../spark_jobs/warehouse/export_order_snapshot_lines.py).

Các kết quả dưới đây là của các lần chạy AWS đã ghi lại; trang này không khẳng định dịch vụ hiện vẫn đang bật.

## Lakehouse trên AWS: gộp file nhỏ bằng partition evolution

**Kết luận:** đổi cách chia partition của bảng Shopee từ theo ngày sang theo tháng giúp số file giảm từ 180 xuống 6
và truy vấn nhanh hơn rõ rệt, trong khi dữ liệu giữ nguyên.

**Vấn đề.** DAG `maintenance_iceberg` chạy trên `bronze.raw_shopee_orders_dp1` ra **no-op** (không có gì để làm).
Bảng chia partition theo ngày và mỗi ngày chỉ có đúng 1 file khoảng 2,4 MB. Lệnh `rewrite_data_files` chỉ gộp file
bên trong từng partition, nên không có gì để gộp. Nguyên nhân gốc là các kênh có kích thước rất khác nhau: Shopee nhỏ
nhất nhưng vẫn bị chia theo ngày giống Amazon (gap #16).

**Cách sửa.** Job [`repartition_small_channel.py`](../../spark_jobs/maintenance/repartition_small_channel.py) làm bốn
việc:

1. Đổi partition spec ngay trên bảng hiện có (partition evolution):
   `ALTER TABLE … REPLACE PARTITION FIELD export_date WITH months(export_date)`.
2. Viết lại toàn bộ file theo spec mới: `rewrite_data_files(options => map('rewrite-all','true'))`.
3. Đo các chỉ số trước và sau.
4. Dừng lại nếu fingerprint (mã băm nội dung bảng) thay đổi, tức dữ liệu bị đổi.

| | Trước (ngày) | Sau (tháng) |
|---|---|---|
| Số file dữ liệu | 180 | **6** |
| Cỡ trung bình | 2,44 MB | **73,13 MB** |
| Spec đang dùng | 1 | 1 (mọi file đã sang spec mới) |
| `count(*)` cả bảng (trung vị 3 lần) | 1,040 s | **0,286 s** (3,6×) |
| Lọc 30 ngày cuối (trung vị 3 lần) | 7,943 s | **0,968 s** (8,2×) |
| Dòng / fingerprint | 4.689.957 / -2551885948954549477803 | **giống hệt** |

Vì sao lọc 30 ngày nhanh gấp 8 lần: trước đây phải mở khoảng 30 file nhỏ, mỗi file tốn một lần đọc footer và một
request S3; nay chỉ cần mở 1–2 file theo tháng. Đổi lại, khi lọc đúng một ngày thì nay phải đọc cả file tháng chứa
ngày đó.

Giới hạn: job Bronze `dp1` ghi bằng `createOrReplace … partitionedBy("export_date")`, nên chạy lại dp1 sẽ đưa bảng
về chia theo ngày. Muốn đóng hẳn gap #16, bước ingest cần chọn spec theo từng kênh. Muốn quay về spec cũ: chạy
`CALL lakehouse.system.rollback_to_snapshot(...)` với `snapshot_before` ghi trong biên lai.

## Warehouse trên AWS: 30 ngày, năm Channel

**Kết luận:** index trên `seller_sku` làm truy vấn SKU hiếm nhanh gấp 28 lần; với SKU phổ biến, PostgreSQL vẫn
quét cả bảng, và đó là hành vi đúng.

DAG [`dp4_warehouse`](../../dags/dp4_warehouse.py) chép 30 ngày trước cutoff (từ 2026-05-31 đến 2026-06-30) của
`lakehouse.gold.fact_order_lines_dp2` sang `warehouse.fact_order_lines_30d_dp2` trên PostgreSQL của EC2
([job](../../spark_jobs/warehouse/export_order_snapshot_lines.py)). Bảng có **37.045.741 dòng**, nặng **5.513 MB**;
index `(seller_sku)` nặng **250 MB**. Trước khi tạo index, job đối soát số dòng theo từng Channel với Gold.

Số dòng theo SKU lệch rất mạnh: nhiều nhất là `ELE-01103` với 2.011.389 dòng (5,4%), ít nhất là `APP-00482` với
4.517 dòng (0,012%).

Cách đo: chạy `EXPLAIN (ANALYZE, BUFFERS) SELECT count(*), sum(line_net_amount) … WHERE seller_sku = …` trên cùng một
bảng, lần đầu sau khi `DROP INDEX`, lần sau khi đã `CREATE INDEX` và `ANALYZE`. "Buffer đọc" là số trang dữ liệu
PostgreSQL phải đọc.

| SKU | Index | Kế hoạch | Buffer đọc | `Execution Time` |
|---|---|---|---|---|
| `APP-00482` (0,012%) | không | `Parallel Seq Scan`, lọc bỏ ~37 tr dòng | 690.595 | **42.567 ms** |
| `APP-00482` | có | `Bitmap Index Scan` → `Bitmap Heap Scan` | **4.475** | **1.497 ms** (28×) |
| `ELE-01103` (5,4%) | không | `Parallel Seq Scan` | 690.558 | 41.745 ms |
| `ELE-01103` | có | **vẫn `Parallel Seq Scan`** — planner bỏ index | 690.502 | 41.304 ms |

- Index chỉ có lợi khi điều kiện lọc **chọn lọc** (chỉ khớp ít dòng): với SKU hiếm, số trang đĩa phải đọc giảm
  khoảng 150 lần.
- Dòng của SKU phổ biến (5,4%) rải trên gần mọi trang. Đi qua index thì vẫn phải mở hầu hết các trang, lại theo thứ
  tự ngẫu nhiên. Vì vậy planner chọn quét tuần tự. Đây là hành vi mong đợi, không phải lỗi.
- Có index rồi mà `APP-00482` vẫn mất 1,5 s, vì 4.517 dòng của nó nằm rải trên 4.467 trang (bảng không xếp theo
  SKU, và cache trên EBS còn lạnh). Muốn nhanh hơn có hai cách: sắp lại bảng theo index bằng `CLUSTER … USING`,
  hoặc dùng index bao (`INCLUDE (line_net_amount)`) để truy vấn chỉ cần đọc index.

Ảnh EXPLAIN và log được nhúng ở [mục AWS](#trên-aws).

## Trên AWS

Mục này gom ảnh và log cho hai phép đo ở trên. Spark/Iceberg và PostgreSQL chạy bằng Compose trên EC2; dữ liệu
lakehouse nằm ở S3. [Receipt AWS](../evidence/aws/data-platform-aws-20261027.json) có hai mục
`lakehouse_repartition_shopee` và `airflow_dp4_warehouse`. Phân tích chi tiết xem ở
[Lakehouse](#lakehouse-trên-aws-gộp-file-nhỏ-bằng-partition-evolution) và
[Warehouse](#warehouse-trên-aws-30-ngày-năm-channel).

**Lakehouse (Shopee).** [Job partition evolution](../../spark_jobs/maintenance/repartition_small_channel.py) đổi
partition từ ngày sang tháng rồi viết lại toàn bộ (rewrite-all). Số file giảm từ 180 xuống 6; bảng vẫn 4.689.957
dòng và fingerprint `-2551885948954549477803` giữ nguyên. Trung vị ba lần chạy `count(*)` giảm từ 1,040 s xuống
0,286 s; lọc 30 ngày giảm từ 7,943 s xuống 0,968 s. Các số này lấy từ receipt; ảnh AWS chụp riêng trước/sau
compaction nằm ngay bên dưới. Lần maintenance trước đó ra no-op vì mỗi partition ngày chỉ có một file.

Để có ảnh trước/sau mà không phải chạy lại job, đọc cùng một bảng ở hai snapshot (phiên bản bảng) qua Trino. Các
câu này chỉ đọc; đã chạy trên EC2 và kết quả ở bên dưới:

```sql
-- 1. snapshot trước khi viết lại: snapshot cuối cùng có operation = append/overwrite trước lần replace
SELECT snapshot_id, committed_at, operation
FROM lakehouse.bronze."raw_shopee_orders_dp1$snapshots" ORDER BY committed_at;
-- 2. trước (mong đợi 180 file, 4.689.957 dòng)
SELECT count(DISTINCT "$path") AS files, count(*) AS rows
FROM lakehouse.bronze.raw_shopee_orders_dp1 FOR VERSION AS OF <snapshot_id>;
-- 3. sau (mong đợi 6 file, 4.689.957 dòng)
SELECT count(DISTINCT "$path") AS files, count(*) AS rows
FROM lakehouse.bronze.raw_shopee_orders_dp1;
```

**Kết quả trên AWS (Trino 483 trên EC2).** Bảng `bronze.raw_shopee_orders_dp1` có đúng hai snapshot:

- `564699590744609361` (`append`): lần ghi ban đầu.
- `2727359824964749759` (`replace`): lần viết lại của job `repartition_small_channel`.

Snapshot cũ chưa bị `expire_snapshots` xóa nên vẫn đọc lại được:

| Trạng thái | Số file dữ liệu | Số dòng |
|---|---:|---:|
| Trước: `FOR VERSION AS OF 564699590744609361` (append, chia theo ngày) | **180** | **4.689.957** |
| Sau: hiện tại (replace, chia theo tháng) | **6** | **4.689.957** |

Số dòng bằng nhau ở hai phía (dữ liệu không đổi) và khớp receipt (180 → 6 file; fingerprint giữ nguyên).

![Trino CLI trên EC2: $snapshots có append và replace; trước 180 file, 4.689.957 dòng; sau 6 file, 4.689.957 dòng](../evidence/aws/lakehouse-before-after-trino.png)

Nếu sau này snapshot cũ bị `expire_snapshots` xóa thì không đọc lại được nữa. Khi đó chỉ chụp kết quả bước 3, kèm
receipt trên EC2 (`/mnt/platform-data/airflow/logs/iceberg-maintenance/repartition-shopee.json`, có sẵn số
before/after).

**Warehouse.** DAG [dp4_warehouse](../../dags/dp4_warehouse.py), run `manual__2026-10-29T08:34:06.602356+00:00`,
gọi job [export và index](../../spark_jobs/warehouse/export_order_snapshot_lines.py). Job chép 30 ngày của năm
Channel, so số dòng từng Channel với Gold, rồi tạo index B-tree `(seller_sku)` và chạy `ANALYZE`.
Các ảnh dưới cho thấy vấn đề khi chưa có index: lọc một SKU hiếm vẫn phải quét cả bảng.

![Airflow dp4_warehouse attempt 3 màu xanh: log đối soát Gold và warehouse, đã ghi index seller_sku, rows 37.045.741](../evidence/aws/dp4-warehouse-log-20261029.png)

![Session Manager: thống kê SKU, bảng 5.513 MB, index 250 MB và Parallel Seq Scan sau DROP INDEX](../evidence/aws/warehouse-explain-before-1-20261029.png)

![EXPLAIN trước index: Parallel Seq Scan, APP-00482 mất 42.567,429 ms và ELE-01103 mất 41.744,754 ms](../evidence/aws/warehouse-explain-before-2-20261029.png)

![EXPLAIN sau CREATE INDEX và ANALYZE: APP-00482 dùng Bitmap Index Scan; ELE-01103 vẫn Parallel Seq Scan](../evidence/aws/warehouse-explain-after-20261029.png)

`APP-00482` có 4.517 trên tổng 37.045.741 dòng. Trước khi có index, PostgreSQL dùng Parallel Seq Scan, mất
42.567,429 ms và đọc 690.595 buffer. Sau khi có index, nó dùng Bitmap Index Scan rồi Bitmap Heap Scan, mất
1.497,187 ms và đọc 4.475 buffer.
Với SKU phổ biến `ELE-01103` (2.011.389 dòng), planner vẫn chọn Seq Scan, vì index không có lợi khi phải đọc phần
lớn các trang. Đây là các lượt EXPLAIN quan sát trên EBS; chưa tách riêng ảnh hưởng của cache để thành một benchmark
ổn định.

<details>
<summary>Tham khảo: thiết kế, hướng dẫn phát triển và ghi nhận trước triển khai AWS</summary>

Các ghi nhận local giữ nguyên môi trường, không dùng thay bằng chứng cloud.

## Data Storage — compaction Iceberg

Phần này ghi bằng chứng thực chạy cho `rewrite_data_files`
trên bảng Amazon Bronze toàn kỳ. Code đo và chạy: `spark_jobs/maintenance/inspect_iceberg_storage.py`
và `spark_jobs/maintenance/compact_amazon_order_snapshots.py`. Tài liệu API tương ứng là
[Apache Iceberg 1.11 Spark procedures](https://iceberg.apache.org/docs/1.11.0/spark-procedures/)
và [metadata table `.files`](https://iceberg.apache.org/docs/1.11.0/spark-queries/).

## Chọn bảng bằng metadata, không suy từ số file raw

`/data/raw` có 5.774 JSONL, nhưng sau khi Spark ghi Bronze/Silver, các bảng
DP1/DP2 đang có **một file trên mỗi partition**. Vì vậy chỉ đếm file nhỏ rồi
chạy compaction sẽ không giảm số file: Iceberg giữ nguyên ranh giới partition.

| Bảng | File | Partition | Partition có nhiều file |
|---|---:|---:|---:|
| `bronze.raw_amazon_orders_dp1` | 10 | 10 | 0 |
| `bronze.raw_walmart_orders_dp1` | 20 | 20 | 0 |
| `silver.stg_amazon_order_lines_dp2` | 28 | 28 | 0 |
| `silver.stg_walmart_order_lines_dp2` | 180 | 180 | 0 |
| `bronze.raw_amazon_orders` toàn kỳ, trước compaction | 187 | 180 | 7 |

Bảy ngày Amazon có hai file là 2026-02-23, 05-07, 05-16, 05-18, 06-11,
06-23 và 06-28. Truy vấn đo chỉ quét bảy ngày này:

```sql
SELECT count(*) AS rows, sum(length(payload)) AS payload_chars
FROM lakehouse.bronze.raw_amazon_orders
WHERE export_date IN (
  DATE '2026-02-23', DATE '2026-05-07', DATE '2026-05-16',
  DATE '2026-05-18', DATE '2026-06-11', DATE '2026-06-23',
  DATE '2026-06-28'
);
```

`sum(length(payload))` buộc Spark đọc cột dữ liệu, thay vì trả `count(*)`
từ metadata. Mỗi phía có một lượt làm nóng rồi ba lượt đo cùng câu SQL.

## Kết quả thực chạy

Lệnh `CALL lakehouse.system.rewrite_data_files` dùng bin-pack,
`target-file-size-bytes=134217728`, `min-input-files=2`, và chỉ một nhóm
rewrite đồng thời để vừa RAM laptop. Lệnh báo **12 file được viết lại thành 6
file**, 311.825.996 byte đầu vào, 0 file thất bại, mất 16,088 giây.

| Chỉ số | Trước | Sau |
|---|---:|---:|
| File hiện hành toàn bảng | 187 | 181 |
| Dòng toàn bảng | 108.248.563 | 108.248.563 |
| Byte file hiện hành toàn bảng | 8.525.005.070 | 8.524.986.535 |
| Dòng của truy vấn bảy ngày | 5.757.639 | 5.757.639 |
| Tổng ký tự payload của truy vấn | 4.381.958.279 | 4.381.958.279 |
| Ba lượt quét, giây | 10,511 / 10,348 / 9,451 | 9,641 / 9,405 / 9,066 |
| Trung vị, giây | **10,348** | **9,405** |

Trung vị giảm 0,943 giây, khoảng **9,1%** trên máy này. Số dòng và tổng ký tự
payload trùng khớp trước/sau. Lượt đo sau phải chạy trong Spark session mới vì
phiên đầu bị ngắt sau khi Iceberg đã commit compaction; do đó số giây là quan
sát thực nghiệm, không tách riêng được tác động của cache, tải máy và startup.
Một partition (`2026-06-28`) vẫn có hai file, nên hiện có 181 file thay vì 180.

## PostgreSQL warehouse và index

`spark_jobs/warehouse/export_order_snapshot_lines.py` đã sao chép lát Walmart của
`lakehouse.gold.fact_order_lines_dp2` sang
`sources.warehouse.fact_order_lines_walmart_dp2`. Bảng có **1.875.607** dòng,
khoảng `created_at` từ 2026-01-01 đến 2026-06-29; trước index chiếm **253 MB**.
Chỉ giữ sáu cột cần cho truy vấn để vừa đĩa máy dev. Đây là bản sao của Gold,
không phải một fact đầy đủ năm Channel trong PostgreSQL.

Cùng truy vấn, SKU `FTW-00664` có 211 dòng và tổng tiền net 12.038,74 theo
đồng tiền nguồn:

```sql
SELECT count(*), sum(line_net_amount)
FROM warehouse.fact_order_lines_walmart_dp2
WHERE seller_sku = 'FTW-00664';
```

DDL tạo index dùng cho phép đo dưới đây:

```sql
CREATE INDEX idx_fact_order_lines_walmart_sku
  ON warehouse.fact_order_lines_walmart_dp2 (seller_sku);
```

| `EXPLAIN (ANALYZE, BUFFERS)` | Trước index | Sau index |
|---|---|---|
| Kế hoạch | `Parallel Seq Scan` | `Bitmap Index Scan` → `Bitmap Heap Scan` |
| Dòng lọc / đọc | 211 / 1.875.607 | 211 / 211 khóa index |
| Buffer | hit 15.704, read 16.635 | read 213 |
| `Execution Time` | **51,611 ms** | **1,134 ms** |

Index `idx_fact_order_lines_walmart_sku` trên `(seller_sku)` chiếm **13 MB**;
bảng cùng index là **266 MB**. Đây là hai lần `EXPLAIN ANALYZE` trên cùng máy,
không phải benchmark nhiều lượt; cache có thể khác nhau giữa hai lần. Bằng chứng
mạnh nhất là kế hoạch đã đổi từ quét toàn bảng sang dùng index, và số buffer
đọc giảm rõ rệt.

## Phạm vi phép đo

Tài liệu này ghi compaction Iceberg và chỉ mục PostgreSQL. Phép đối chiếu cùng
câu hỏi giữa Iceberg và warehouse bằng Trino được ghi riêng tại
[trino.md](reference/trino.md); z-order chưa được đo trong bộ bằng chứng hiện có.

</details>

[Mục lục phần này](README.md) · [Mục lục chung](../README.md)
