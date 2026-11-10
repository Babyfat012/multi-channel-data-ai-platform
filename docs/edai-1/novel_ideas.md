# Novel Ideas — Trino và CDC trên AWS

Trang này ứng với EDAI-1, dòng 39–40 của rubric: mô tả ý tưởng, cách triển khai và bằng chứng chạy được. Có hai ý
tưởng. Thứ nhất, dùng Trino để đối chiếu dữ liệu giữa Iceberg trên S3 và PostgreSQL trong một câu SQL. Thứ hai, dùng
Debezium để giữ đủ các thay đổi create/update/delete của nguồn và chuyển chúng qua Kafka.

## Trên AWS

**Ý tưởng 1: truy vấn liên nguồn bằng Trino.** Trino 483 chạy trên EC2 (profile `query`, chỉ nghe loopback cổng
8083). Catalog được khai trong [`trino/catalog-aws/lakehouse.properties`](../../trino/catalog-aws/lakehouse.properties):

- `lakehouse`: Iceberg REST (Lakekeeper) cùng S3 thật, truy cập qua IAM role của EC2 (không dùng access key).
- `postgres`: PostgreSQL trên EC2.

Câu SQL dưới đọc cùng lúc bảng Gold trên S3 và bảng warehouse 30 ngày trên PostgreSQL:

```sql
SELECT 'iceberg' AS nguon, count(*) AS dong, sum(line_net_amount) AS tien
FROM lakehouse.gold.fact_order_lines_dp2
WHERE seller_sku = 'APP-00482'
  AND created_at >= TIMESTAMP '2026-05-31 00:00:00 UTC' AND created_at < TIMESTAMP '2026-06-30 00:00:00 UTC'
UNION ALL
SELECT 'postgres', count(*), sum(line_net_amount)
FROM postgres.warehouse.fact_order_lines_30d_dp2
WHERE seller_sku = 'APP-00482'
```

| nguon | dong | tien |
|---|---|---|
| iceberg | 4.517 | 49.632.312,55 |
| postgres | 4.517 | 49.632.312,55 |

Hai nguồn khớp cả số dòng lẫn tổng tiền. Kết quả này có hai ý nghĩa: đây là một truy vấn liên nguồn chạy được, đồng
thời là phép kiểm chéo cho thấy DAG `dp4_warehouse` đã chép đúng 30 ngày từ Gold sang PostgreSQL.

Query `20261029_092844_00002_zfrx9` kết thúc ở trạng thái FINISHED sau 15,50 s (lập kế hoạch mất 571 ms), gồm 3
stage. Stage 1 đọc Iceberg: quét 7,18M dòng, tổng thời gian xử lý (scheduled) 27,3 s trên 1 worker.

![Trino Cluster overview: một worker, query UNION ALL hai nguồn FINISHED](../evidence/aws/trino-queries-20261029.png)

![Trino query 20261029_092844_00002_zfrx9 FINISHED, elapsed 15,50 giây](../evidence/aws/trino-federation-overview-20261029.png)

![Trino: SQL đọc lakehouse.gold và postgres.warehouse, cả ba stage FINISHED](../evidence/aws/trino-federation-stages-20261029.png)

Lợi ích: kiểm chéo Gold trên S3 với warehouse PostgreSQL chỉ bằng một câu SQL, bớt bước ghép kết quả bằng tay.

Giới hạn và nguồn số:

- `UNION ALL` giữ riêng kết quả của từng nguồn. Hai con số tổng hợp khớp nhau chưa chứng minh từng dòng đều bằng nhau.
- Số 4.517 và 49.632.312,55 lấy từ [receipt](../evidence/aws/data-platform-aws-20261027.json)
  (`novel_trino_federation`). Ảnh UI chỉ cho thấy SQL đọc hai catalog và trạng thái FINISHED, không hiển thị bảng
  kết quả CLI.
- Catalog PostgreSQL: [postgres.properties](../../trino/catalog-aws/postgres.properties).

## Trên AWS

**Ý tưởng 2: CDC (Change Data Capture, bắt thay đổi) bằng Debezium.** Debezium 3.4.3, PostgreSQL, Kafka, Airflow và
Spark chạy bằng Compose trên EC2 ở Sydney; bảng Bronze Iceberg ghi lên S3. Ý tưởng là đọc WAL (nhật ký ghi của
PostgreSQL) để giữ đủ create/update/delete xảy ra sau lần nạp batch, thay vì phải quét lại toàn bảng nguồn.

DAG [dp1_cdc](../../dags/dp1_cdc.py), run `manual__2026-10-28T10:51:33.422898+00:00`, có hai bước:

1. [ingest_walmart_cdc](../../spark_jobs/cdc/ingest_walmart_order_changes.py) chốt cố định khoảng offset sẽ đọc, rồi
   ghi envelope (bản tin thay đổi đầy đủ) cùng provenance (thông tin nguồn gốc) vào Bronze.
2. [validate_walmart_cdc](../../spark_jobs/cdc/validate_walmart_order_changes.py) so Kafka với Bronze, chỉ trong đúng
   source epoch (lượt nguồn) của run đó.

Cấu hình connector: [setup_order_snapshots_debezium.py](../../scripts/setup/setup_order_snapshots_debezium.py).

Kết quả [probe AWS](../evidence/aws/cdc-probe-20261028.json) `CDC-DEMO-fb4b96588a6d` (probe là ba sự kiện thử ghi
vào nguồn):

- Các sự kiện vào topic `platform_cdc.walmart.orders`, partition 0, offsets 0/1/2 tương ứng create/update/delete
  (`c/u/d`).
- Thời gian từ lúc commit ở nguồn tới lúc có trên Kafka lần lượt là 730,7 / 452,1 / 295,3 ms.
- Nội dung đúng loại sự kiện: create có bản after, update có cả before lẫn after, delete có bản before.
- Sau khi delete, nguồn còn 0 dòng probe.
- Validator ghi: số tin trên Kafka bằng số dòng Bronze bằng số offset duy nhất, đều là 3. Số tin thiếu, thừa, sai
  payload và lỗi đều bằng 0.

Không dùng các số thời gian trên làm độ trễ từ commit tới Bronze.

![Airflow validate_walmart_cdc: Kafka 3, Bronze 3, offset duy nhất 3; thiếu, thừa, sai payload và lỗi đều 0](../evidence/aws/dp1-cdc-validate-20261028.png)

![DataHub CDC: Kafka orders qua ingest_walmart_cdc tới raw_walmart_cdc_v2 và validate_walmart_cdc](../evidence/aws/datahub/dp1-cdc-lineage.png)

![DataHub Quality raw_walmart_cdc_v2: assertion dp1_cdc.validate_walmart_cdc có dấu xanh](../evidence/aws/datahub/dp1-cdc-validation.png)

Ảnh Lineage cho thấy dữ liệu đi từ topic, qua task ingest, tới `bronze.raw_walmart_cdc_v2`. Ảnh Quality cho thấy
assertion của validator có dấu xanh. [Manifest AWS](../evidence/aws/datahub-validation-aws-20261029.json) giữ cùng
run_id và các số đo. Giới hạn: đây chỉ là probe ba sự kiện, chưa phải phép đo tải CDC, và chưa chứng minh Silver/Gold
được cập nhật tự động.

[Mục lục phần này](README.md) · [Mục lục chung](../README.md)
