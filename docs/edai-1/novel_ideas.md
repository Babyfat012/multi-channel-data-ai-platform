# Novel Ideas — Trino và CDC trên AWS

EDAI-1, dòng 39–40: ý tưởng, triển khai và proof hoạt động. Trino đối chiếu Iceberg S3/PostgreSQL; Debezium giữ create/update/delete qua Kafka.

## Trên AWS (2026-10-29)

Trino 483 chạy trên EC2 (profile `query`, loopback 8083), catalog [`trino/catalog-aws/lakehouse.properties`](../../trino/catalog-aws/lakehouse.properties):
`lakehouse` = Iceberg REST (Lakekeeper) + S3 thật qua IAM role của EC2 (không access key), `postgres` =
PostgreSQL EC2. Một câu SQL đọc cùng lúc Gold trên S3 và warehouse 30 ngày trên PostgreSQL:

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

Hai nguồn khớp số dòng và tổng tiền: vừa là truy vấn liên nguồn, vừa là phép kiểm chéo rằng
`dp4_warehouse` chép đúng 30 ngày từ Gold.

Query `20261029_092844_00002_zfrx9`: FINISHED, 15,50 s (planning 571 ms), 3 stage; stage 1 đọc Iceberg
(7,18M dòng quét, 27,3 s scheduled trên 1 worker).

![Trino Cluster overview: một worker, query UNION ALL hai nguồn FINISHED](../evidence/aws/trino-queries-20261029.png)

![Trino query 20261029_092844_00002_zfrx9 FINISHED, elapsed 15,50 giây](../evidence/aws/trino-federation-overview-20261029.png)

![Trino: SQL đọc lakehouse.gold và postgres.warehouse, cả ba stage FINISHED](../evidence/aws/trino-federation-stages-20261029.png)

Ý tưởng là kiểm chéo Gold S3 với warehouse PostgreSQL trong một câu SQL, giảm bước ghép kết quả thủ công.
`UNION ALL` giữ riêng kết quả từng nguồn; hai giá trị tổng hợp khớp không chứng minh toàn bộ từng dòng bằng nhau.
Số 4.517 và 49.632.312,55 lấy từ [receipt](../evidence/aws/data-platform-aws-20261027.json)
(`novel_trino_federation`); ảnh UI cho thấy SQL hai catalog và FINISHED, không hiển thị bảng kết quả CLI.
Catalog PostgreSQL: [postgres.properties](../../trino/catalog-aws/postgres.properties).

## Trên AWS (2026-10-28)

Debezium 3.4.3, PostgreSQL, Kafka, Airflow và Spark chạy Compose trên EC2 Sydney; Bronze Iceberg ghi S3.
Ý tưởng: bắt WAL để giữ create/update/delete sau lần nạp batch, không phải quét lại toàn bảng nguồn.
[DAG dp1_cdc](../../dags/dp1_cdc.py) run `manual__2026-10-28T10:51:33.422898+00:00`:
[ingest_walmart_cdc](../../spark_jobs/cdc/ingest_walmart_order_changes.py) đóng băng offset và ghi envelope/provenance;
[validate_walmart_cdc](../../spark_jobs/cdc/validate_walmart_order_changes.py) so Kafka/Bronze trong đúng source epoch.
Cấu hình connector: [setup_order_snapshots_debezium.py](../../scripts/setup/setup_order_snapshots_debezium.py).

[Probe AWS](../evidence/aws/cdc-probe-20261028.json) `CDC-DEMO-fb4b96588a6d` ghi topic
`platform_cdc.walmart.orders`, partition 0, offsets 0/1/2 tương ứng `c/u/d`.
Commit → Kafka lần lượt 730,7 / 452,1 / 295,3 ms; create có after, update có before/after, delete có before.
Sau delete nguồn còn 0 dòng probe. Không dùng các số này làm độ trễ commit → Bronze.
Validator ghi Kafka = Bronze = unique offset 3; thiếu, thừa, sai payload và lỗi đều 0.

![Airflow validate_walmart_cdc: Kafka 3, Bronze 3, offset duy nhất 3; thiếu, thừa, sai payload và lỗi đều 0](../evidence/aws/dp1-cdc-validate-20261028.png)

![DataHub CDC: Kafka orders qua ingest_walmart_cdc tới raw_walmart_cdc_v2 và validate_walmart_cdc](../evidence/aws/datahub/dp1-cdc-lineage.png)

![DataHub Quality raw_walmart_cdc_v2: assertion dp1_cdc.validate_walmart_cdc có dấu xanh](../evidence/aws/datahub/dp1-cdc-validation.png)

Lineage cho thấy topic qua task ingest tới `bronze.raw_walmart_cdc_v2`; Quality ghi assertion validator xanh.
[Manifest AWS](../evidence/aws/datahub-validation-aws-20261029.json) giữ cùng run_id và metrics.
Đây là probe ba sự kiện, chưa phải đo tải CDC hoặc chứng minh cập nhật Silver/Gold tự động.

[Mục lục phần này](README.md) · [Mục lục chung](../README.md)
