# Tiến độ Data Platform scope mở rộng (v2) — bảng một trang

> **Pipeline v2 (order lifecycle) — thiết kế và thử ở local, không phải pipeline đã nộp.** Pipeline chạy trên AWS là v1 (DP1 → DP4 + ML), xem [Data Platform evidence](../evidence.md). Các chỗ nhắc GCP dưới đây là kế hoạch cũ, chưa chạy; đích cloud hiện hành là [AWS](../../shared/aws_deploy.md).

Bằng chứng gồm kết quả local có receipt không đính kèm repo.
Tổng hợp artifact công khai tại [evidence](../evidence.md).
Đích triển khai: **AWS**, region `ap-southeast-1`, Data Platform batch trên EC2 + S3 (người vận hành chạy theo [runbook](../../shared/aws_deploy.md)).
"Local" = đã kiểm trên máy dev, fixture/harness/container tạm; **chưa** phải bằng chứng AWS.
Một gói có kiểm local không đồng nghĩa đã kiểm chứng ở full-scale.

| Thành phần | Tài liệu | Local | AWS |
|---|---|---|---|
| Nguồn v2 fixture 5 Channel | [silver-v2](silver_order_lifecycle.md) | ✅ fixture 15 Order / 30 line | ⏳ |
| Nguồn v2 scale (60k đơn ≈ 1,55 GB / 7 phút) | [source-v2-scale](source_order_lifecycle_scale.md) | ✅ sinh 60k; Spark 3/3 trên 2.000 đơn (200 × 10), reconcile = 0; chưa ≥100 GB | ⏳ |
| Bronze/Silver v2 + đối soát | [silver-v2](silver_order_lifecycle.md), [Bronze](../../evidence/bronze-v2-jdbc-20261018.json), [Silver](../../evidence/silver-v2-receipt-20261018.json) | ✅ fixture: Bronze 550 = manifest; Silver 19/19 check = 0 | ⏳ |
| Data contract + cổng cách ly | [data contracts](data_contracts.md) | ✅ Spark 32/32 sau sửa identity_path; registry file Git | ⏳ |
| Iceberg maintenance | [Iceberg maintenance](iceberg_maintenance.md), [evidence](../../evidence/iceberg-maintenance-local-20261018.json) | ✅ Spark 3/3 trên bảng demo; chưa chạy lịch/bảng nghiệp vụ thật | ⏳ bảng thật |
| Nhãn Return v2 (L01–L17) + Gold + feature PIT | [gold-v2](gold_order_lifecycle.md) | ✅ Spark fixture 5/5; Gold chỉ đọc Silver history, đã sửa OOM keyless | ⏳ |
| Gold-equivalence sau refactor | [gold-v2](gold_order_lifecycle.md) | ✅ diff fact/nhãn/feature = 0 fixture và nguồn scale 60 ngày, trước/sau đổi tên; feature PIT có tín hiệu sau khi generator phát coverage theo tuần (ADR-0013): ca Gold 60 ngày có **131 line** `labeled_count_90d > 0`, Spark khớp Python thuần | ⏳ |
| Warehouse v2 PostgreSQL, publish an toàn | [warehouse-v2](warehouse_order_lifecycle.md) | ✅ PostgreSQL thật; warehouse-checks PASS 116 s trước đổi tên; chưa kết luận lợi ích index scale | ⏳ EXPLAIN scale |
| DAG dp_order_lifecycle / dp_order_lifecycle_cdc / dp_order_lifecycle_warehouse + DataHub dry-run + ERD | [airflow-v2](airflow_order_lifecycle.md), [datahub-v2](datahub_order_lifecycle.md), [ERD](../../diagrams/schema-order-lifecycle.md) | ✅ DagBag 10 DAG / 0 lỗi; dry-run không phải publish thật | ⏳ |
| Nguồn → Bronze → Silver → Gold → PostgreSQL → EXPLAIN | [e2e-v2-local](e2e_order_lifecycle_local.md) | ✅ small 45 ngày × 50 đơn, 100 SKU, 3.631 fact: PASS 92 s sau sửa và đổi tên; mọi check = 0 | ⏳ |
| Chuỗi đầu-cuối trên nguồn medium | [e2e-v2-local](e2e_order_lifecycle_local.md) | ⏳ chưa chạy lại; trước sửa: 500 × 60, 48.874 fact, Silver 2.090 s, gold_build 42 s, gold_validate OOM | ⏳ nghiệm thu medium theo [runbook AWS](../../shared/aws_deploy.md#pipeline); harness local giữ làm tham chiếu |
| Baseline / salted, cùng input và bag equality | [spark-v2](spark_order_lifecycle.md) | ⏳ chưa đo, chưa có kết luận giảm skew | ⏳ đo S3 theo [runbook AWS](../../shared/aws_deploy.md#do); không dùng nguyên lệnh container MinIO local |
| Chạy thật DAG v2 + publish/capture DataHub v2 | [airflow-v2](airflow_order_lifecycle.md), [datahub-v2](datahub_order_lifecycle.md) | ⏳ chưa chạy DAG/publish/capture; chưa có receipt/Graph/Grid/Lineage/Quality | ⏳ pipeline → DataHub → capture theo [runbook AWS](../../shared/aws_deploy.md#pipeline) |
| Terraform + Ansible + overlay S3 | [IaC](../../ml-llm-platform/iac.md), [runbook AWS](../../shared/aws_deploy.md), rà soát offline | ✅ kiểm offline gói AWS; chưa triển khai Cloud | ⏳ người vận hành |
| Stream v2 Kafka → Flink + oracle | [stream-v2](stream_order_lifecycle.md), [evidence](../../evidence/stream-v2-local-20261019.json) | ✅ harness 2/2 epoch + restart; không phải HA hay official sales/label | — |
| CDC v2 trên walmart_lifecycle.orders | [cdc-v2](cdc_order_lifecycle.md), [evidence](../../evidence/cdc-v2-local-20261019.json) | ✅ harness insert/update/delete + tiêm extra bị chặn | — |
| Inventory + so naive/opt cùng bộ thư viện | [Docker](../docker.md), [so sánh](../../evidence/docker-compare-20261019.json), [inventory local](../../evidence/docker-inventory-local-20261019.json), [overlay GCP lưu trữ](../../evidence/docker-inventory-gcp-20261019.json) | ✅ 27 package giống, 2,39 GB → 698 MB; cả inventory overlay được thu local | — |

✅ đã kiểm trong phạm vi ghi · ⏳ chờ chạy · 🔨 đang chạy · — ngoài mốc 1 AWS batch

## Số đo và giới hạn

Gold v2 từng **OOM ở small 3.631 line** vì coverage keyless bị nhân bản theo line.
Đã sửa; **e2e small PASS 92 s** sau đổi tên, `gold_build` **7,6 s**, mọi check **= 0**;
chi tiết và EXPLAIN tại [e2e local](e2e_order_lifecycle_local.md). Q3 giảm **104 → 4** buffer,
Q2 **583 → 583**; Q1 **173 → 2** nhưng **0 dòng**, không chứng minh lợi ích index.
Gold-equivalence fact/nhãn/feature **diff = 0** trên fixture và nguồn scale **60 ngày**,
cả trước và sau đổi tên; feature từng luôn **= 0**; sau ADR-0013 ca Gold 60 ngày có **131 line** feature > 0, khớp Python thuần.
**warehouse-checks PASS 116 s trước đổi tên**; **DagBag 10 DAG / 0 lỗi** gồm
`dp_order_lifecycle`, `dp_order_lifecycle_cdc`, `dp_order_lifecycle_warehouse` và bảy DAG snapshots giữ tên,
chỉ là kiểm import. Pytest unit do người vận hành xác nhận: **718 pass**.
Medium chưa chạy lại sau ba bản sửa Gold keyless, oracle validate gom theo SKU và
Silver `collect_set`; kết quả OOM trong bảng là lần trước sửa.

Các số trên được ghi nhận từ log local; receipt ở `out/` không commit,
chỉ ghi số, không dẫn link receipt. Không suy từ small/fixture sang medium,
≥100 GB hoặc AWS; không dùng thời lượng warehouse-checks làm số cải thiện index.

## Còn mở

- **Đo trên AWS:** theo [runbook](../../shared/aws_deploy.md#pipeline), chạy lại e2e medium sau sửa, đo skew v2, chạy thật DAG v2 +
  gửi/capture DataHub v2 bằng các lệnh trong bảng trên. Người vận hành chọn bỏ các bước
  lâu ở local; chưa có số đo/ảnh.
- AWS fixture → scale, receipt pipeline/warehouse và EXPLAIN cùng input theo
  [runbook](../../shared/aws_deploy.md#pipeline); DataHub chạy local cần receipt đúng generation AWS.
- Nguồn v2 **≥100 GB** trên AWS, sau nghiệm thu scale nhỏ; không đổi số
  100 GB nguồn v1 thành bằng chứng v2 hoặc cộng volume nhiều tầng.
- Iceberg maintenance trên **bảng nghiệp vụ thật**, lịch maintenance/retention và kiểm sau chạy;
  Spark 3/3 hiện mới bao phủ bảng demo local.
- Ảnh/schema inventory bảng v2 đã deploy, ảnh Spark UI v2 và proof
  dimension SCD2/quan hệ dimension–fact v2 còn thiếu. Inventory Docker
  local và overlay đã có; overlay không thay inventory runtime của EC2 AWS.
