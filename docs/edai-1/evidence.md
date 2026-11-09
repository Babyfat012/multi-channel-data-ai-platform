# Bằng chứng Data Platform trên AWS

Các kết quả dưới đây thuộc các lần chạy AWS, không xác nhận dịch vụ đang bật.

## Trên AWS (cloud)

Sydney `ap-southeast-2`, EC2 `c6i.2xlarge` + S3, Spark 2 worker × 2 core, quyền S3 qua IAM role (không access key).
Receipt: [data-platform-aws-20261027.json](../evidence/aws/data-platform-aws-20261027.json).

| Thành phần | Kết quả đã ghi nhận | Evidence | Giới hạn |
|---|---|---|---|
| Raw trên S3 | 17.393 object / 141.774.479.087 byte, khớp từng byte đĩa EC2; phần nguồn Amazon + Walmart + SKU = 100.012.790.412 byte, khớp manifest local | [Receipt](../evidence/aws/data-platform-aws-20261027.json) | Ba Channel thêm (TikTok Shop, Shopify, Shopee) chỉ có manifest EC2 đối chiếu |
| Nguồn Walmart (PostgreSQL) | `walmart.orders` 10.949.058 dòng, `seller.skus` 2.000, nạp 443 s | [Receipt](../evidence/aws/data-platform-aws-20261027.json) | |
| Bronze Amazon 100 GB | 108.248.563 dòng, 180 ngày, đọc `s3a://`, 506,6 s | [Receipt](../evidence/aws/data-platform-aws-20261027.json) | Chạy tay bằng `spark-submit`; bản trong DAG ở dòng dưới |
| Bronze Walmart | 10.949.058 dòng qua JDBC, tự so nguồn khớp, 267,2 s | [Receipt](../evidence/aws/data-platform-aws-20261027.json) | Chạy tay bằng `spark-submit` |
| Airflow `dp1_bronze` | 10/10 task success (5 Channel ingest + validate, đủ 180 ngày), ~55 phút; `validate_amazon`: nguồn 108.248.563 = Bronze 108.248.563, lỗi 0 | [Graph / ảnh và giải thích](pipeline_orchestration.md#trên-aws), [Spark master 2×2 core / ảnh và giải thích](processing_jobs.md#trên-aws-2026-10-29) | Ghi rồi mới kiểm (chưa phải branch WAP); `dp3_features` xem dòng dưới |
| Airflow `dp2_silver` | 14/14 task success; Summary ghi duration 1 giờ 58 phút (không phải thời gian đầu-cuối qua Clear/retry). Silver = Gold theo Channel: amazon 153.849.901, tiktok_shop 33.338.110, shopify 17.782.786, walmart 15.562.675, shopee 6.666.831 (Order Line, nên nhiều hơn số dòng Bronze). Gold invalid 0, trùng khoá Order Line 0. Amazon SCD2: 106.109.637 bản hiện hành + 1.062.966 bản lịch sử, lỗi 0 | [Summary / ảnh và giải thích](pipeline_orchestration.md#trên-aws) | `validate_walmart_silver` lần 1 hỏng vì ổ gốc đầy, chạy lại PASS; Summary chỉ ghi "success", lần hỏng chỉ thấy ở Gantt |
| Airflow `dp3_features` | 2/2 task success, 1 giờ 12 phút (build ~8 phút, validate ~64 phút). Feature 227.200.303 = Gold fact 227.200.303 (tổng 5 Channel dp2); missing 0, extra 0, trùng khoá 0, sai event time 0 | [Summary / ảnh và giải thích](pipeline_orchestration.md#trên-aws) | Validate so từng dòng với Gold nên lâu gấp ~8 lần build |
| Airflow `dp3_walmart_labels` | Sổ tra 15.562.675 dòng (= Silver/Gold Walmart) tạo từ ground truth seed 42, đọc từ S3; 12.165.007 Order Line có nhãn, trùng 0, lỗi 0 | log task (receipt `airflow_dp3_walmart_labels`) | Attempt 1 OOM exit 137 (worker hứa 3g > trần container), hạ `SPARK_WORKER_MEMORY` 2560m rồi PASS; dòng thiếu nhãn = chưa giao |
| Airflow `dp1_cdc` + Debezium trên EC2 | Debezium 3.4.3 đọc WAL Postgres (`snapshot.mode=no_data`) → Kafka `platform_cdc.walmart.orders` → Spark → `bronze.raw_walmart_cdc_v2`. Probe c/u/d: 731/452/295 ms commit→Kafka; Validate Kafka=3, Bronze=3, offset duy nhất 3, thiếu/thừa/lỗi 0 | [probe](../evidence/aws/cdc-probe-20261028.json), [log Validate / ảnh và giải thích](novel_ideas.md#trên-aws-1), [lineage / ảnh và giải thích](data_governance.md#trên-aws), receipt `airflow_dp1_cdc` | Chỉ 3 sự kiện probe, không đo tải; nguồn thiếu index nên probe chậm (gap #15) |
| Kafka + Flink 1.20.3 trên EC2 | Producer 41 s: Amazon 399.410, TikTok 93.554 (1.397 trùng) = local. 5 job FINISHED: baseline 399.410; dedup 92.157 (= unique_keys, cạnh HASH → Deduplicate, 93.554 record vào); tumbling 7.555 / hopping 37.870 cửa sổ; tin muộn 10.166 | [Completed Jobs / ảnh và giải thích](processing_jobs.md#trên-aws-2026-10-28), [manifest + số](../evidence/aws/stream-manifest-20261028.json) | Stream cỡ `default.yaml`, không 100 GB; Kafka volume trên ổ gốc; số tin muộn không tất định (local 9.143) |
| DataHub 1.7.0.1 trên EC2 | Publish từ log Airflow AWS, đủ 5 DAG (`--dags dp1_bronze dp1_cdc dp2_silver dp3_features dp3_walmart_labels`, `--raw-root s3a://…/source-exports`): 21 dataset, 29 task, 15 assertion PASS. Lineage S3/Postgres/Kafka → Bronze → Silver → Gold → feature, nhãn | [lineage Bronze / ảnh và giải thích](data_governance.md#trên-aws) | Lineage khai tay (gap #3); UI lỗi menu/tìm kiếm (gap #14) |
| Airflow `maintenance_iceberg` | 3/3 task success (plan → execute → validate), bảng `bronze.raw_shopee_orders_dp1`. Plan `no_op`: chỉ kiểm kê orphan (dry-run, `prefix_listing` qua S3FileIO). Trước = sau: 4.689.957 dòng, 180 file, 461.051.515 byte, 1 snapshot, fingerprint trùng | [Receipt](../evidence/aws/data-platform-aws-20261027.json) (`airflow_maintenance_iceberg`) | Bảng 1 file/partition ngày (~2,6 MB/file) nên compaction không có gì để gộp; partition theo ngày cho kênh nhỏ là gap riêng |
| Airflow `ml_feature_materialize` (Gold A → Feast) | 4/4 task success, run `manual__2026-10-29T00:30:43+00:00`: `export_sku_daily` + `export_training_lines` ghi Parquet `s3://<lakehouse>/feature-store/` (training_lines: mẫu 1% theo hash, nhãn có trước cutoff 2026-06-30); `feast_apply` dùng IAM role EC2 (log: `Found credentials from IAM Role: platform-data-platform-vm`); `materialize_incremental` nạp `sku_return_daily` vào Redis trên EKS (NodePort 30379) | [Log feast_apply / ảnh và giải thích](../edai-2/feature_store.md#trên-aws-1) | Gantt thấy 2 lần hỏng đã sửa: `export_training_lines` (cột `run_id` trùng giữa fact và nhãn), `feast_apply` (overlay AWS thiếu mount `feature_repo`). Tại run ngày 29, view `sku_return_24h` rỗng; cập nhật [stream A ngày 30](../edai-2/stream_features.md#trên-aws-pipeline-a-2026-10-30) |
| ML trên EKS: Kubeflow → MLflow → KServe | KFP 2.17.2 trên EKS: run `train-aws-3` (`0485ebd9…`) 5/5 bước, 10:42; đọc `training_lines` S3 qua Pod Identity `kubeflow/pipeline-runner`, image ECR `platform-train:9d49807-m3b`. MLflow trên EC2 (artifact S3, gọi qua IP riêng :5050): `return-risk` v1 alias `production`, PR-AUC model ≈ 0,25 vs baseline ≈ 0,25 (`beats_baseline`=1, lift 2,25, prevalence 0,11, test 16.411 dòng). KServe v0.21.0 RawDeployment: `return-risk` READY, image ECR `platform-return-risk:v1-9d49807-m4a-55e51c6b`; gọi `/v1/models/return-risk:predict` SKU `APP-00042` → score 0,166, `abstain=false` (Feast online: registry S3 qua Pod Identity `ml-serving/return-risk` + Redis EKS) | [KFP runs / ảnh và giải thích](../edai-2/training_pipeline.md#trên-aws) | 4 run KFP hỏng trước (OOM với 1% mẫu; Feast file store nối mọi ảnh chụp ngày → gọi theo lô 20k; port-forward chưa sẵn). Mẫu train 2‰ (359.457 dòng). Model thắng baseline rất sát; view stream 24h rỗng tại run train ngày 29 (xem cập nhật stream A ngày 30). Cache KFP tắt (EKS không ký CSR) |
| Airflow `dp4_warehouse` + index PostgreSQL | 30 ngày năm Channel → `warehouse.fact_order_lines_30d_dp2`: 37.045.741 dòng, 5.513 MB, đối soát theo Channel khớp Gold rồi mới index. EXPLAIN SKU hiếm `APP-00482`: Seq Scan 42.567 ms → Bitmap Index Scan 1.497 ms (buffer 690.595 → 4.475); SKU phổ biến `ELE-01103` (5,4%) planner vẫn chọn Seq Scan | [Phân tích](data_storage.md#warehouse-trên-aws-30-ngày-năm-channel), [log DAG / ảnh và giải thích](data_storage.md#trên-aws) | Ghi overwrite không atomic (gap #19); 2 lần hỏng trước: thiếu credential Postgres, thiếu schema `warehouse` |
| Novel #1: Trino liên nguồn trên EC2 | Một câu SQL đọc Iceberg Gold (S3, IAM role) và PostgreSQL warehouse: SKU `APP-00482` 30 ngày = 4.517 dòng / 49.632.312,55 ở **cả hai** nguồn | [trino.md](novel_ideas.md#trên-aws), [ảnh query / ảnh và giải thích](novel_ideas.md#trên-aws), catalog [`trino/catalog-aws/lakehouse.properties`](../../trino/catalog-aws/lakehouse.properties) | Trino chỉ bật khi đo (profile `query`) |
| Lakehouse optimize: partition evolution + compaction | `bronze.raw_shopee_orders_dp1`: ngày → tháng + rewrite-all: 180 → 6 file (2,44 → 73,13 MB), `count(*)` 1,04 → 0,29 s, lọc 30 ngày 7,94 → 0,97 s; 4.689.957 dòng, fingerprint giống hệt | [Phân tích](data_storage.md#lakehouse-trên-aws-gộp-file-nhỏ-bằng-partition-evolution), [job](../../spark_jobs/maintenance/repartition_small_channel.py) | Chạy lại dp1 trả bảng về chia theo ngày (gap #16 mới đóng một phần) |
| Spark handle skew trên AWS (Spark UI) | 20 ngày Amazon từ S3, AQE tắt: `hash` TB 91,1 s (nhiễu 5,6%), 20 file 47,3 MB; `distribution-mode=none` 71,6 s (−21,4%), 309 file 3,1 MB. Stage ghi `hash`: median 2 ms/0 dòng, max 12 s/2.173.164 dòng (180 task rỗng) | [spark.md 5g](processing_jobs.md#g-đo-lại-trên-aws-2026-10-29-ec2-r6ixlarge-4-vcpu-nguồn-s3a), [summary stage / ảnh và giải thích](processing_jobs.md#trên-aws-2026-10-29) | Giữ `hash` như local: không đổi 15× file nhỏ lấy 21% lúc ghi |
| DataHub 6 DAG (dp1 → dp4) | Publish lại từ log Airflow AWS: **22 dataset, 30 job, 16 assertion PASS** ([manifest](../evidence/aws/datahub-validation-aws-20261029.json)); `dp3_return_labels` 179.436.428 Order Line có nhãn năm Channel; `dp4_warehouse` lineage Iceberg Gold → PostgreSQL `warehouse.fact_order_lines_30d_dp2` + assertion đối soát theo Channel | [Lineage warehouse / ảnh và giải thích](data_governance.md#trên-aws) | UI sidebar lỗi (gap #14), vào thẳng URL; tài khoản mặc định (gap #23) |
| Vault trên EC2 + Ingress HTTPS trên EKS | Vault 2.1.1 (EBS `/mnt/platform-data/vault`, loopback 8200): init/unseal, KV v2, mật khẩu UI sinh và cất ở `secret/platform/{agent-ui,ops-ui}`. F5 NGINX Ingress 5.6.3 sau NLB public `15.134.182.157` (`*.15-134-182-157.sslip.io`); cert Let's Encrypt thật (issuer YR2, hết hạn 2027-01-28) cho `api`, `agent`, `grafana`, `metrics`, `traces`. Từ laptop: `http://` → 301 `https://`; `grafana`/`metrics` không mật khẩu → 401; `ssl_verify=0` | [Grafana HTTPS / ảnh và giải thích](#hạ-tầng-và-quan-sát-aws), receipt `wave2_ingress_vault_observability` | Lần cấp đầu mở route ~1–2 phút (bỏ basic auth + redirect); gia hạn ngày 60 của route có mật khẩu bị 401 (gap #24). Vault unseal tay sau mỗi lần EC2 khởi động. Mốc receipt wave2 trước khi có Feature API; cập nhật API/telemetry ngày 30 tại [Observability EKS](../edai-2/observability.md#trên-eks). Receipt này chưa đo rate limit NGINX UI |
| Observability trên EKS + cảnh báo EC2 | Prometheus, Loki + Fluent Bit, Jaeger, Grafana (6 pod Running, Loki PVC `gp3`). Prometheus **pull** exporter EC2 qua IP riêng (SG chỉ mở 9100/9187 từ SG cluster): `up` ec2-node = 1, ec2-postgres = 1, `pg_up` = 1, ổ `/mnt/platform-data` 41,9%, 1 replication slot. 3 luật `EC2ExporterDown` / `EC2DataDiskAlmostFull` (> 80%) / `PostgresSlotWalRetained` (> 1 GiB) health ok, inactive | [Targets / ảnh và giải thích](#hạ-tầng-và-quan-sát-aws), [values AWS](../../infra/k8s/observability/prometheus-values-aws.yaml) | Không Alertmanager: chỉ thấy khi mở trang Alerts. Ảnh wave2 mới thấy trace Jaeger; telemetry ứng dụng ngày 30 xem [Observability EKS](../edai-2/observability.md#trên-eks) |
| EKS `platform-apps` | Kubernetes 1.37, 2 node `c6i.xlarge` Ready; EBS CSI + Pod Identity, StorageClass `gp3` mặc định, PVC ghi/đọc đạt; Redis Feast chạy, EC2 → Redis NodePort 30379 `PONG` từ cả hai node | [PVC smoke / ảnh và giải thích](#hạ-tầng-và-quan-sát-aws) | Mốc bootstrap chỉ có Redis; KFP/KServe ở dòng ML trên EKS, agent/LLM ngày 30 ở [Agents EKS](../edai-2/agents.md) |

## Đọc bằng chứng AWS theo rubric

Mỗi doc dưới nhúng ảnh đúng chủ đề, giải thích phép đo và link code. Các mục ghi thiếu capture chỉ giữ
receipt/giới hạn; không tự chuyển thành kết luận đủ rubric.

| Rubric | Doc AWS |
|---|---|
| DE3–4 Docker | [Docker: runtime và đo kích thước image EC2](docker.md#trên-aws) |
| DE5–8 batch generator | [Thiết kế](data_generator.md#trên-aws-2026-10-27-đến-2026-10-28), [batch: volume và đặc tính đo trên AWS](data_generator.md#trên-aws) |
| DE9–10 stream generator | [Stream: late, duplicate và manifest](data_generator.md#trên-aws-1) |
| DE11–15 Spark | [Spark UI và tích hợp DAG](processing_jobs.md#trên-aws-2026-10-29), [Silver: Spark UI AWS Union schema cũ/mới và Window dedup (04/11)](processing_jobs.md#spark-ui-aws-union-schema-cũmới-và-window-dedup-silver-amazon) |
| DE16–19 Flink | [Baseline, dedup, window, late arrival; 04/11 bốn job chạy liên tục RUNNING](processing_jobs.md#flink-baseline-late-arrival-dedup-và-window); chưa có UI Flink AWS mở operator watermark |
| DE20–21 Storage | [Compaction receipt và EXPLAIN nhúng](data_storage.md#trên-aws), [maintenance no-op](reference/iceberg_maintenance.md#trên-aws) |
| DE22–27 orchestration | [Airflow DP1/DP2/DP3 và từng component](pipeline_orchestration.md#trên-aws) |
| DE28–33 governance | [DataHub bảng, lineage, assertion và run_id](data_governance.md#trên-aws) |
| DE34–38 schema | [Schema: zones, DESCRIBE SCD2/feature](schema_design.md#trên-aws), [ảnh DBeaver ER có đường nối trên Trino AWS (04/11)](schema_design.md#ảnh-dbeaver-nối-trino-runtime), [Gold validation](reference/gold.md#trên-aws), [quan hệ thiết kế](reference/gold_design.md#trên-aws) |
| DE39 Trino | [SQL hai catalog và UI FINISHED](novel_ideas.md#trên-aws) |
| DE40 CDC | [Probe c/u/d, validator và lineage](novel_ideas.md#trên-aws-1) |
| MLE11 Feast | [DAG materialize và IAM role](../edai-2/feature_store.md#trên-aws-1) |
| MLE13–14 training/versioning | [KFP 5 bước, MLflow model v1, ảnh Tags/Artifacts và đối chiếu SHA-256 train/test (04/11)](../edai-2/training_pipeline.md#trên-aws) |

### Hạ tầng và quan sát AWS

Các capture này bổ sung môi trường cho bảng tổng hợp: PVC EKS đọc/ghi, EC2 tới Redis,
UI Grafana/Jaeger và Prometheus pull exporter EC2. Cấu hình [StorageClass gp3](../../infra/k8s/storage-aws.yaml)
và [Prometheus AWS](../../infra/k8s/observability/prometheus-values-aws.yaml).
Ảnh Jaeger chỉ có service jaeger; không dùng thay trace ứng dụng. Ảnh Alerts chỉ thấy luật inactive,
chưa chứng minh gửi thông báo. Telemetry mới hơn có ở [Observability EKS](../edai-2/observability.md#trên-eks).

![kubectl: Pod Identity và EBS CSI rollout thành công, gp3 mặc định, pvc-smoke Bound và log đọc lại chuỗi ok](../evidence/aws/eks-ebs-gp3-pvc-smoke-20261027.png)

![Session Manager EC2: redis-cli tới hai địa chỉ node ở cổng 30379 đều trả PONG](../evidence/aws/ec2-to-eks-redis-ping-20261028.png)

![Grafana trang Dashboards: thư mục PLATFORM có A/B LLM + Agent và Agents, LLM, MCP](../evidence/aws/grafana-dashboards-https-20261030.png)

![Jaeger Search: service jaeger, 10 trace và các span api/services, GET](../evidence/aws/jaeger-search-https-20261030.png)

![Prometheus Target health: ec2-node và ec2-postgres đều UP, mỗi pool 1/1](../evidence/aws/prometheus-targets-ec2-20261030.png)

![Prometheus Alerts: EC2ExporterDown, EC2DataDiskAlmostFull và PostgresSlotWalRetained trong nhóm INACTIVE (3)](../evidence/aws/prometheus-alerts-ec2-20261030.png)

<details>
<summary>Tham khảo: thiết kế, hướng dẫn phát triển và ghi nhận trước triển khai AWS</summary>

Các ghi nhận local giữ nguyên môi trường, không dùng thay bằng chứng cloud.

## Data Platform — evidence

Bằng chứng chạy trên AWS đứng trước; bằng chứng local giữ nguyên bên dưới làm đối chứng. Mỗi phép đo có phạm vi dữ liệu và môi trường riêng; không suy từ fixture sang full-scale hoặc từ local sang AWS.

## Local

Receipt, log và ảnh gốc giữ nguyên nội dung, timestamp và định danh của môi trường đã chạy. Tên tài nguyên trong artifact lịch sử có thể khác cấu hình hiện hành dùng tiền tố `platform`; đổi tên cấu hình không phải là chạy lại phép đo.

### Nguồn và xử lý dữ liệu

| Thành phần | Kết quả đã ghi nhận | Evidence / mô tả phép đo | Giới hạn |
|---|---|---|---|
| Docker | So sánh cùng 27 package: naive 2,39 GB → optimized 698 MB | [So sánh image](../evidence/docker-compare-20261019.json), [inventory local](../evidence/docker-inventory-local-20261019.json), [phương pháp](docker.md) | Inventory overlay GCP lịch sử cũng được thu local; không chứng minh deploy cloud |
| Generator batch v1 | 100.012.790.412 byte raw từ Amazon, Walmart và SKU; duplicate 1,98% / 1,97% | [Manifest](../evidence/batch-100gb-manifest.json), [báo cáo batch](data_generator.md) | Ba Channel còn lại dùng nguồn nhỏ; không phải 100 GB v2 |
| Generator stream v1 | 492.964 message; late arrival, out-of-order, duplicate và burst | [Báo cáo stream](data_generator.md) | Phạm vi cấu hình nhỏ, tách khỏi batch 100 GB |
| Nguồn lifecycle v2 | 60.000 Order, 97.890 line, 1.546.066.142 byte; Spark reconcile trên 2.000 Order | [Scale](reference/source_order_lifecycle_scale.md), [fixture](reference/source_order_lifecycle_fixtures.md) | Chưa đạt 100 GB v2 |
| Bronze v2 | 550 bản ghi: 547 file + 3 JDBC, khớp manifest | [Receipt Bronze](../evidence/bronze-v2-jdbc-20261018.json) | Fixture local |
| Silver v2 | 19/19 check có sai lệch bằng 0; giữ revision và chặn conflict | [Receipt Silver](../evidence/silver-v2-receipt-20261018.json), [xử lý Silver](reference/silver_order_lifecycle.md) | Receipt sạch có duplicate_rows_dropped=0; không thay phép đo duplicate v1 |
| Spark v1 | Baseline/AQE, skew, schema evolution, dedup; execution 3: 8.449.302 → 8.314.312 dòng | [Báo cáo Spark](processing_jobs.md), [event log và ảnh UI](../evidence/spark-ui/README.md) | Các run có cửa sổ dữ liệu khác nhau; không suy thành benchmark v2 |
| Spark v2 | Có workload baseline/salted và kiểm bag equality | [Workload và protocol](reference/spark_order_lifecycle.md) | Chưa có phép đo hiệu năng baseline/salted v2 |
| Flink v1 | Watermark, late arrival, keyed dedup, tumbling/sliding window | [Báo cáo Flink](processing_jobs.md), [state metrics](../evidence/flink-dedup-state.json) | Kết quả gắn với job/offset ghi trong báo cáo |
| Stream lifecycle v2 | Hai epoch, restart/restore checkpoint; kept 15.261 khớp oracle | [Receipt stream](../evidence/stream-v2-local-20261019.json), [protocol](reference/stream_order_lifecycle.md) | Đếm sự kiện bounded; không phải HA hay số liệu kinh doanh chính thức |
| CDC lifecycle v2 | Insert/update/delete khớp oracle; ca extra bị chặn | [Receipt CDC](../evidence/cdc-v2-local-20261019.json), [protocol](reference/cdc_order_lifecycle.md) | Harness local, chưa chứng minh live DAG v2 |

### Storage, điều phối và governance

| Thành phần | Kết quả đã ghi nhận | Evidence / mô tả phép đo | Giới hạn |
|---|---|---|---|
| Iceberg v1 | Compaction 187 → 181 file; truy vấn bảy ngày có trung vị 10,348 → 9,405 s | [Storage](data_storage.md) | Theo bảng và lần chạy đã ghi |
| Warehouse v1 | 1.875.607 dòng Walmart; một lượt EXPLAIN 51,611 → 1,134 ms sau index | [Storage](data_storage.md) | Không phải phép đo trên toàn bộ Gold năm Channel |
| Federation | Gold và warehouse cùng SKU: 211 dòng, net 12.038,74 | [SQL và kết quả](novel_ideas.md), [đối chứng](reference/data_integration.md) | Một SKU Walmart, không kết luận truy vấn liên nguồn luôn nhanh hơn |
| Airflow v1 | DP1/DP2/DP3 có Graph/Grid/log; runtime configuration smoke thành công 2/2 task | [Airflow](pipeline_orchestration.md), [receipt runtime](../evidence/airflow-runtime-20261015.json) | Smoke cấu hình không ghi lại bảng nghiệp vụ |
| DataHub v1 | Lineage và assertion trích từ run Validate | [DataHub](data_governance.md), [manifest](../evidence/datahub-validation.json) | Assertion lịch sử, không xác nhận mọi run sau đều tự publish |
| Airflow / DataHub v2 | DAG và dry-run metadata có sẵn | [Airflow v2](reference/airflow_order_lifecycle.md), [DataHub v2](reference/datahub_order_lifecycle.md) | Chưa có receipt live DAG v2 và ảnh lineage/quality tương ứng |
| Data contracts | Infer/diff/gate bằng fixture Python; mô tả kiểm Spark 32/32 local | [Fixture receipt](../evidence/de-n1-contracts-20261018.json), [data contracts](reference/data_contracts.md) | Receipt Python không phải bằng chứng Spark |
| Iceberg maintenance | Spark 3/3 trên bảng demo; kiểm compact/zorder, dữ liệu và tag | [Receipt maintenance](../evidence/de-n2-maintenance-local-20261018.json), [cơ chế](reference/iceberg_maintenance.md) | Chưa có lịch maintenance và phép đo trên bảng nghiệp vụ thật |

Các kết quả Gold/e2e có log chỉ lưu trong `out/` được ghi riêng tại [trạng thái lifecycle](reference/data_platform_order_lifecycle_status.md), [Gold](reference/gold_order_lifecycle.md) và [e2e local](reference/e2e_order_lifecycle_local.md). Những con số đó là ghi nhận lịch sử có giới hạn kiểm chứng công khai, không được xem như receipt đính kèm repo.

## Kiểm chứng còn thiếu

- E2e medium sau các bản sửa OOM; phép đo skew v2 với cùng input.
- Live DAG lifecycle, publish/capture DataHub cùng generation và schema inventory đã deploy.
- Dữ liệu v2 100 GB, EXPLAIN trên scale tương ứng và maintenance bảng nghiệp vụ thật.
- Runtime AWS đã có receipt và ảnh theo bảng trên. [Runbook EC2 + S3](../shared/aws_deploy.md) là hướng dẫn tái tạo; riêng Terraform validate hoặc inventory local không thay bằng chứng runtime.

[Mục lục tài liệu](../README.md)

</details>

[Mục lục phần này](../README.md) · [Mục lục chung](../../README.md)
