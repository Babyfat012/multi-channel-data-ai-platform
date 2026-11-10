# Bằng chứng Data Platform trên AWS

Trang này gom kết quả các lần chạy Data Platform trên AWS vào một bảng. Mỗi dòng cho biết thành phần nào đã chạy,
đo được gì, bằng chứng nằm ở đâu và còn giới hạn gì. Đây là kết quả của các lần chạy đã ghi lại; trang này không
khẳng định các dịch vụ hiện vẫn đang bật.

Một số từ dùng trong bảng:

- **Receipt** (biên lai): file JSON do script ghi ngay sau lần chạy, chứa số đo và định danh của run.
- **Bronze / Silver / Gold**: ba tầng của lakehouse. Bronze giữ dữ liệu gốc; Silver đã làm sạch và tách thành
  từng Order Line (một dòng hàng trong đơn); Gold là bảng fact dùng cho phân tích và tạo feature.
- **Channel**: kênh bán hàng (Amazon, Walmart, TikTok Shop, Shopify, Shopee).
- **gap #N**: một hạn chế đã biết, đánh số theo danh sách gap của dự án.

## Trên AWS (cloud)

Môi trường: vùng Sydney `ap-southeast-2`, một máy EC2 `c6i.2xlarge` cùng S3. Spark có 2 worker, mỗi worker 2 core.
Quyền đọc/ghi S3 cấp qua IAM role của máy (không dùng access key).
Receipt chung: [data-platform-aws-20261027.json](../evidence/aws/data-platform-aws-20261027.json).

| Thành phần | Kết quả đã ghi nhận | Evidence | Giới hạn |
|---|---|---|---|
| Raw trên S3 | Có 17.393 object, tổng 141.774.479.087 byte, khớp từng byte với dữ liệu trên đĩa EC2. Riêng phần nguồn Amazon, Walmart và SKU là 100.012.790.412 byte, khớp với manifest (danh sách file và kích thước) sinh ở local | [Receipt](../evidence/aws/data-platform-aws-20261027.json) | Ba Channel thêm (TikTok Shop, Shopify, Shopee) chỉ đối chiếu được với manifest trên EC2 |
| Nguồn Walmart (PostgreSQL) | Bảng `walmart.orders` có 10.949.058 dòng, `seller.skus` có 2.000 SKU. Nạp mất 443 s | [Receipt](../evidence/aws/data-platform-aws-20261027.json) | |
| Bronze Amazon 100 GB | Đọc dữ liệu từ S3 qua `s3a://` và ghi 108.248.563 dòng (180 ngày) vào Bronze, mất 506,6 s | [Receipt](../evidence/aws/data-platform-aws-20261027.json) | Chạy tay bằng `spark-submit`; bản chạy trong DAG Airflow ở dòng `dp1_bronze` bên dưới |
| Bronze Walmart | Đọc 10.949.058 dòng từ PostgreSQL qua JDBC. Job tự so số dòng với nguồn và thấy khớp. Mất 267,2 s | [Receipt](../evidence/aws/data-platform-aws-20261027.json) | Chạy tay bằng `spark-submit` |
| Airflow `dp1_bronze` | Cả 10/10 task thành công (ingest và validate cho 5 Channel), đủ 180 ngày, mất khoảng 55 phút. Task `validate_amazon`: nguồn có 108.248.563 dòng, Bronze cũng có 108.248.563 dòng, 0 lỗi | [Graph / ảnh và giải thích](pipeline_orchestration.md#trên-aws), [Spark master 2×2 core / ảnh và giải thích](processing_jobs.md#trên-aws-2026-10-29) | Dữ liệu được ghi trước rồi mới kiểm tra, chưa theo mô hình WAP (write-audit-publish: ghi vào branch riêng, kiểm xong mới công bố). `dp3_features` xem dòng dưới |
| Airflow `dp2_silver` | Cả 14/14 task thành công. Trang Summary ghi thời lượng 1 giờ 58 phút; con số này không tính thời gian thêm do Clear/retry. Số dòng Silver bằng Gold ở từng Channel: amazon 153.849.901, tiktok_shop 33.338.110, shopify 17.782.786, walmart 15.562.675, shopee 6.666.831. Đây là số Order Line nên lớn hơn số dòng Bronze (một đơn có thể có nhiều dòng hàng). Gold có 0 dòng invalid và 0 khoá Order Line bị trùng. Bảng Amazon SCD2 (Slowly Changing Dimension loại 2: giữ cả bản hiện hành lẫn các bản cũ của một đơn) có 106.109.637 bản hiện hành và 1.062.966 bản lịch sử, 0 lỗi | [Summary / ảnh và giải thích](pipeline_orchestration.md#trên-aws) | Task `validate_walmart_silver` hỏng ở lần 1 vì ổ gốc đầy, chạy lại thì PASS. Trang Summary chỉ ghi "success"; lần hỏng chỉ thấy trên biểu đồ Gantt |
| Airflow `dp3_features` | Cả 2/2 task thành công, mất 1 giờ 12 phút (build khoảng 8 phút, validate khoảng 64 phút). Bảng feature có 227.200.303 dòng, bằng bảng Gold fact 227.200.303 dòng (tổng 5 Channel của dp2). Không thiếu dòng (missing 0), không thừa dòng (extra 0), không trùng khoá, không dòng nào sai event time (thời điểm sự kiện) | [Summary / ảnh và giải thích](pipeline_orchestration.md#trên-aws) | Bước validate so từng dòng với Gold nên lâu gấp khoảng 8 lần bước build |
| Airflow `dp3_walmart_labels` | Tạo sổ tra nhãn 15.562.675 dòng (bằng số dòng Silver/Gold Walmart) từ ground truth (đáp án do generator sinh, seed 42), đọc từ S3. Có 12.165.007 Order Line được gắn nhãn; 0 trùng, 0 lỗi | log task (receipt `airflow_dp3_walmart_labels`) | Lần chạy 1 hết bộ nhớ (OOM, exit 137) vì worker xin 3g, vượt trần bộ nhớ của container. Hạ `SPARK_WORKER_MEMORY` xuống 2560m thì PASS. Dòng thiếu nhãn là đơn chưa giao |
| Airflow `dp1_cdc` + Debezium trên EC2 | Debezium 3.4.3 đọc WAL (nhật ký ghi của PostgreSQL) với `snapshot.mode=no_data` (không chụp lại dữ liệu cũ), đẩy thay đổi vào Kafka topic `platform_cdc.walmart.orders`, rồi Spark ghi vào `bronze.raw_walmart_cdc_v2`. Probe (ba sự kiện thử create/update/delete, viết tắt c/u/d) mất lần lượt 731/452/295 ms từ lúc commit tới lúc có trên Kafka. Task Validate: Kafka 3, Bronze 3, 3 offset duy nhất; thiếu, thừa, lỗi đều 0 | [probe](../evidence/aws/cdc-probe-20261028.json), [log Validate / ảnh và giải thích](trino_federation_and_cdc.md#trên-aws-1), [lineage / ảnh và giải thích](data_governance.md#trên-aws), receipt `airflow_dp1_cdc` | Chỉ có 3 sự kiện thử, chưa đo tải. Bảng nguồn thiếu index nên probe chậm (gap #15) |
| Kafka + Flink 1.20.3 trên EC2 | Producer phát xong trong 41 s: Amazon 399.410 tin, TikTok 93.554 tin (1.397 tin trùng), giống số chạy local. Cả 5 job Flink FINISHED. Job baseline đọc 399.410 tin. Job dedup (bỏ trùng) nhận 93.554 record qua cạnh HASH vào bước Deduplicate và giữ 92.157, đúng bằng số khoá duy nhất (unique_keys). Job cửa sổ ra 7.555 cửa sổ tumbling (cửa sổ nối tiếp, không chồng nhau) và 37.870 cửa sổ hopping (cửa sổ trượt, chồng lên nhau). Job late tách 10.166 tin muộn | [Completed Jobs / ảnh và giải thích](processing_jobs.md#trên-aws-2026-10-28), [manifest + số](../evidence/aws/stream-manifest-20261028.json) | Stream theo cỡ `default.yaml`, không phải 100 GB. Volume Kafka nằm trên ổ gốc. Số tin muộn thay đổi giữa các lần chạy (local ra 9.143) |
| DataHub 1.7.0.1 trên EC2 | Metadata được publish từ log Airflow trên AWS, đủ 5 DAG (`--dags dp1_bronze dp1_cdc dp2_silver dp3_features dp3_walmart_labels`, `--raw-root s3a://…/source-exports`): 21 dataset, 29 task, 15 assertion (luật kiểm tra chất lượng) PASS. Lineage (đường đi của dữ liệu) nối S3/PostgreSQL/Kafka tới Bronze, Silver, Gold rồi tới feature và nhãn | [lineage Bronze / ảnh và giải thích](data_governance.md#trên-aws) | Lineage được khai bằng tay trong script (gap #3). UI lỗi menu và tìm kiếm (gap #14) |
| Airflow `maintenance_iceberg` | Cả 3/3 task thành công (plan, execute, validate) trên bảng `bronze.raw_shopee_orders_dp1`. Bước plan trả `no_op` (không có gì cần làm): chỉ kiểm kê file mồ côi (orphan) ở chế độ chạy thử (dry-run, liệt kê bằng `prefix_listing` qua S3FileIO). Trước và sau giống nhau: 4.689.957 dòng, 180 file, 461.051.515 byte, 1 snapshot, fingerprint (mã băm nội dung bảng) trùng nhau | [Receipt](../evidence/aws/data-platform-aws-20261027.json) (`airflow_maintenance_iceberg`) | Bảng có 1 file cho mỗi partition ngày (~2,6 MB/file) nên compaction (gộp file nhỏ) không có gì để gộp. Việc chia partition theo ngày cho kênh nhỏ là một gap riêng |
| Airflow `ml_feature_materialize` (Gold A → Feast) | Cả 4/4 task thành công, run `manual__2026-10-29T00:30:43+00:00`. `export_sku_daily` và `export_training_lines` ghi Parquet vào `s3://<lakehouse>/feature-store/` (training_lines lấy mẫu 1% theo hash, chỉ dùng nhãn có trước cutoff 2026-06-30). `feast_apply` dùng IAM role của EC2 (log ghi `Found credentials from IAM Role: platform-data-platform-vm`). `materialize_incremental` nạp `sku_return_daily` vào Redis trên EKS (NodePort 30379) | [Log feast_apply / ảnh và giải thích](../ml-llm-platform/feature_store.md#trên-aws-1) | Biểu đồ Gantt cho thấy 2 lần hỏng đã sửa: `export_training_lines` hỏng vì cột `run_id` có ở cả bảng fact lẫn bảng nhãn; `feast_apply` hỏng vì overlay AWS thiếu mount `feature_repo`. Tại run ngày 29, view `sku_return_24h` còn rỗng; xem cập nhật ở [stream A ngày 30](../ml-llm-platform/stream_features.md#trên-aws-pipeline-a-2026-10-30) |
| ML trên EKS: Kubeflow → MLflow → KServe | Kubeflow Pipelines (KFP) 2.17.2 trên EKS: run `train-aws-3` (`0485ebd9…`) chạy đủ 5/5 bước trong 10:42. Run đọc `training_lines` trên S3 qua Pod Identity `kubeflow/pipeline-runner`, dùng image ECR `platform-train:9d49807-m3b`. MLflow chạy trên EC2 (artifact lưu ở S3, gọi qua IP riêng cổng 5050) và đăng ký `return-risk` v1 với alias `production`. PR-AUC của model ≈ 0,25, của baseline cũng ≈ 0,25 (`beats_baseline`=1, lift 2,25, prevalence 0,11, tập test 16.411 dòng). KServe v0.21.0 ở chế độ RawDeployment: `return-risk` READY, image ECR `platform-return-risk:v1-9d49807-m4a-55e51c6b`. Gọi `/v1/models/return-risk:predict` cho SKU `APP-00042` trả score 0,166, `abstain=false`. Feature online lấy từ Feast: registry trên S3 đọc qua Pod Identity `ml-serving/return-risk`, dữ liệu trong Redis trên EKS | [KFP runs / ảnh và giải thích](../ml-llm-platform/training_pipeline.md#trên-aws) | Trước đó có 4 run KFP hỏng: hết bộ nhớ (OOM) khi dùng mẫu 1%; Feast file store nối mọi ảnh chụp theo ngày nên phải chuyển sang gọi theo lô 20k; port-forward chưa sẵn sàng. Mẫu train cuối cùng là 2‰ (359.457 dòng). Model thắng baseline rất sát. View stream 24h còn rỗng lúc train (xem cập nhật stream A). Cache KFP bị tắt vì EKS không ký CSR (yêu cầu cấp chứng chỉ) |
| Airflow `dp4_warehouse` + index PostgreSQL | Chép 30 ngày của năm Channel vào `warehouse.fact_order_lines_30d_dp2`: 37.045.741 dòng, 5.513 MB. Số dòng theo từng Channel được đối soát khớp với Gold rồi mới tạo index. EXPLAIN với SKU hiếm `APP-00482`: không có index thì Seq Scan (quét cả bảng) mất 42.567 ms; có index thì Bitmap Index Scan mất 1.497 ms, số buffer (trang dữ liệu) phải đọc giảm từ 690.595 xuống 4.475. Với SKU phổ biến `ELE-01103` (5,4% số dòng), planner vẫn chọn Seq Scan | [Phân tích](data_storage.md#warehouse-trên-aws-30-ngày-năm-channel), [log DAG / ảnh và giải thích](data_storage.md#trên-aws) | Ghi đè (overwrite) không atomic (gap #19). Có 2 lần hỏng trước đó: thiếu credential Postgres, thiếu schema `warehouse` |
| Novel #1: Trino liên nguồn trên EC2 | Một câu SQL đọc cùng lúc Iceberg Gold (trên S3, qua IAM role) và warehouse PostgreSQL. SKU `APP-00482` trong 30 ngày có 4.517 dòng và tổng tiền 49.632.312,55 ở **cả hai** nguồn | [trino.md](trino_federation_and_cdc.md#trên-aws), [ảnh query / ảnh và giải thích](trino_federation_and_cdc.md#trên-aws), catalog [`trino/catalog-aws/lakehouse.properties`](../../trino/catalog-aws/lakehouse.properties) | Trino chỉ bật khi đo (profile `query`) |
| Lakehouse optimize: partition evolution + compaction | Bảng `bronze.raw_shopee_orders_dp1` đổi partition từ theo ngày sang theo tháng rồi viết lại toàn bộ (rewrite-all). Số file giảm từ 180 xuống 6 (cỡ file từ 2,44 MB lên 73,13 MB). `count(*)` giảm từ 1,04 s xuống 0,29 s; lọc 30 ngày giảm từ 7,94 s xuống 0,97 s. Bảng vẫn 4.689.957 dòng, fingerprint giống hệt | [Phân tích](data_storage.md#lakehouse-trên-aws-gộp-file-nhỏ-bằng-partition-evolution), [job](../../spark_jobs/maintenance/repartition_small_channel.py) | Chạy lại dp1 sẽ đưa bảng về chia theo ngày, nên gap #16 mới đóng một phần |
| Spark handle skew trên AWS (Spark UI) | Đọc 20 ngày Amazon từ S3, tắt AQE (Adaptive Query Execution: Spark tự điều chỉnh kế hoạch khi đang chạy). Chế độ ghi `hash`: trung bình 91,1 s (dao động 5,6%), ra 20 file, cỡ 47,3 MB. Chế độ `distribution-mode=none`: 71,6 s (nhanh hơn 21,4%) nhưng ra 309 file, cỡ 3,1 MB. Trong stage ghi của `hash`, task trung vị mất 2 ms và xử lý 0 dòng, task lớn nhất mất 12 s với 2.173.164 dòng (180 task rỗng) | [spark.md 5g](processing_jobs.md#g-đo-lại-trên-aws-2026-10-29-ec2-r6ixlarge-4-vcpu-nguồn-s3a), [summary stage / ảnh và giải thích](processing_jobs.md#trên-aws-2026-10-29) | Giữ `hash` như ở local: không chấp nhận số file nhỏ gấp 15 lần để đổi lấy 21% thời gian ghi |
| DataHub 6 DAG (dp1 → dp4) | Publish lại từ log Airflow AWS: **22 dataset, 30 job, 16 assertion PASS** ([manifest](../evidence/aws/datahub-validation-aws-20261029.json)). DAG `dp3_return_labels` có 179.436.428 Order Line được gắn nhãn trên năm Channel. Lineage của `dp4_warehouse` nối Iceberg Gold tới PostgreSQL `warehouse.fact_order_lines_30d_dp2`, kèm assertion đối soát theo Channel | [Lineage warehouse / ảnh và giải thích](data_governance.md#trên-aws) | Sidebar UI lỗi (gap #14) nên phải vào thẳng URL. Vẫn dùng tài khoản mặc định (gap #23) |
| Vault trên EC2 + Ingress HTTPS trên EKS | Vault 2.1.1 lưu dữ liệu trên EBS `/mnt/platform-data/vault`, chỉ nghe loopback cổng 8200. Đã init và unseal (khởi tạo, mở khoá), bật KV v2; mật khẩu UI được sinh và cất ở `secret/platform/{agent-ui,ops-ui}`. F5 NGINX Ingress 5.6.3 đứng sau NLB public `15.134.182.157` (tên miền `*.15-134-182-157.sslip.io`), dùng cert Let's Encrypt thật (issuer YR2, hết hạn 2027-01-28) cho `api`, `agent`, `grafana`, `metrics`, `traces`. Kiểm tra từ laptop: `http://` được chuyển hướng 301 sang `https://`; gọi `grafana`/`metrics` không kèm mật khẩu bị từ chối 401; `ssl_verify_result = 0` (biến `%{ssl_verify_result}` của curl; 0 nghĩa là cert xác thực thành công) | [Grafana HTTPS / ảnh và giải thích](#hạ-tầng-và-quan-sát-aws), receipt `wave2_ingress_vault_observability` | Lần cấp cert đầu phải mở route khoảng 1–2 phút (tạm bỏ basic auth và chuyển hướng). Lần gia hạn ở ngày 60 cho route có mật khẩu bị 401 (gap #24). Vault phải unseal bằng tay sau mỗi lần EC2 khởi động. Receipt wave2 được ghi trước khi có Feature API; số đo API/telemetry ngày 30 xem ở [Observability EKS](../ml-llm-platform/observability.md#trên-eks). Receipt này chưa đo rate limit của NGINX UI |
| Observability trên EKS + cảnh báo EC2 | Prometheus, Loki + Fluent Bit, Jaeger, Grafana chạy trên EKS (6 pod Running, Loki dùng PVC `gp3`). Prometheus **chủ động kéo (pull)** số đo từ exporter trên EC2 qua IP riêng; security group (SG) chỉ mở cổng 9100/9187 cho SG của cluster. Kết quả: `up` của ec2-node = 1, của ec2-postgres = 1, `pg_up` = 1, ổ `/mnt/platform-data` đã dùng 41,9%, có 1 replication slot. Ba luật cảnh báo `EC2ExporterDown`, `EC2DataDiskAlmostFull` (> 80%) và `PostgresSlotWalRetained` (> 1 GiB) đều health ok và đang inactive (chưa kích hoạt) | [Targets / ảnh và giải thích](#hạ-tầng-và-quan-sát-aws), [values AWS](../../infra/k8s/observability/prometheus-values-aws.yaml) | Không có Alertmanager nên cảnh báo chỉ thấy khi mở trang Alerts. Ảnh wave2 mới thấy trace của Jaeger; telemetry của ứng dụng xem [Observability EKS](../ml-llm-platform/observability.md#trên-eks) |
| EKS `platform-apps` | Kubernetes 1.37, 2 node `c6i.xlarge` Ready. Đã cài EBS CSI và Pod Identity; StorageClass `gp3` là mặc định; thử PVC ghi rồi đọc lại thành công. Redis cho Feast đang chạy; từ EC2 gọi Redis qua NodePort 30379 nhận `PONG` từ cả hai node | [PVC smoke / ảnh và giải thích](#hạ-tầng-và-quan-sát-aws) | Mốc bootstrap (lần dựng ban đầu) chỉ có Redis; KFP/KServe xem dòng ML trên EKS, agent/LLM xem [Agents EKS](../ml-llm-platform/agents.md) |

## Đọc bằng chứng AWS theo rubric

Bảng dưới chỉ từ mỗi nhóm tiêu chí rubric tới tài liệu AWS tương ứng. Mỗi tài liệu nhúng ảnh của đúng chủ đề,
giải thích phép đo và link tới code. Mục nào ghi là còn thiếu ảnh chụp thì chỉ giữ receipt và giới hạn; không tự
coi mục đó là đã đạt đủ rubric.

| Rubric | Doc AWS |
|---|---|
| DE3–4 Docker | [Docker: runtime và đo kích thước image EC2](docker.md#trên-aws) |
| DE5–8 batch generator | [Thiết kế](data_generator.md#trên-aws-2026-10-27-đến-2026-10-28), [batch: volume và đặc tính đo trên AWS](data_generator.md#trên-aws) |
| DE9–10 stream generator | [Stream: late, duplicate và manifest](data_generator.md#trên-aws-1) |
| DE11–15 Spark | [Spark UI và tích hợp DAG](processing_jobs.md#trên-aws-2026-10-29), [Silver: Spark UI AWS Union schema cũ/mới và Window dedup](processing_jobs.md#spark-ui-aws-union-schema-cũmới-và-window-dedup-silver-amazon) |
| DE16–19 Flink | [Baseline, dedup, window, late arrival; bốn job chạy liên tục RUNNING](processing_jobs.md#flink-baseline-late-arrival-dedup-và-window); chưa có ảnh UI Flink AWS mở operator watermark |
| DE20–21 Storage | [Compaction receipt và EXPLAIN nhúng](data_storage.md#trên-aws), [maintenance no-op](reference/iceberg_maintenance.md#trên-aws) |
| DE22–27 orchestration | [Airflow DP1/DP2/DP3 và từng component](pipeline_orchestration.md#trên-aws) |
| DE28–33 governance | [DataHub bảng, lineage, assertion và run_id](data_governance.md#trên-aws) |
| DE34–38 schema | [Schema: zones, DESCRIBE SCD2/feature](schema_design.md#trên-aws), [ảnh DBeaver ER có đường nối trên Trino AWS](schema_design.md#ảnh-dbeaver-nối-trino-runtime), [Gold validation](reference/gold.md#trên-aws), [quan hệ thiết kế](reference/gold_design.md#trên-aws) |
| DE39 Trino | [SQL hai catalog và UI FINISHED](trino_federation_and_cdc.md#trên-aws) |
| DE40 CDC | [Probe c/u/d, validator và lineage](trino_federation_and_cdc.md#trên-aws-1) |
| MLE11 Feast | [DAG materialize và IAM role](../ml-llm-platform/feature_store.md#trên-aws-1) |
| MLE13–14 training/versioning | [KFP 5 bước, MLflow model v1, ảnh Tags/Artifacts và đối chiếu SHA-256 train/test](../ml-llm-platform/training_pipeline.md#trên-aws) |

### Hạ tầng và quan sát AWS

Các ảnh dưới bổ sung bằng chứng về môi trường cho bảng tổng hợp ở trên:

- PVC trên EKS ghi và đọc được; EC2 gọi được Redis.
- Giao diện Grafana và Jaeger mở qua HTTPS.
- Prometheus kéo được số đo từ exporter trên EC2.

Cấu hình liên quan: [StorageClass gp3](../../infra/k8s/storage-aws.yaml) và
[Prometheus AWS](../../infra/k8s/observability/prometheus-values-aws.yaml).

Lưu ý khi đọc ảnh: ảnh Jaeger chỉ có trace của chính service jaeger, không dùng thay cho trace của ứng dụng. Ảnh
Alerts chỉ cho thấy các luật đang inactive, chưa chứng minh hệ thống gửi được thông báo. Telemetry mới hơn xem ở
[Observability EKS](../ml-llm-platform/observability.md#trên-eks).

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
| Federation | Gold và warehouse cùng SKU: 211 dòng, net 12.038,74 | [SQL và kết quả](trino_federation_and_cdc.md), [đối chứng](reference/data_integration.md) | Một SKU Walmart, không kết luận truy vấn liên nguồn luôn nhanh hơn |
| Airflow v1 | DP1/DP2/DP3 có Graph/Grid/log; runtime configuration smoke thành công 2/2 task | [Airflow](pipeline_orchestration.md), [receipt runtime](../evidence/airflow-runtime-20261015.json) | Smoke cấu hình không ghi lại bảng nghiệp vụ |
| DataHub v1 | Lineage và assertion trích từ run Validate | [DataHub](data_governance.md), [manifest](../evidence/datahub-validation.json) | Assertion lịch sử, không xác nhận mọi run sau đều tự publish |
| Airflow / DataHub v2 | DAG và dry-run metadata có sẵn | [Airflow v2](reference/airflow_order_lifecycle.md), [DataHub v2](reference/datahub_order_lifecycle.md) | Chưa có receipt live DAG v2 và ảnh lineage/quality tương ứng |
| Data contracts | Infer/diff/gate bằng fixture Python; mô tả kiểm Spark 32/32 local | [Fixture receipt](../evidence/contracts-validation-20261018.json), [data contracts](reference/data_contracts.md) | Receipt Python không phải bằng chứng Spark |
| Iceberg maintenance | Spark 3/3 trên bảng demo; kiểm compact/zorder, dữ liệu và tag | [Receipt maintenance](../evidence/iceberg-maintenance-local-20261018.json), [cơ chế](reference/iceberg_maintenance.md) | Chưa có lịch maintenance và phép đo trên bảng nghiệp vụ thật |

Các kết quả Gold/e2e có log chỉ lưu trong `out/` được ghi riêng tại [trạng thái lifecycle](reference/data_platform_order_lifecycle_status.md), [Gold](reference/gold_order_lifecycle.md) và [e2e local](reference/e2e_order_lifecycle_local.md). Những con số đó là ghi nhận lịch sử có giới hạn kiểm chứng công khai, không được xem như receipt đính kèm repo.

## Kiểm chứng còn thiếu

- E2e medium sau các bản sửa OOM; phép đo skew v2 với cùng input.
- Live DAG lifecycle, publish/capture DataHub cùng generation và schema inventory đã deploy.
- Dữ liệu v2 100 GB, EXPLAIN trên scale tương ứng và maintenance bảng nghiệp vụ thật.
- Runtime AWS đã có receipt và ảnh theo bảng trên. [Runbook EC2 + S3](../shared/aws_deploy.md) là hướng dẫn tái tạo; riêng Terraform validate hoặc inventory local không thay bằng chứng runtime.

[Mục lục tài liệu](../README.md)

</details>

[Mục lục phần này](README.md) · [Mục lục chung](../README.md)
