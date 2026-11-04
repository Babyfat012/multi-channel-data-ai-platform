# Processing Jobs trên AWS

EDAI-1, dòng 11–19: Spark xử lý offline và Flink xử lý streaming. Spark/Flink chạy Compose EC2, lakehouse ở S3. Đọc kết quả cùng baseline, cấu hình, đánh đổi và giới hạn ảnh AWS.

<a id="g-đo-lại-trên-aws-2026-10-29-ec2-r6ixlarge-4-vcpu-nguồn-s3a"></a>
## Spark: benchmark trên EC2, 29/10/2026

Đo trên EC2 bằng `PLATFORM_STORAGE=s3 bash scripts/evidence/measure_order_snapshots_bronze.sh
skew_0|skew_b` trên EC2 (driver trong container Airflow, Spark 2 worker × 2 core, đọc raw S3 qua IAM role):
20 ngày mẫu, AQE tắt, 12.627.457 dòng mỗi lần.

| Lần chạy | "XONG sau" | Số file | Cỡ file TB |
|---|---|---|---|
| đối chứng `hash` (lần 1/2/3) | 93,8 / 88,7 / 90,7 s → **TB 91,1 s**, nhiễu 5,1 s (5,6%) | 20 | 47,3 MB |
| **(b)** `distribution-mode=none` | **71,6 s** (−19,5 s = **−21,4%**, ~4× nhiễu) | **309** | **3,1 MB** |

Spark UI (History Server EC2): với `hash`, job ghi có **2 stage** — stage 0 đọc 9,4 GiB / 114 task (44 s) và
xáo 2,2 GiB; stage 1 nhận 2,2 GiB / 200 task (31 s), ghi 945,4 MiB. Summary Metrics stage 1: **median 2 ms /
0 dòng, max 12 s / 2.173.164 dòng** — 180 task rỗng vì chỉ có 20 ngày, task của ngày đông nhất ôm 2,17 tr
dòng . Với `none` chỉ **1 stage** / 114 task (1,0 phút): bỏ hẳn shuffle nên nhanh hơn, đổi lại
mỗi task ghi file riêng cho mọi ngày nó gặp ⇒ 309 file nhỏ.

Quyết định triển khai: **giữ `hash`** — nhanh hơn 21% lúc ghi không bù được
15× số file nhỏ cho mọi lần đọc sau (xem compaction ở [storage.md](data_storage.md)).
Ảnh AWS được nhúng ở [mục AWS](#trên-aws-2026-10-29).

<a id="trên-aws-2026-10-29"></a>
## Spark: tích hợp pipeline và Spark UI

Thí nghiệm skew chạy Compose trên EC2 `r6i.xlarge`, hai Spark worker × hai core, nguồn S3;
20 ngày Amazon, AQE tắt. [Benchmark EC2](#g-đo-lại-trên-aws-2026-10-29-ec2-r6ixlarge-4-vcpu-nguồn-s3a)
và [receipt](../evidence/aws/data-platform-aws-20261027.json) (`spark_skew_aws`) giữ số đo.
Code: [script đo](../../scripts/evidence/measure_order_snapshots_bronze.sh),
[job ingest](../../spark_jobs/bronze/ingest_amazon_order_snapshots.py).

UI baseline `hash` có 314 task ở hai stage. Stage ghi có median 2 ms/0 dòng nhưng max 12 s/2.173.164 dòng:
phần lớn partition hash rỗng, một số task chịu ngày nhiều dữ liệu. Đây là skew theo ngày ghi, không phải bằng chứng
skew SKU trong phép join. Bỏ phân phối hash bằng `write.distribution-mode=none` bỏ shuffle, còn 114 task/một stage.
Thời gian giảm từ trung bình 91,1 xuống 71,6 giây; file tăng 20 → 309. Giữ `hash` để tránh nhiều file nhỏ;
đổi spec cho kênh nhỏ được đo riêng ở [Storage AWS](data_storage.md#trên-aws-2026-10-29).

![Terminal EC2: ba run skew_0 mất 93,8, 88,7, 90,7 giây và skew_b mất 71,6 giây; mỗi run ghi 12.627.457 dòng](../evidence/aws/spark-skew-runs-20261029.png)

![Spark Jobs đối chứng hash: createOrReplace có 2 stage và 314 task thành công](../evidence/aws/spark-ui-skew0-jobs-20261029.png)

![Spark Stages hash: stage 0 có 114 task đọc 9,4 GiB, stage 1 có 200 task nhận shuffle 2,2 GiB](../evidence/aws/spark-ui-skew0-stages-20261029.png)

![Spark stage 1: median 2 ms và output 0 dòng; max 12 giây và output 2.173.164 dòng](../evidence/aws/spark-ui-skew0-stage1-summary-20261029.png)

![Spark Jobs none: createOrReplace hoàn tất một stage, 114 task, duration 1,0 phút](../evidence/aws/spark-ui-skewb-jobs-20261029.png)

Tích hợp: [dp1_bronze](../../dags/dp1_bronze.py) submit ingest rồi validate từng nguồn;
[dp2_silver](../../dags/dp2_silver.py) submit chuẩn hóa/dedup rồi Gold/SCD2;
[dp3_features](../../dags/dp3_features.py) submit build/validate feature.
[Airflow AWS](pipeline_orchestration.md#trên-aws-2026-10-27-đến-2026-10-28) nhúng Graph/Gantt/log và giải thích từng component.
Ảnh Master dưới thuộc DP1 ngày 2026-10-27; thí nghiệm skew ngày 29 ở trên chạy riêng bằng script,
không gán run_id của benchmark cho run DAG.

![Spark Master: hai worker ALIVE, mỗi worker hai core; ứng dụng dp1-validate-amazon của user airflow đang RUNNING](../evidence/aws/spark-master-2x2core-20261027.png)

![Airflow DP1: danh sách 10 task màu xanh và cạnh ingest nối validate trong Graph](../evidence/aws/dp1-bronze-graph-20261027.png)

DP2 AWS đã validate Silver/Gold không lỗi ([schema evolution và dedup](#trên-aws-2026-10-28)).
Spark UI AWS của job Silver Amazon (Union parse schema cũ/mới và Window dedup) nằm ở [mục Spark: schema evolution và dedup](#trên-aws-2026-10-28).
Ảnh **local** cùng job: [Union và Window dedup, schema details](reference/spark.md#10-proof-silver-schema-evolution-và-dedup-trong-execution-3) (Spark UI, nguồn Amazon).

<a id="trên-aws-2026-10-28"></a>
## Spark: schema evolution và dedup, 28/10/2026

DP2 chạy Spark Compose trên EC2, Bronze/Silver/Gold ở S3;
run `manual__2026-10-27T16:05:58+00:00` trong [receipt](../evidence/aws/data-platform-aws-20261027.json).
[Job Amazon](../../spark_jobs/silver/build_amazon_order_snapshots.py) parse hai schema rồi chuẩn hóa,
Window theo khóa giữ revision mới nhất; [DAG](../../dags/dp2_silver.py) đặt validator sau từng job Silver.

![Summary DP2: Total success 1, Total Tasks 14 và danh sách task Silver, Gold, SCD2 màu xanh](../evidence/aws/dp2-silver-summary-20261028.png)

![Session Manager EC2: log DP2 ghi Silver validate PASS, Gold khớp từng Channel và Amazon SCD2 không lỗi](../evidence/aws/dp2-validate-logs-20261028.png)

Receipt ghi Silver Amazon 153.849.901, Walmart 15.562.675, TikTok Shop 33.338.110,
Shopify 17.782.786, Shopee 6.666.831 dòng; Gold khớp từng kênh, lỗi và khóa Order Line trùng 0.
Ảnh trên chứng minh trạng thái DAG và kết quả validator. [Airflow AWS](pipeline_orchestration.md#trên-aws-2026-10-27-đến-2026-10-28).

### Spark UI AWS: Union schema cũ/mới và Window dedup (Silver Amazon)

Event log của task Amazon Silver trong DP2 run `manual__2026-10-27T16:05:58+00:00` (xem [receipt](../evidence/aws/data-platform-aws-20261027.json)): application
`app-20261027160606-0024`, tên `silver-amazon-stg_amazon_order_lines_dp2`, chạy 27/10/2026 16:06–16:58 UTC (khoảng 52 phút), đã hoàn tất. Mở lại bằng Spark History Server
trên EC2 (04/11/2026, không chạy lại job) và chụp bằng [`capture_spark_silver_evidence.py`](../../scripts/evidence/capture_spark_silver_evidence.py);
execution 3 là phần xử lý dữ liệu, [REST](../evidence/aws/silver-amazon-execution-3-aws.json) lưu `status=COMPLETED`, plan và metrics.

![Spark UI AWS, Query 3: hai nhánh BatchScan raw_amazon_orders_dp1 → Filter → Generate → Union → Sort → WindowGroupLimit → Window → Filter](../evidence/aws/12-silver-amazon-dedup-plan-aws.png)

Hai nhánh cùng đọc bảng Bronze, lọc và parse riêng từng schema rồi `Generate` nổ mảng Order Line, `Union`, sắp xếp và chia theo khóa, `WindowGroupLimit`/`Window` giữ bản `row_number = 1`.
Details của plan: nhánh schema cũ lọc `get_json_object(payload, $.AmazonOrderId)` và `from_json` với `AmazonOrderId`/`OrderItems`; nhánh schema 2026 lọc `$.orderId` và `from_json` với `orderId`/`orderItems`; hai nhánh cùng ánh xạ về một bộ cột Silver.

![Details của execution 3 trên AWS: Filter theo AmazonOrderId và orderId, from_json hai schema, ánh xạ về cột Silver](../evidence/aws/13-silver-amazon-schema-details-aws.png)

| Số đo execution 3 (AWS) | Số dòng |
|---|---:|
| Order Line từ nhánh schema cũ (`Generate`, node 18) | 76.365.986 |
| Order Line từ nhánh schema 2026 (`Generate`, node 26) | 80.584.967 |
| Trước dedup (`WindowGroupLimit`, node 12) | 156.948.256 |
| Sau dedup (`Filter`, node 6) | 153.849.901 |
| **Bản trùng đã bỏ** | **3.098.355 (1,974%)** |

Số sau dedup 153.849.901 khớp số Silver Amazon trong receipt. Tỷ lệ 1,974% khớp mức trùng offline của generator (1,98%).

<a id="flink-trên-aws-2026-10-28"></a>
## Flink: baseline, late arrival, dedup và window, 28/10/2026

Flink 1.20.3 và Kafka chạy Compose trên EC2; đầu ra bounded ở EBS `/mnt/platform-data/flink`.
[Manifest AWS](../evidence/aws/stream-manifest-20261028.json) ghi job_id, số đầu vào/đầu ra và cấu hình generator.
Ảnh Completed Jobs có baseline, late, dedup, state proof và windows đều FINISHED.

### Luồng xử lý stream tổng thể

![Luồng xử lý stream: Generator phát vào hai topic Kafka; bốn job Flink SQL (baseline, late arrival, dedup, window) ghi CSV](../images/flink/stream-flow.svg)

① Generator phát Amazon (có trễ và đảo thứ tự) và TikTok (có trùng ID) vào hai topic Kafka. ② Mỗi job SQL là một
consumer group riêng trên Flink 1.20.3 (JobManager + TaskManager, Compose EC2), tự giữ watermark/state của mình.
③ Đích là CSV trên EBS `/mnt/platform-data/flink`, đối chiếu với [manifest](../evidence/aws/stream-manifest-20261028.json).
Pipeline stream phục vụ feature store (Kafka → Flink HOP 24h → Redis/S3) là job riêng, xem [Streaming Feature](../edai-2/stream_features.md).

### Late arrival: tin muộn được xử lý thế nào

Amazon gửi notification không theo thứ tự thời gian. [`late_arrival.sql`](../../flink_jobs/order_snapshots/late_arrival.sql)
khai `WATERMARK FOR event_ts AS event_ts - INTERVAL '1' HOUR`: watermark của partition luôn đứng sau event time mới nhất đã thấy
đúng một giờ. Một tin có `event_ts <= CURRENT_WATERMARK(event_ts)` là tin đã bị watermark vượt qua nên được ghi riêng vào
`late-amazon-1ms`, kèm cột `watermark_time` để tự chứng minh. Ví dụ dòng thật trong đầu ra `late-amazon-live` (lần chạy 2026-11-04, AWS EC2, bản chạy liên tục):

| notification_id | event_time (xảy ra) | publish_time (đến) | watermark_time lúc xử lý | partition | Vì sao là late |
|---|---|---|---|---:|---|
| `qb8vznf1-5f5n-…` | 2026-06-30 01:24:05 | 2026-06-30 02:45:52 | 01:28:13 | 1 | event_time < watermark: tin này nằm sau watermark của partition 1 |
| `5i4wg38o-v0ee-…` | 2026-06-30 08:04:34 | 2026-06-30 12:42:02 | 11:34:14 | 1 | event_time < watermark, đến trễ hơn 3 giờ so với watermark |
| `b98ozk0b-uiw7-…` | 2026-06-30 17:25:59 | 2026-06-30 22:26:34 | 21:08:20 | 1 | event_time < watermark, đến trễ hơn 4 giờ so với watermark |

Cả **6.674** dòng của lần chạy này đều có `event_time <= watermark_time` (lệnh kiểm in `TONG 6674 dong, event_time<=watermark: 6674`).
Tin không muộn quá ngưỡng một giờ không vào file này, nên số dòng nhỏ hơn số tin out-of-order của generator (19.729).
Ảnh Completed Jobs/graph bên dưới chỉ chứng minh job chạy; **bằng chứng xử lý là cột `watermark_time` ở trên**.
Các dòng trên lấy từ `/mnt/platform-data/flink/late-amazon-live/part-*` trên EC2; xem [output nguyên văn](../evidence/aws/flink-live-output-20261104.txt) và mục Trạng thái job.

![Terminal EC2: ba dòng late arrival từ late-amazon-live, TONG 6674 dòng đều có event_time <= watermark, và số dòng các thư mục *-live](../evidence/aws/flink-late-arrival-live.png)

Lệnh lấy 3 dòng ví dụ và kiểm tra điều kiện late từ đầu ra (đã chạy trên EC2 ngày 04/11, kết quả ở bảng trên):

```bash
cat /mnt/platform-data/flink/late-amazon-live/part-* | tr -d '"' | awk -F, '{e=$2;sub("T"," ",e);sub("Z","",e);p=$3;sub("T"," ",p);sub("Z","",p);w=substr($4,1,19);t++;if(e<=w)ok++; if(t%80==1&&n<3){n++;printf "| `%s` | %s | %s | %s | %s |\n",$1,e,p,substr($4,12,8),$5}} END{printf "TONG %d dong, event_time<=watermark: %d\n",t,ok}'
```

**Trạng thái job.** Lần chạy 28/10 dùng `scan.bounded.mode = latest-offset` nên cả bốn job SQL trên chạy xong rồi `FINISHED`; các số
trong bảng dưới và ảnh Completed Jobs đều từ bản bounded đó. Sau đó bốn file SQL được sửa để chạy liên tục: bỏ `scan.bounded.mode`,
bật checkpoint ra EBS, file CSV đóng mỗi 30 s, đầu ra sang thư mục `*-live`, và TaskManager nâng lên 6 slot
([`docker-compose.aws-stream.yml`](../../docker-compose.aws-stream.yml)). `dedup_state_proof.sql` giữ bounded vì chỉ dùng để chụp execution plan.

**Chạy lại bản liên tục trên AWS, 04/11/2026.** Kafka trên EC2 đã mất hai topic của 28/10 (còn lại `ml.sku_return_24h` và `order_snapshots.return_events`), nên phát lại bằng generator
(`platform-generator:opt`, seed 42, `--speed 0`): **399.410** tin Amazon (132.222 + 133.332 + 133.856 qua 3 partition) và **93.554** tin TikTok (31.375 + 30.918 + 31.261), khớp từng tin với
[manifest 28/10](../evidence/aws/stream-manifest-20261028.json), nên các số trong bảng dưới vẫn so sánh được. Rồi submit bốn SQL bằng `sql-client.sh`; cả bốn job ở trạng thái **RUNNING**
(sau khoảng 5 phút: dedup-tiktok 2 task, baseline-amazon 1, window-tiktok 3, late-amazon 1), TaskManager 1 với 6 slot (2 slot còn trống), 0 job Finished/Canceled/Failed.

![Flink 1.20.3 Running Jobs trên AWS: flink-dedup-tiktok, flink-baseline-amazon, flink-window-tiktok, flink-late-amazon đều RUNNING](../evidence/aws/flink-running-jobs.png)

![Flink Overview trên AWS: 4 Running Jobs, Total Task Slots 6, Available 2, Finished 0, Canceled 0, Failed 0](../evidence/aws/flink-overview-4-running.png)

Số dòng đầu ra các thư mục `*-live` của lần chạy liên tục (đếm bằng `wc -l` trên EC2, [output nguyên văn](../evidence/aws/flink-live-output-20261104.txt)), so với manifest 28/10 (bản bounded):

| Thư mục | Dòng 04/11 (liên tục) | Manifest 28/10 (bounded) | Ghi chú |
|---|---:|---:|---|
| `baseline-amazon-live` | **399.410** | 399.410 | khớp |
| `unique-tiktok-live` | **92.157** | 92.157 | khớp (93.554 vào, bỏ 1.397 trùng) |
| `tiktok-tumble-5m-live` | 7.553 | 7.555 | thiếu 2 cửa sổ cuối: không còn watermark kết thúc nên cửa sổ cuối chưa đóng |
| `tiktok-hop-5m-1m-live` | 37.863 | 37.870 | thiếu 7 cửa sổ cuối, cùng lý do |
| `late-amazon-live` | 6.674 | 10.166 | không tất định: phụ thuộc thời điểm phát watermark theo partition |

Job ML `ml-sku-return-24h` chưa được submit lại trong buổi này (Running đã có ảnh ở [Streaming Feature](../edai-2/stream_features.md#trên-aws-pipeline-a-2026-10-30)).

| Bước / code | Vấn đề và cách xử lý | Số AWS từ manifest |
|---|---|---|
| [Baseline](../../flink_jobs/order_snapshots/baseline.sql) | Đọc Amazon bounded, chưa tách muộn | 399.410 dòng, job `ffd9de8a94c5bf67179482bfe03d3b59` |
| [Late arrival](../../flink_jobs/order_snapshots/late_arrival.sql) | Watermark event time trễ 1 giờ; lọc `event_ts <= CURRENT_WATERMARK(event_ts)` ra đích riêng | 10.166 dòng, job `b172bcf2510b47e1cbcd532f3078a82d` |
| [Dedup](../../flink_jobs/order_snapshots/dedup_webhooks.sql), [state proof](../../flink_jobs/order_snapshots/dedup_state_proof.sql) | TikTok trùng ID; `ROW_NUMBER` giữ bản đầu, cạnh HASH đưa cùng ID vào keyed Deduplicate | 93.554 vào → 92.157 ra, bỏ 1.397; state-proof job `f5b642f64b133e28ec0f57021f1eab22` |
| [Window SQL](../../flink_jobs/order_snapshots/windows.sql) | Đếm Order mới theo tumbling 5 phút và HOP 5 phút trượt 1 phút | 7.555 / 37.870 cửa sổ, job `dad323a91493cdf365c5a3d3d4dd0adf` |

![Flink 1.20.3 Completed Jobs: baseline Amazon, late Amazon, hai job dedup và job cửa sổ đều FINISHED](../evidence/aws/flink-completed-jobs-20261028.png)

![Flink dedup_tiktok_state_proof: cạnh HASH tới Deduplicate, source gửi 93.554 record, job FINISHED](../evidence/aws/flink-dedup-graph-20261028.png)

UI dedup thấy HASH → Deduplicate và 93.554 record nhận, chứng minh keyed operator đã chạy.
Số 92.157 dòng đầu ra lấy từ manifest, không đọc nhầm cột Records Sent của file sink bằng 0 thành mất dữ liệu.
Ảnh Completed Jobs (28/10) chỉ xác nhận job late FINISHED. Lần chạy liên tục 04/11 bổ sung ảnh terminal AWS lấy ba tin muộn và kiểm `event_time <= watermark` đúng 6.674/6.674 dòng ([ảnh](../evidence/aws/flink-late-arrival-live.png)); vẫn chưa có UI Flink AWS mở operator watermark. Ảnh **local** cùng job late: [watermark và tin muộn](reference/flink.md#bắt-tin-đến-muộn-bằng-watermark) (UI Flink local, 13–14/10).
10.166 không phải số tất định: watermark phát theo chu kỳ, khác phép đếm out-of-order toàn cục 19.729 của generator.
Window AWS dài hạn 24h trượt 1h có [ảnh operator/checkpoint riêng](../edai-2/stream_features.md#trên-aws-pipeline-a-2026-10-30).

[Mục lục phần này](README.md) · [Mục lục chung](../README.md)
