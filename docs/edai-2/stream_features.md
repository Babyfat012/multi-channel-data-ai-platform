# Streaming Feature trên AWS

EDAI-2, dòng 12: push streaming feature vào offline và online store.

Code / cấu hình: [Flink SQL](../../flink_jobs/ml/sku_return_feature_a.sql), [pusher](../../feature_repo/push_stream_features.py).

Các kết quả dưới đây thuộc các lần chạy AWS được ghi ngày, không xác nhận dịch vụ đang bật.

<a id="trên-aws-pipeline-a-2026-10-30"></a>
## Streaming trên AWS, 30–31/10/2026

Stream đơn hàng đã triển khai (thông báo Amazon, webhook TikTok) không có sự kiện Return, chỉ có trạng thái đơn.
Nên trên AWS phát **lại** Return đã duyệt từ Gold vào Kafka, theo đúng thứ tự thời điểm biết Return (giới hạn:
feature tới Redis/Parquet thật, nhưng model `return-risk` v1 train khi feature này còn rỗng nên chưa dùng nó).

```text
Trino (EC2) ──① Gold: fact_return_labels_dp3 ⋈ fact_order_lines_dp2, label=1, 3 ngày trước 2026-06-30
   │            sql/ml/return_events_replay.sql → ~/return_events.jsonl (401.129 dòng)
   ▼
replay_return_events.py ──② Kafka order_snapshots.return_events (3 partition, tổng offset 401.129)
   ▼
Flink ml-sku-return-24h ──③ bỏ trùng theo Order Line, HOP 24h trượt 1h → Kafka ml.sku_return_24h
   │                       flink_jobs/ml/sku_return_feature_a.sql 
   ▼
feast-pusher ──④ push ONLINE_AND_OFFLINE: Redis EKS (NodePort) + S3 sku_return_24h.parquet
   ▼
Feature API EKS ──⑤ /features trả approved_returns_24h
```

Ba lỗi khi chạy lần đầu (đã sửa):

1. `docker compose` thiếu overlay AWS ⇒ Trino lấy catalog MinIO local. Runbook đặt
   `COMPOSE_FILE=docker-compose.yml:docker-compose.aws.yml:docker-compose.aws-stream.yml` một lần.
2. Replay `run feast-pusher` chỉ bật profile `ml-stream` ⇒ "depends on undefined service kafka"; thêm
   `--profile stream --no-deps`. Job Flink `RESTARTING` vì topic nguồn chưa có là bình thường: Kafka tự tạo topic
   khi có tin đầu, Flink tự thử lại.
3. Job `RESTARTING` mãi, mọi checkpoint hỏng. Nghi đầu tiên là thiếu RAM (log có `OutOfMemoryError: Java heap space`)
   ⇒ cho taskmanager AWS 4 GB ([`docker-compose.aws-stream.yml`](../../docker-compose.aws-stream.yml)) nhưng vẫn hỏng.
   Log taskmanager mới chỉ ra gốc: `Size of the state is larger than the maximum permitted memory-backed state.
   Size=72467011, maxSize=5242880` — chưa khai nơi lưu checkpoint nên Flink giữ ở bộ nhớ JobManager (tối đa 5 MB),
   state bỏ trùng + HOP ~80 MB. Sửa: [`sku_return_feature_a.sql`](../../flink_jobs/ml/sku_return_feature_a.sql) 
   `SET 'state.checkpoints.dir' = 'file:///opt/flink/output/checkpoints'` (thư mục mount EBS cả JobManager lẫn
   taskmanager); test `test_stream_feature_checkpoints_go_to_disk_not_jobmanager_memory`. Trước khi sửa, feature đã
   tới API nhưng chỉ tới cửa sổ 2026-06-27 09:00.

Sau khi sửa, job mới chạy hết 3 ngày phát lại: 401.129 sự kiện vào, 76/76 checkpoint thành công (~79 MB mỗi lần,
`file:/opt/flink/output/checkpoints/…`), 0 lỗi.

![Flink ml-sku-return-24h RUNNING: Source return_events → Deduplicate + LocalWindowAggregate → GlobalWindowAggregate → sink ml.sku_return_24h; 401.129 bản ghi vào](../evidence/aws/flink-sku-return-24h-running-20261031.png)

![Checkpoint: 76 triggered, 76 completed, 0 failed; checkpoint mới nhất 79,1 MB ghi ra file:/opt/flink/output/checkpoints](../evidence/aws/flink-sku-return-24h-checkpoints-20261031.png)

**Đối chiếu độc lập** (script đếm lại trên EC2, không dùng Flink): đọc lại `~/return_events.jsonl`, bỏ trùng theo
`order_line_key`, đếm Return trong `[window_end − 24h, window_end)` cho từng tin Flink ghi ở `ml.sku_return_24h`
(gồm cả tin của job hỏng trước đó) ⇒ **1.004.354 / 1.004.354 cửa sổ khớp, 0 lệch**, 2.000 SKU. Cửa sổ cuối
2026-06-29 22:00 khớp số Feature API trả qua Redis:

| SKU | Flink cửa sổ cuối | Feature API `/features` |
|---|---:|---:|
| BEA-01708 | 39 | 39 |
| ELE-01084 | 38 | 38 |
| APP-00042 | 23 | 23 |

![Swagger /features trên EKS: approved_returns_24h BEA-01708 = 39, APP-00042 = 23, cạnh feature batch 90 ngày](../evidence/aws/feature-api-stream-a-features-20261031.png)

Cửa sổ cuối dừng ở 06-29 22:00 (không tới 06-30 00:00) vì watermark trễ 1 giờ sau sự kiện cuối, luồng không tự kết
thúc . Code job: [`sku_return_feature_a.sql`](../../flink_jobs/ml/sku_return_feature_a.sql),
phát lại: [`replay_return_events.py`](../../feature_repo/replay_return_events.py),
[`return_events_replay.sql`](../../sql/ml/return_events_replay.sql), pusher:
[`push_stream_features.py`](../../feature_repo/push_stream_features.py).

<details>
<summary>Tham khảo: thiết kế, hướng dẫn phát triển và ghi nhận trước triển khai AWS</summary>

Các ghi nhận local giữ nguyên ngày và môi trường, không dùng thay bằng chứng cloud.

<!-- Mục đích: job đẩy stream feature vào kho OFFLINE và ONLINE của Feast, -->
## Stream feature vào Feast — ML Platform bước 3

deploy các job đẩy **streaming feature** vào kho OFFLINE và ONLINE.

## Đường đi của dữ liệu

```
Kafka order_lifecycle.seller_postsales.return_events     (publisher của Data Platform)
   │ ① Flink SQL sku_return_feature.sql (chạy liên tục)
   │    - bỏ bản retry trùng theo record_sha256
   │    - tra line_id → seller_sku ở PostgreSQL warehouse_lifecycle.fact_order_lines (lookup join, cache)
   │    - HOP 24h trượt 1h theo event time, đếm Return "approved"
   ▼
Kafka ml.sku_return_24h        {seller_sku, event_timestamp = window_end, approved_returns_24h}
   │ ② feast-pusher (push_stream_features.py, service Compose)
   │    store.push(..., to=ONLINE_AND_OFFLINE), commit offset SAU khi push
   ├──► Redis trong kind (ONLINE)          feature view sku_return_24h
   └──► s3://platform/feature-store/sku_return_24h.parquet (OFFLINE, train dùng PIT join)
```

| Thành phần | File |
|---|---|
| Job Flink | [`flink_jobs/ml/sku_return_feature.sql`](../../flink_jobs/ml/sku_return_feature.sql) |
| Submit job (điền credential từ `.env`) | [`scripts/run/run_sku_return_feature.sh`](../../scripts/run/run_sku_return_feature.sh) |
| Jar JDBC + PostgreSQL cho Flink | [`docker/Dockerfile.flink`](../../docker/Dockerfile.flink) |
| Pusher | [`feature_repo/push_stream_features.py`](../../feature_repo/push_stream_features.py), image [`docker/Dockerfile.feast`](../../docker/Dockerfile.feast), service `feast-pusher` |
| Feature view | `sku_return_24h` trong [`feature_repo/features.py`](../../feature_repo/features.py) |

## Vì sao làm vậy

- **Event Return không có SKU**, chỉ có `line_id`. Flink tra SKU từ warehouse PostgreSQL thay vì
  chở cả đơn hàng vào Kafka. Cache 1h vì một line không đổi SKU.
- **Không cho Flink ghi thẳng Redis**: làm vậy bỏ qua Feast, kho offline không có dữ liệu và train
  không PIT join được.
- **At-least-once**: pusher commit offset sau khi push. Chết giữa chừng thì đọc lại, có thể ghi
  trùng dòng offline — cùng giá trị nên PIT join không đổi.

**Giới hạn (đã đánh dấu `Giới hạn:` trong SQL):** HOP chỉ phát cửa sổ có tin. SKU hết Return thì
Redis giữ số cũ tới khi có tin mới. Push offline đọc + ghi lại cả file mỗi lần — ổn ở cỡ demo.

## Chạy local

```bash
## 0. Đã có Gold (docs/edai-2/feature_store.md bước 1) và Redis trong kind (setup_kind_feature_store.sh)

## 1. Bảng tra line → SKU: publish warehouse từ Gold hiện tại (run_id = generation_id của Gold).
##    Máy chật RAM thì chạy thẳng job export_order_lifecycle.py bằng Spark local[2] (driver 1g) thay vì DAG.
docker compose --profile airflow --profile spark up -d --wait airflow spark-master spark-worker-1 spark-worker-2
docker compose --profile airflow exec airflow airflow dags unpause dp_order_lifecycle_warehouse
docker compose --profile airflow exec airflow airflow dags trigger dp_order_lifecycle_warehouse \
  --conf '{"run_id": "<generation_id>", "cutoff": "2026-06-01T00:00:00.000000Z"}'
docker compose --profile airflow --profile spark stop airflow spark-master spark-worker-1 spark-worker-2

## 2. Kafka + Flink + pusher
docker compose --profile stream --profile flink --profile ml-stream up -d kafka flink-jobmanager flink-taskmanager feast-pusher

## 3. Phát event vận hành vào Kafka
.venv/bin/python -m generator.publish_order_lifecycle_events --source out/order_lifecycle_live \
  --bootstrap localhost:9094 --epoch 1 --manifest out/stream-ml-1.json

## Lần đầu (làm TRƯỚC bước 3): tạo topic và đăng ký view với Feast
for t in carrier.delivery_events seller_postsales.return_events seller_postsales.refund_events seller_wms.stock_operations; do
  docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic order_lifecycle.$t --partitions 3 --replication-factor 1
done
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic ml.sku_return_24h --partitions 1 --replication-factor 1
docker compose --profile ml-stream run --rm --no-deps feast-pusher feast apply

## 4. Submit job Flink (UI: http://localhost:8082)
scripts/run/run_sku_return_feature.sh

## 5. Kiểm
docker compose --profile ml-stream logs feast-pusher | grep pushed
kubectl --context kind-platform -n feature-store exec deploy/redis -- redis-cli dbsize
```

## CI/CD

[`ci/jenkins/stream-features.Jenkinsfile`](../../ci/jenkins/stream-features.Jenkinsfile): **Test** (ruff, test docstring +
renderer stream, test Spark `check_sku_daily_features.py`) → **Build** (image Flink có jar JDBC, image
Feast) → **Deploy** (Kafka + Flink, `feast apply`, recreate pusher, huỷ job Flink cùng tên rồi submit lại)
→ **Verify** (đúng 1 job `ml-sku-return-24h` RUNNING, pusher running).

Secret không nằm trong repo: Jenkins credential `platform-postgres` và `platform-minio` (username/password),
truyền vào bằng `withCredentials`; [`run_sku_return_feature.sh`](../../scripts/run/run_sku_return_feature.sh)
chỉ đọc `.env` khi chạy tay. Jenkins server chưa dựng (việc chung của nhóm CI/CD) — file này chưa chạy.

## Bằng chứng

Lần chạy local 2026-10-21 (fixture 15 đơn / 2 SKU):

- Warehouse: 30 line `line_id → sku-A/sku-B` trong `warehouse_lifecycle.fact_order_lines`.
- Flink job `ml-sku-return-24h`: **RUNNING** (streaming, không tự dừng).
- Pusher: `pushed 72 rows, last 2026-02-05 10:00:00+00:00`; consumer group `feast-pusher` lag = 0.
- Offline: `sku_return_24h.parquet` 72 dòng. Online: `sku-A = 5`, `sku-B = None` (sku-B không có
  Return approved nào — đúng).
- **Đối chiếu độc lập**: đếm lại từ file JSONL nguồn (bỏ trùng theo `source_event_id`, tra SKU từ
  PostgreSQL, cửa sổ `[end−24h, end)`) → 72/72 cửa sổ khớp, 0 sai lệch.
- Cửa sổ cuối dừng ở 02-05 10:00 vì watermark chỉ tiến khi có tin mới (luồng không tự kết thúc).

</details>

[Mục lục phần này](../README.md) · [Mục lục chung](../../README.md)
