# Feature Store trên AWS

EDAI-2, dòng 11: incremental materialize từ kho offline sang kho online. "Materialize" là chép giá trị feature
từ kho offline (file Parquet trên S3, dùng để train) sang kho online (Redis, dùng khi phục vụ request).
"Incremental" nghĩa là mỗi lần chỉ chép phần thời gian mới kể từ lần trước, không chép lại từ đầu.

Code / cấu hình: [DAG](../../dags/ml_feature_materialize.py), [Feast](../../feature_repo/feature_store.yaml).

Các kết quả dưới đây thuộc các lần chạy AWS, không xác nhận dịch vụ đang bật.

### Trên AWS

DAG `ml_feature_materialize` trên Airflow EC2 từng hỏng ở task `feast_apply`. Nguyên nhân:
- Lệnh `feast apply` import mọi file `.py` trong `feature_repo/`, kể cả script phụ `replay_return_events.py`.
- Script đó dùng `datetime.UTC`, thứ chỉ có từ Python 3.11.
- Venv Feast của Airflow lại chạy Python 3.10, nên import lỗi.

![feast_apply lỗi: ImportError cannot import name 'UTC' from 'datetime'](../evidence/aws/airflow-feast-apply-utc-error-20261030.png)

Cách sửa: thêm [`feature_repo/.feastignore`](../../feature_repo/.feastignore) để Feast bỏ qua hai script không phải
định nghĩa feature; có test `test_feast_apply_skips_helper_scripts`. Chạy lại thì cả bốn task xanh và Redis có
`DBSIZE` 2000 (2.000 key, mỗi SKU một key).

![ml_feature_materialize: export_sku_daily, export_training_lines, feast_apply, materialize_incremental đều success](../evidence/aws/airflow-ml-feature-materialize-success-20261030.png)

## Trên AWS

Vị trí các thành phần:
- Airflow và Spark chạy bằng Docker Compose trên EC2.
- Kho offline (Parquet) và registry của Feast (nơi Feast lưu định nghĩa feature) nằm trên S3.
- Kho online là Redis trên EKS, truy cập qua NodePort 30379.

Run `manual__2026-10-29T00:30:43+00:00` của [DAG ml_feature_materialize](../../dags/ml_feature_materialize.py)
hoàn tất 4/4 task, theo [receipt](../evidence/aws/data-platform-aws-20261027.json) (mục `airflow_ml_feature_materialize`).
Bốn task làm việc sau:

1. `export_sku_daily` gọi [export feature SKU](../../spark_jobs/gold/export_sku_daily_features.py).
2. `export_training_lines` gọi [export tập train](../../spark_jobs/gold/export_training_lines.py), chỉ giữ nhãn có trước cutoff 2026-06-30.
3. `feast_apply` đăng ký [feature repo](../../feature_repo/feature_store.yaml). Log của task này cho thấy credentials được lấy từ IAM role của EC2.
4. `materialize_incremental` nạp phần thời gian mới của `sku_return_daily` vào Redis.

![Airflow ml_feature_materialize: export_sku_daily, export_training_lines, feast_apply, materialize_incremental đều success](../evidence/aws/ml-feature-materialize-graph-20261029.png)

![Gantt ml_feature_materialize: export_training_lines và feast_apply có attempt đỏ trước trạng thái cuối xanh](../evidence/aws/ml-feature-materialize-gantt-20261029.png)

![Log feast_apply: triển khai sku_return_daily và sku_return_24h, lấy credentials từ IAM Role platform-data-platform-vm, Done](../evidence/aws/ml-feature-materialize-feast-apply-log-20261029.png)

Receipt ghi hai lỗi đã sửa trong run này: cột `run_id` bị trùng khi export training lines, và thiếu mount
`feature_repo` khi chạy `feast_apply`. Vì vậy biểu đồ Gantt còn dấu các lần thử lỗi, còn Graph ghi trạng thái cuối là success.

Lưu ý: tại thời điểm này kho offline của `sku_return_24h` còn rỗng. `feast_apply` thành công chỉ chứng minh định nghĩa
feature đã được đăng ký, không chứng minh stream pusher đã ghi dữ liệu. Lần chạy stream có bằng chứng riêng: [Streaming Feature](stream_features.md).

<details>
<summary>Tham khảo: thiết kế, hướng dẫn phát triển và ghi nhận trước triển khai AWS</summary>

Các ghi nhận local giữ nguyên môi trường, không dùng thay bằng chứng cloud.

### Công bố batch A/B (thiết kế đã code, chưa chạy trên AWS)

Mục này mô tả code và test đã có trong repo; chưa có lần chạy AWS nào, nên không dùng làm bằng chứng cloud.

Ý tưởng: giữ hai bản feature batch A và B (hai bộ feature của cùng các SKU). Một bản đang được phục vụ ("active"),
bản kia được nạp mới ở chỗ khác. Chỉ khi bản mới nạp xong và kiểm đúng mới chuyển người đọc sang nó. Nhờ vậy người
đọc không bao giờ thấy một bản nạp dở.

Cách chia Redis (một Redis standalone, nhiều DB số):
- DB0 dành cho feature stream.
- DB1 và DB2 luân phiên chứa batch: lần này nạp vào DB1 thì lần sau nạp vào DB2.
- Các DB dùng chung RAM, CPU và file AOF. Đây **không** phải hai Redis có khả năng chịu lỗi độc lập.
- Manifest đặt `maxmemory-policy noeviction`, để việc nạp không tự đẩy dữ liệu cũ ra khỏi RAM khi đầy.
- Cần đo RAM cho cả hai bản trước khi bật trên cloud; giới hạn hiện vẫn là 256 MB.

**Phía ghi.** Task `publish_batch` ở cuối DAG gọi `feature_repo/publish_batch.py`, làm theo các bước:

1. Giữ khóa writer (chỉ một tiến trình ghi tại một thời điểm) trong Redis DB0, rồi đọc con trỏ "active" trên S3
   (con trỏ cho biết bản nào đang được phục vụ).
2. Nếu bản active đã mất sạch dữ liệu nhưng registry vẫn nhớ mốc đã nạp, thì dừng với lỗi. Muốn phục hồi phải chủ động
   chạy với `full_reload=true`. Lần đầu (bootstrap), khi chưa có bản active nào, thì được nạp bình thường.
3. Chọn DB không active làm "candidate" (bản ứng viên), và chặn các reader thuộc generation cũ của DB đó. Nếu còn
   lượt đọc đang có hiệu lực trên DB đó thì dừng với lỗi, trước khi xóa hay nạp gì. Reader nào dùng con trỏ cache cũ
   sẽ bị Redis từ chối và phải làm mới con trỏ, nên không có chuyện đọc slot mới mà tưởng là bản cũ.
4. Copy Parquet nguồn sang prefix riêng `publications/releases/<release_id>/sku_daily` (kiểm bằng ETag), kèm registry
   riêng ở cùng prefix. Nhờ vậy lần export kế tiếp không ghi đè lên dữ liệu nguồn của bản này.
5. Chỉ xóa DB candidate sau khi đã chặn (fence) reader cũ, rồi apply/materialize **toàn bộ** snapshot đến mốc `end`.
   Sau đó đối soát từng SKU (count, rate và nguồn gốc dữ liệu) với snapshot, và kiểm tổng số key. Nếu lỗi thì dọn candidate.
6. Ghi lên S3 một phiếu `validation=passed` không sửa được nữa, rồi đổi `publications/active.json` bằng ghi có điều
   kiện (chỉ ghi nếu ETag vẫn là ETag cũ). Nếu nạp hoặc đối soát lỗi thì con trỏ không đổi, bản cũ vẫn tiếp tục được phục vụ.

Publisher không materialize incremental vào slot candidate, vì slot đó có trạng thái và mốc riêng, phải được dựng
đầy đủ trước khi công bố. Hàm kiểm (guard) của luồng incremental cũ vẫn được dùng để kiểm bản active; hàm này không tự
xóa registry và không tự chữa lỗi. DB1/DB2 chỉ chứa batch; tuyệt đối không flush DB0.

**Phía đọc.** Feature API và KServe dùng chung reader `feature_repo/publication.py`:

- Con trỏ trên S3 được cache 5 giây. Mỗi lần đọc batch đều ghim một bộ manifest/registry/DB, không đổi giữa chừng.
- Reader giữ một lease (quyền đọc có thời hạn) trong Redis, tối đa 30 giây. Kết quả chỉ được trả về nếu lease vẫn
  còn hợp lệ khi đọc xong. Script Lua dùng đồng hồ của Redis, đồng thời kiểm generation để không đọc phải slot đã bị dùng lại.
- Request đang đọc A vẫn đọc tiếp A khi B được công bố. Slot A chỉ được dùng lại khi mọi reader trên đó đã hết hạn
  hoặc đã kết thúc. Lease quá hạn thì kết quả bị bỏ. Request chỉ cần feature stream thì không cần con trỏ batch.
- Nếu cache con trỏ hết hạn mà S3 lỗi, phần batch báo unavailable, không giữ con trỏ cũ mãi; phần stream vẫn có thể chạy.
- API trả thêm `batch_status`, `stream_status`, `batch_release`, `batch_cutoff`. Cách hiểu trạng thái:
  - SKU có trong phiếu nhưng không đọc được: unavailable.
  - SKU nằm ngoài snapshot: not_available (không tự kết luận đó là SKU mới).
  - Count = 0 đã được xác nhận: insufficient_history.
  - Chỉ những nhóm feature có dữ liệu hợp lệ mới được dùng.
- Predictor thiếu count, hoặc không chọn/đọc được bản batch, thì không chấm điểm mà trả `score=null`,
  `abstain=true`, `reason=feature_data_unavailable`. Khi đọc thành công thì kết quả kèm bản và cutoff đã dùng.

Khóa writer **không tự hết hạn**. Nếu tiến trình ghi chết, người vận hành phải xác minh run và tiến trình Feast con
đã dừng rồi mới mở khóa. Không tự xóa khóa, vì như vậy có thể có hai writer chạy chồng nhau. Phiếu và dữ liệu offline
của các release cũ được giữ trên S3 để truy vết; chính sách dọn S3 và ngưỡng dữ liệu được coi là quá cũ vẫn chưa chốt.


<!-- Mục đích: cách dựng và chạy kho feature (Feast) cho phần ML Platform, -->
## Kho feature (Feast) — ML Platform bước 2

Pipeline Airflow tạo feature OFFLINE, nạp một bản batch ONLINE riêng, đối soát rồi công bố.
Phần bằng chứng bên dưới ghi lại luồng incremental cũ, có trước cơ chế công bố A/B (cơ chế A/B mới có code, chưa chạy trên AWS).

## Đường đi của dữ liệu

```
Iceberg Gold (lifecycle_fact_order_lines + lifecycle_label_segments)
   │ ① Spark: export_sku_daily_features.py — ảnh chụp mỗi SKU lúc 00:00 UTC mỗi ngày
   ▼
Parquet  s3://platform/feature-store/sku_daily/          ← kho OFFLINE (MinIO local; AWS: S3)
   │ ② feast apply — ghi định nghĩa vào registry s3://platform/feature-store/registry.db
   │ ③ feast materialize-incremental <end> — chỉ nạp ảnh chụp trong (lần trước, end]
   ▼
Redis trong cụm kind platform, NodePort 30379          ← kho ONLINE
```

DAG [`dags/ml_feature_materialize.py`](../../dags/ml_feature_materialize.py) chạy theo thứ tự: bước ①, rồi xuất tập
train (`export_training_lines.py`, đầu vào của KFP), rồi bước ②, cuối cùng bước ③.

| Thành phần | File |
|---|---|
| Định nghĩa feature (entity `sku`, view `sku_return_daily`) | [`feature_repo/features.py`](../../feature_repo/features.py) |
| Cấu hình Feast (đọc biến môi trường) | [`feature_repo/feature_store.yaml`](../../feature_repo/feature_store.yaml) |
| Hàm tính ảnh chụp theo ngày | `sku_daily_features` trong [`spark_jobs/lib/order_lifecycle_gold.py`](../../spark_jobs/lib/order_lifecycle_gold.py) |
| Job Spark xuất Parquet | [`spark_jobs/gold/export_sku_daily_features.py`](../../spark_jobs/gold/export_sku_daily_features.py) |
| Cụm kind + Redis | [`infra/k8s/kind-config.yaml`](../../infra/k8s/kind-config.yaml), [`infra/k8s/feature-store/redis.yaml`](../../infra/k8s/feature-store/redis.yaml) |
| Kiểm tra | [`tests/spark/check_sku_daily_features.py`](../../tests/spark/check_sku_daily_features.py) |

## Vì sao chụp theo ngày, khoá theo SKU

Feature cũ `feat_sku_return_rate_90d` được tính **cho từng Order Line** tại lúc đặt. Khi có đơn mới thì chưa có
dòng nào tính sẵn cho nó, nên không dùng để phục vụ trực tiếp được. Ảnh chụp theo ngày dùng **cùng** cửa sổ và cùng
phép quét (`_sweep`) với feature cũ, nên một định nghĩa phục vụ được cả hai việc:

- Train: Feast PIT join (point-in-time join) lấy ảnh chụp gần nhất **trước** giờ đặt đơn.
- Serve: Redis giữ ảnh chụp mới nhất.

Bài kiểm tra chứng minh hai điều:
1. Số trong ảnh chụp khớp với phép đếm viết bằng Python thuần.
2. Line đặt đúng 00:00 nhận đúng số của ảnh chụp ngày đó, tức train và serve dùng cùng định nghĩa.

Feature trễ tối đa 1 ngày. Nhãn vốn phải chờ 30 ngày mới có, nên độ trễ này không đáng kể.

## Redis giữ dữ liệu khi thay Pod

Redis giữ dữ liệu trong RAM, nên cần ghi xuống đĩa để không mất khi Pod bị thay:
- Manifest Redis dùng một PVC (ổ đĩa bền của Kubernetes) tên `redis-data`, 2 GiB, gắn tại `/data`.
- Redis ghi AOF (append-only file: nhật ký mọi lệnh ghi) vào chính thư mục đó, với `appendonly yes`, `appendfsync everysec`.
- PVC dùng StorageClass mặc định của cụm (AWS hiện dùng gp3).
- Pod thay thế gắn lại PVC cũ và đọc AOF để dựng lại dữ liệu trong RAM.

### Kiểm chứng trên EKS

Vấn đề ban đầu: Redis trên EKS từng chạy bản manifest cũ (`--save "" --appendonly no`, không PVC, chỉ lưu trong RAM).
Khi node group EKS được đưa về 0 rồi bật lại, pod Redis mất sạch dữ liệu (`dbsize = 0`). Smoke test của gateway báo
`FAIL` đúng một dòng: `get_sku_features APP-00042` trả toàn `null`. Cách xử lý:

1. Áp dụng manifest hiện hành (PVC `redis-data` 2 GiB gp3, AOF `appendonly yes`, `Recreate`): PVC `Bound`, pod `Running`, `appendonly yes`.

   ![kubectl apply redis.yaml: PVC redis-data Bound 2Gi gp3, pod Running, appendonly yes](../evidence/aws/redis-persistence-apply.png)

2. Nạp lại batch bằng DAG `ml_feature_materialize` với `full_reload=true` (sau khi sửa `PLATFORM_FEAST_REDIS` sang IP node EKS hiện tại): bốn task đều xanh, Redis có **2.000 key** (số SKU), AOF ghi bình thường.

   ![Airflow AWS ml_feature_materialize full_reload: export_sku_daily, export_training_lines, feast_apply, materialize_incremental đều success](../evidence/aws/airflow-ml-feature-materialize-reload-success.png)

3. Thay pod Redis (`kubectl delete pod -l app=redis`): pod mới `Ready`, `dbsize` **2000 trước và 2000 sau**, `loading 0`, `aof_enabled 1`, `aof_last_write_status ok`, PVC `redis-data` vẫn `Bound`.

   ![Xóa pod Redis: dbsize 2000 trước và sau, aof_enabled 1, PVC redis-data Bound](../evidence/aws/redis-persistence-pod-replace.png)

**Thử thêm: tắt cả node rồi bật lại (19:53).** Node group EKS được đưa về 0 lúc 17:43, rồi bật lại 2 node mới lúc
19:49 (IP node khác hẳn lần trước). Kết quả sau khi cụm khởi động lại ([output](../evidence/aws/redis-after-node-restart.txt)):
- Pod Redis được tạo lại trên node mới và gắn lại **cùng PVC** `redis-data` (`pvc-7069083e…`, `Bound`).
- `dbsize` vẫn là **2000**; `aof_enabled 1`, `loading 0`, `aof_last_write_status ok`.
- Không phải nạp lại từ Airflow như lần đầu.

Giới hạn:
- Phép thử chứng minh dữ liệu sống sót khi thay pod và khi tắt cả node (PVC giữ nguyên). Nếu PVC bị xóa thì mất;
  PVC và AOF không thay thế backup.
- Lần nạp lại từ Airflow chỉ phục hồi feature batch. Các feature stream (ví dụ `approved_returns_24h`) cần job
  PyFlink online chạy lại mới có giá trị.

Deployment dùng chiến lược `Recreate`: dừng Pod cũ rồi mới tạo Pod mới, chấp nhận gián đoạn ngắn trong lúc cập nhật
và khôi phục. Xóa PVC hoặc xóa cả cụm local vẫn có thể mất dữ liệu; PVC/AOF không thay thế backup. Với chính sách
đồng bộ mỗi giây, nếu máy chết đột ngột có thể mất khoảng một giây thay đổi cuối; xem [Redis persistence](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/).

Cách kiểm chứng sau mỗi lần triển khai:
1. Ghi lại giá trị của một feature đã biết và `DBSIZE`.
2. Thay Pod (giữ nguyên PVC).
3. Tra lại cùng feature đó và kiểm AOF đã được tải.

Việc này phải thử trước khi đóng D3. Nếu Redis đã trống từ trước, PVC mới không tự có lại dữ liệu cũ: cần chạy
`full_reload` để nạp lại batch từ Parquet; feature stream cần phục hồi theo đường riêng.

### Thứ tự chuyển sang luồng mới

Learner chạy mọi thao tác trên cloud. Cần xử lý Vault S1 và endpoint D4 trước khi bật pipeline.

1. Đưa code DAG và `feature_repo` lên EC2. Chạy DAG để tạo bản công bố (publication) đầu tiên, rồi kiểm phiếu và dữ liệu.
2. Build và push image Feature API và predictor có module publication. Script deploy truyền
   `FEAST_PUBLICATION_ROOT=s3://<bucket>/feature-store/publications` cho cả hai reader.
3. Kiểm các trường hợp sau:
   - Response hiển thị đúng release và cutoff.
   - Nạp B bị lỗi thì vẫn đọc A; B đạt rồi mới chuyển sang B.
   - Khi còn lượt đọc A đang chạy thì DB A không được dùng lại.
   - Khi chưa có con trỏ, reader đã bật chế độ mới phải báo batch chưa sẵn sàng, không tự đọc DB0 thay thế.
4. Trong giai đoạn chuyển, DB0 có thể còn feature batch kiểu cũ (legacy). Không xóa cả DB0 vì stream dùng chung.
   Đo dung lượng trước khi quyết định dọn dữ liệu legacy. Reader chưa bật biến opt-in vẫn chạy luồng cũ.

Đã kiểm ở local: unit test cho publication và API, integration test với Redis 7.4 + Feast 0.66, và mutation test.
Các kiểm tra này chưa thay được việc kiểm chứng S3/IAM, rollout và phục hồi trên AWS.

## Chạy local

RAM: chỉ bật phần cần. Không chạy cùng DataHub.

```bash
## 1. Cần Gold order_lifecycle trong catalog. Chưa có thì chạy pipeline Data Platform (~5 phút với fixture):
bash scripts/run/run_order_lifecycle_live.sh --phase source
bash scripts/run/run_order_lifecycle_live.sh --phase airflow     # chạy xong tự dừng stack

## 2. Cụm kind + Redis, nối vào mạng Compose
scripts/setup/setup_kind_feature_store.sh

## 3. Bật lại stack, nạp Variable (gồm platform_feast_root / platform_feast_redis)
docker compose --profile airflow --profile spark up -d minio-init
docker compose --profile airflow --profile spark up -d --wait airflow spark-master spark-worker-1 spark-worker-2
scripts/setup/setup_airflow.sh

## 4. Chạy DAG hai lần với mốc giả lập khác nhau (hoặc UI http://localhost:8081 → Trigger w/ config)
docker compose --profile airflow exec airflow airflow dags trigger ml_feature_materialize \
  --conf '{"end": "2026-05-01T00:00:00Z"}'
docker compose --profile airflow exec airflow airflow dags trigger ml_feature_materialize \
  --conf '{"end": "2026-06-01T00:00:00Z"}'

## 5. Redis có dữ liệu (số key = số SKU)
kubectl --context kind-platform -n feature-store exec deploy/redis -- redis-cli dbsize
```

Log của task `materialize_incremental` ở lần chạy thứ hai bắt đầu từ mốc kết thúc của lần một, không nạp lại từ
đầu. Đó là bằng chứng cho tính "tăng dần" (incremental).

Feast nhớ mốc đã nạp trong registry. Muốn chụp lại bằng chứng từ đầu thì xoá registry trước bước 4. Redis cũng nên
xoá, bằng `kind delete cluster` rồi chạy lại bước 2:

```bash
docker compose run --rm --entrypoint sh minio-init -c \
  'mc alias set local http://minio:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD && mc rm local/platform/feature-store/registry.db'
```

`end` là thời gian **giả lập**. Ở lần nạp đầu, Feast chỉ quét từ 1 năm trước giờ thật (ttl = 0), nên dữ liệu giả
lập phải nằm trong khoảng đó.

## Tắt

```bash
kind delete cluster --name platform        # Redis không giữ dữ liệu; materialize lại là có
docker compose --profile spark --profile airflow down
```

## Bằng chứng

Lần chạy local (fixture 15 đơn / 2 SKU, cutoff 2026-06-01):

| Run | `end` | Log `materialize_incremental` | Kết quả |
|---|---|---|---|
| mat_4 | 2026-05-01 | `sku_return_daily from 2025-10-22 … to 2026-05-01` | success |
| mat_5 | 2026-06-01 | `sku_return_daily from 2026-05-01 … to 2026-06-01` | success |

- `export_sku_daily`: `wrote 304 rows` (2 SKU × 152 ngày).
- Redis `dbsize` = 2; `get_online_features` trả đúng ảnh chụp 2026-06-01 của Parquet.
- Feature = 0 ở fixture này là đúng, vì trong 90 ngày trước cutoff chưa line nào có nhãn đủ 30 ngày.
  Chỉ với dữ liệu cỡ lớn (ở bước train) mới có số > 0.

</details>

[Mục lục phần này](README.md) · [Mục lục chung](../README.md)
