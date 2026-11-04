<!-- Mô tả metadata v2 từ code/hợp đồng/receipt; chưa gửi lên DataHub trong sandbox. -->
# DataHub — dataset, job và assertion nguồn v2

> **Pipeline v2 (order lifecycle) — thiết kế và thử ở local, không phải pipeline đã nộp.** Pipeline chạy trên AWS là v1 (DP1 → DP4 + ML), xem [Data Platform evidence](../evidence.md). Các chỗ nhắc GCP dưới đây là kế hoạch cũ, chưa chạy; đích cloud hiện hành là [AWS](../../shared/aws_deploy.md).

[`scripts/run/publish_order_lifecycle_datahub.py`](../../../scripts/run/publish_order_lifecycle_datahub.py) tạo JSON
MetadataChangeProposal (MCP) bằng thư viện chuẩn. Mặc định chỉ in offline;
`--dry-run` cũng in **toàn bộ MCP sẽ gửi**, gồm cả schema, lineage và kết quả FAIL.
Chỉ `--publish` mới nạp SDK DataHub và gửi MCP tới GMS. Script v1 giữ nguyên.

## Dataset và schema

Danh sách Iceberg lấy trực tiếp bằng AST từ job `spark_jobs/bronze/ingest_order_lifecycle.py`, `spark_jobs/silver/build_order_lifecycle.py`, `spark_jobs/gold/build_order_lifecycle.py`, không import Spark:

- Một bảng `lakehouse.bronze.lifecycle_raw_records`.
- 18 bảng `lakehouse.silver.*`: 17 bảng `lifecycle_stg_…` (gồm `lifecycle_stg_observed_evidence`,
  `lifecycle_stg_coverage_validity`) và `quarantine_records`.
- Năm bảng `lakehouse.gold.*`: `fact_order_lines`, `label_return`,
  `feat_sku_return_rate_90d`, `label_segments`, `label_inputs`. Job Gold chỉ có input Silver (không còn Bronze).

Xem tên đầy đủ, grain và quan hệ tại [ERD v2](../../diagrams/schema-order-lifecycle.md).
Nguồn được đăng riêng: 16 feed file (5 native, 9 operational, coverage,
uncertainties) và PostgreSQL `walmart_lifecycle.orders`. Không đăng truth/probe.
Nếu `--source-root` có `manifest.json`, publisher lấy thư mục feed từ đường dẫn
`role=source` trong manifest; nếu không, dùng mẫu thư mục đúng layout job `spark_jobs/bronze/ingest_order_lifecycle.py`:
`<source_root>/dataset_version=*/producer=*/feed=<feed>`.
Dùng cùng định danh `--source-root` giữa các lần publish để không tạo dataset
nguồn khác nhau chỉ vì đổi vị trí mount. URN dùng platform `file/postgres/iceberg`,
env `PROD`; các namespace v2 tách khỏi danh sách `DATASETS` v1.

Schema nguồn lấy từ `contracts/registry.json` và JSON Schema tương ứng. Khi không
có manifest, schema là hợp đường dẫn của các version đăng ký, có schema_id trong
mô tả field; đó là hợp đồng nguồn, không khẳng định mọi field đã hiện diện trong
run. Có manifest thì native chọn version khai báo. Với PostgreSQL, schema mô tả
payload contract được query JDBC dựng, chưa là introspection schema vật lý DB.

Khi có `--receipt`, job `spark_jobs/bronze/ingest_order_lifecycle.py`, `spark_jobs/silver/build_order_lifecycle.py`, `spark_jobs/gold/build_order_lifecycle.py` ghi schema của chính DataFrame đã ghi vào Iceberg,
kèm `"schema_file":"schemas.json"` trong receipt. Đường dẫn tương đối với receipt,
file có dạng
`{"tables":{"lakehouse.gold.lifecycle_fact_order_lines":{"type":"struct","fields":[…]}}}`
(Spark `df.schema.jsonValue()`), giữ tên, kiểu, nullable và field lồng nhau.
Helper `spark_jobs/lib/receipts.py` ghi file tạm rồi rename atomic; schema rỗng hoặc thiếu
field bị từ chối trước khi thay file. Các job trong cùng run chạy tuần tự, hợp
schema theo tên bảng trong `schemas.json`; Silver gồm cả bảng phụ `lifecycle_stg_build_info`.
Mỗi run cần thư mục receipt riêng; khi copy receipt phải copy cả `schemas.json`
cạnh nó. Đây là schema DataFrame đã dùng để ghi, không phải introspection catalog.
Receipt cũ không có `schema_file` vẫn được đọc: dataset description ghi rõ
**“chưa có schema field”**, không dùng contract raw để giả schema Silver/Gold.
Không truyền `--receipt` thì các job không xuất schema/thời gian bổ sung.

## Năm job và cạnh lineage

Flow `airflow/dp_order_lifecycle/prod`; DataJob giữ tên task thật, có chuỗi phụ thuộc
`check_contracts → ingest_bronze → build_silver → build_gold → validate_gold`.

| Job | Input dataset | Output dataset |
|---|---|---|
| `check_contracts` | Không có bảng: kiểm registry file | Không có |
| `ingest_bronze` | 16 feed file; nếu bật JDBC thì thay Walmart file bằng DB | Bronze chung |
| `build_silver` | Bronze | Cả 18 bảng Silver |
| `build_gold` | 4 bảng trong `SILVER_TABLES` job `spark_jobs/gold/build_order_lifecycle.py` (line_lifecycle, native_order_lines, observed_evidence, coverage_validity) — **không** đọc Bronze | Năm bảng Gold |
| `validate_gold` | Cùng 4 input của Gold và năm Gold đã ghi | Không có |

Bảng PostgreSQL luôn được đăng, nhưng chỉ trở thành input khi truyền
`--walmart-jdbc walmart_lifecycle.orders`, đúng Param của run DAG. Walmart file khi đó
vẫn được đăng như bootstrap, không có cạnh input Bronze. Publisher tạo dataset
lineage bảo thủ giữa mọi input/output của từng task biến đổi; không tuyên bố
lineage cấp field. Validator có input và không tạo output dữ liệu.

## Assertion từ receipt đã chạy

Truyền `--receipt-dir` cho thư mục có tên file như DAG, hoặc lặp
`--receipt TASK=PATH` để dùng receipt tên cũ. Không cần đủ năm receipt; chỉ tạo
assertion cho check thực sự có, không tự điền check của task chưa chạy.
Mỗi phần tử `checks` trong Silver/reconcile và Gold/validate thành một assertion
riêng; số vi phạm bằng 0 → `SUCCESS`, khác 0 (kể cả âm) → `FAILURE` (FAIL).
Không loại check fail khi publish. `quarantine` và `info` là số quan sát, không
biến thành assertion thất bại. Check phải là số hữu hạn; thiếu `run_id` bị từ chối.

URN assertion `urn:li:assertion:platform-dp_order_lifecycle-<task>-<check>-v2` tách khỏi v1.
`assertionRunEvent.runId` và `result.nativeResults.run_id` giữ nguyên ID từ receipt,
không thay bằng ID lúc publish. Nhiều receipt độc lập có thể mang run_id khác nhau.
Assertion Silver gắn lên `lifecycle_stg_line_lifecycle` như dataset đại diện của nhóm
reconcile; Gold/Validate gắn `fact_order_lines` đại diện nhóm Gold. Điều này không
có nghĩa check chỉ kiểm bảng đại diện: kiểm PIT/coverage có nhiều input như job.

Khi có `--receipt`, job `spark_jobs/bronze/ingest_order_lifecycle.py`, `spark_jobs/silver/build_order_lifecycle.py`, `spark_jobs/gold/build_order_lifecycle.py`, `spark_jobs/gold/validate_order_lifecycle.py` ghi `completed_at` theo ISO-8601 UTC có hậu tố
`Z`, sau khi ghi bảng và hoàn tất kiểm tra. Receipt Gold `failed_before_write`
cũng có thời gian hoàn tất kiểm tra nhưng không có `schema_file` vì chưa ghi bảng.
`timestampMillis` ưu tiên `completed_at`, đánh dấu `timestamp_source=completed_at`.
Receipt cũ thiếu trường này vẫn dùng mtime file và đánh dấu
`timestamp_source=receipt_mtime`; sao chép file có thể đổi mtime nên đây không phải
thời gian hoàn tất Spark được chứng minh. Contracts/Bronze không xuất `checks`
dạng số nên không bịa assertion cho chúng.

DAG truyền `--receipt <DIRECTORY>/ingest_bronze.json` cho job `spark_jobs/bronze/ingest_order_lifecycle.py` và giữ nguyên
receipt Spark khi thành công, gồm `schema_file` và `completed_at`; publisher
đọc schema Bronze từ `schemas.json` cạnh receipt như Silver/Gold. Chỉ khi Spark
lỗi, operator mới ghi receipt `status=failed`, `checks_available=false` rồi
truyền tiếp lỗi. Receipt lỗi không xác nhận schema hay check số của Bronze.

## Lệnh offline và gửi thật

Dry-run với receipt Silver đã có trong repo; không cần GMS/SDK:

```bash
.venv/bin/python scripts/run/publish_order_lifecycle_datahub.py --dry-run \
  --receipt build_silver=docs/evidence/silver-v2-receipt-20261018.json \
  --source-root /data/order_lifecycle --walmart-jdbc walmart_lifecycle.orders > /tmp/datahub-v2-mcps.json

# Sau khi người vận hành đã chạy DAG và copy receipt về host:
.venv/bin/python scripts/run/publish_order_lifecycle_datahub.py --dry-run \
  --receipt-dir out/receipts-v2-run/THAY_RUN_ID --source-root /data/order_lifecycle \
  --walmart-jdbc walmart_lifecycle.orders > /tmp/datahub-v2-run-mcps.json
```

người vận hành kiểm JSON rồi gửi thật bằng môi trường SDK sẵn có (token đọc từ
`DATAHUB_GMS_TOKEN`, không ghi token vào args):

```bash
"$HOME/.local/share/uv/tools/acryl-datahub/bin/python" scripts/run/publish_order_lifecycle_datahub.py \
  --publish --server http://127.0.0.1:8089 --receipt-dir out/receipts-v2-run/THAY_RUN_ID \
  --source-root /data/order_lifecycle --walmart-jdbc walmart_lifecycle.orders
```

Dùng cùng arguments cho dry-run/publish và giữ nguyên receipt để MCP tương ứng.
Lệnh gửi thật **chưa chạy**. **Chưa đo — Đo trên GCP:**
`bash scripts/run/run_order_lifecycle_live.sh --phase datahub` sau khi pha airflow
thành công trên host đã chuẩn bị theo runbook dưới đây và checklist GCP.
Chưa có xác nhận ingest MCP, UI/Lineage/Quality hay số assertion thực;
các bước này chưa có receipt runtime đính kèm repo.

Kiểm thử offline dùng receipt mẫu trong `tmp_path`, socket bị chặn; không dùng
số mẫu làm bằng chứng Spark thành công. Chưa tự động publish từ DAG, chưa chứng
minh ingest/hiển thị MCP trên GMS. Xem giới hạn vận hành trong [Airflow v2](airflow_order_lifecycle.md).


## Chạy thật

người vận hành chạy [`scripts/run/run_order_lifecycle_live.sh`](../../../scripts/run/run_order_lifecycle_live.sh) trên host đã
chuẩn bị `.env`, Docker Compose, image Spark/Airflow, DataHub quickstart và SDK
như [DataHub v1](../data_governance.md#cách-tái-tạo-trên-máy-đã-chuẩn-bị-datahub-quickstart).
Python `.venv/bin/python` cần generator và `aiohttp`; capture cần `google-chrome`.
Script không cài hay tải công cụ. Mặc định đọc Walmart bootstrap file, không cần
nạp JDBC; dry-run/publish đều dùng nguồn `/data/order_lifecycle` giống DAG.

```bash
# Có thể export PLATFORM_ORDER_LIFECYCLE_DIR=/đường/dẫn/fixture trước cả bốn lệnh.
bash scripts/run/run_order_lifecycle_live.sh --phase source
bash scripts/run/run_order_lifecycle_live.sh --phase airflow
bash scripts/run/run_order_lifecycle_live.sh --phase datahub
bash scripts/run/run_order_lifecycle_live.sh --phase capture
# Hoặc chạy bốn pha theo thứ tự:
# bash scripts/run/run_order_lifecycle_live.sh --phase all
```

Pha source giữ fixture nếu đã có manifest; từ chối ghi đè thư mục không rỗng
thiếu manifest. Pha A bật đúng Airflow, hai Spark worker/master, catalog,
PostgreSQL và MinIO (không chạy generator v1). Timeout khởi động mặc định 600s,
DAG 1800s, chỉnh bằng `PLATFORM_V2_START_TIMEOUT`/`PLATFORM_V2_TIMEOUT`.
Runbook chọn run_id mới, truyền conf `source_root`/`cutoff`, tìm logical timestamp
qua metadata Airflow, copy cả thư mục receipt/schema vào
`out/receipts-v2-run/<run_id>/` rồi dừng pha A. Run thành công ghi
`out/receipts-v2-run/latest-run-id`; pha datahub dùng run này hoặc
`PLATFORM_V2_RUN_ID` được chỉ định. Không trỏ publisher vào thư mục cha nhiều run.
`PLATFORM_ORDER_LIFECYCLE_CUTOFF` mặc định `2026-06-01T00:00:00.000000Z`.

Pha B kiểm đủ năm receipt cùng run và không có check lỗi, dừng pha A, bật
quickstart với override sẵn có, chờ GMS rồi dry-run/publish cùng arguments.
MCP dry-run lưu cạnh receipt tại `datahub-mcps.json`. Python SDK mặc định
`$HOME/.local/share/uv/tools/acryl-datahub/bin/python`, đổi bằng
`DATAHUB_PYTHON`; token chỉ đọc từ `DATAHUB_GMS_TOKEN` của environment.
GMS mặc định `http://127.0.0.1:8089`, UI `http://127.0.0.1:9002`.

Ước lượng pha A 8–12 GiB RAM (Airflow giới hạn 6g gồm driver 3g, worker
2 × 2560m, master 512m, catalog 256m cùng MinIO/PostgreSQL); tổng giới hạn
có thể gần 14 GiB nên dừng workload khác. Pha B quickstart DataHub khoảng
7–10 GiB, Chrome chụp ảnh thêm khoảng 0,5–1 GiB. Đây là dự toán, người vận hành đo
thực tế trên host. Không chạy hai pha cùng lúc; script từ chối pha A nếu
Compose project `datahub` còn container chạy.

Capture dùng [`scripts/evidence/capture_order_lifecycle_datahub_evidence.py`](../../../scripts/evidence/capture_order_lifecycle_datahub_evidence.py),
chờ dataset, job Lineage và assertion Passing (không cố định số check là 1):

| Dataset | Lineage | Quality |
|---|---|---|
| `lakehouse.gold.lifecycle_fact_order_lines` | `images/datahub/v2/dp-order-lifecycle-gold-lineage.png` | `images/datahub/v2/dp-order-lifecycle-gold-validation.png` |
| `lakehouse.silver.lifecycle_stg_line_lifecycle` | `images/datahub/v2/dp-order-lifecycle-silver-lineage.png` | `images/datahub/v2/dp-order-lifecycle-silver-validation.png` |

Kiểm route offline: `.venv/bin/python scripts/evidence/capture_order_lifecycle_datahub_evidence.py --print-urls`.
**Chưa đo — Đo trên GCP:** `bash scripts/run/run_order_lifecycle_live.sh --phase capture`
sau các pha source → airflow → datahub ở trên; hoặc chạy toàn bộ bằng
`bash scripts/run/run_order_lifecycle_live.sh --phase all` trên host đã chuẩn bị.
Chưa có run_id, thời gian, ảnh Lineage/Quality thực; các đường dẫn ảnh trong bảng
là đầu ra dự kiến. Không tạo ảnh giả khi chưa chạy UI thật.

Nếu lỗi, trap in lệnh dọn và giữ receipt/volume. Dùng Compose pha A `stop`
hoặc lệnh quickstart trong docs/edai-1/data_governance.md thay `up -d` bằng `stop` để giải
phóng RAM. Không dùng `down -v`. Pha B để DataHub chạy cho người vận hành kiểm UI;
sau capture người vận hành dừng quickstart bằng lệnh `stop` đó. Run đã append Gold
cần run_id mới khi chạy lại, tránh clear/retry cùng generation. Timeout DAG
có thể để job còn chạy; kiểm log và dừng pha A trước khi chuyển sang pha B.
