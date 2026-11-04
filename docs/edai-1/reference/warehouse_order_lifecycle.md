<!-- Warehouse PostgreSQL v2: mô hình, publish an toàn và quy trình thu bằng chứng thật. -->
# Warehouse v2 trên PostgreSQL

> **Pipeline v2 (order lifecycle) — thiết kế và thử ở local, không phải pipeline đã nộp.** Pipeline chạy trên AWS là v1 (DP1 → DP4 + ML), xem [Data Platform evidence](../evidence.md). Các chỗ nhắc GCP dưới đây là kế hoạch cũ, chưa chạy; đích cloud hiện hành là [AWS](../../shared/aws_deploy.md).

Phần này đo warehouse indexing. Lát Walmart v1 không đại diện
năm Channel và mất index sau overwrite. Job `spark_jobs/warehouse/export_order_lifecycle.py` đọc `build_gold`, ghi schema mới
`warehouse_lifecycle` trên PostgreSQL service của VM. Giữ nguyên v1, không quy đổi hay
cộng chung currency. Cấu hình VM/storage/auth xem [AWS runbook](../../shared/aws_deploy.md).

| Bảng | Grain và dữ liệu giữ lại |
|---|---|
| `fact_order_lines` | PK `(dataset_version,line_id)`; toàn bộ cột Gold thật, identity năm Channel, currency, tiền minor bigint, ordered_basis_qty/canceled_unshipped_qty/allocated_qty/delivered_qty, headline và product refund tách riêng |
| `label_return` | PK `(dataset_version,line_id,cutoff,generation_id)`; state, label nullable, label_version, computed_at, maturity/cohort, input_json, reasons và source_event_refs dạng text[] |
| `feat_sku_return_rate_90d` | PK `(dataset_version,line_id)`; SKU, event_timestamp, created, counts/rate, label_version và generation_id |

DDL [001](../../../sql/warehouse_lifecycle/001_tables.sql) lấy cột từ
`spark_jobs/lib/order_lifecycle_gold.py` và `spark_jobs.lib.order_lifecycle_lakehouse.line_lifecycle`; Spark UTC timestamp
ánh xạ PostgreSQL `timestamp` (không timezone), quantity nguyên và aggregate bigint,
rate double precision. Không tự thêm gross/discount vốn không có trong fact Gold.
Nhãn warehouse là **generation/cutoff đang export**, lịch sử bất biến vẫn ở Gold.
CHECK giữ label NULL khi state khác labeled; không biến unknown thành 0.

| Index từ [002](../../../sql/warehouse_lifecycle/002_indexes.sql) | Truy vấn |
|---|---|
| PK của 001, đi cùng staging khi rename | Chống trùng grain, lookup line khi join |
| `lifecycle_fact_sales(created_at) INCLUDE(channel,currency,headline_product_net_minor)` | Q1: headline 30 ngày, GROUP BY ngày UTC × channel × currency |
| `lifecycle_fact_sku(seller_sku,dataset_version,line_id) INCLUDE(generation_id)` | Q2: chọn SKU rồi join fact/label bằng dataset, line và generation |
| `lifecycle_label_cutoff(cutoff,dataset_version,line_id,generation_id) INCLUDE(label) WHERE state='labeled'` | Q2: mẫu số chỉ gồm labeled đúng cutoff, AVG(label) là tỉ lệ |
| `lifecycle_feature_pit(dataset_version,seller_sku,event_timestamp DESC,created DESC)` | Q3: bản feature mới nhất có event_timestamp ≤ T và created ≤ thời điểm truy vấn biết dữ liệu |

Năm Channel bắt buộc chính xác `amazon,walmart,shopee,shopify,tiktok_shop` khi export fact.
Q3 giữ nguyên grain feature line của Gold, lấy một dòng với tie-break line_id; không
cộng counts giữa các line. `created` là availability lúc tính nên có thể sau
`event_timestamp`; truy vấn lúc đặt đơn có thể chưa có feature và trả 0 dòng.
Phải chọn `--as-of` phù hợp mục tiêu kiểm, không ngầm coi feature tính sau là đã biết tại T.

## Publish an toàn

1. CLI pin snapshot Iceberg hiện hành riêng từng bảng. Fact/feature phải toàn bộ
   thuộc `--run-id`; nhãn lọc đúng generation và cutoff; snapshot thiếu hoặc bảng
   rỗng fail closed. DAG validate Gold trước khi chạy export.
2. Giữ session advisory lock chung export/benchmark. Tạo bảng thường staging
   `<table>__load_<sha256(run_id)[:12]>` từ 001, gồm PK/CHECK, rồi append qua JDBC.
   Không dùng TEMP TABLE vì Spark executors mở nhiều JDBC session.
3. So logical count Gold và staging thực; fact so count và **bốn tổng tiền riêng
   theo `(channel,currency)`**, decimal/numeric khi SUM để tránh tràn bigint.
   Receipt ghi expected/actual và boolean từng check, không ghi credential.
4. Chỉ khi tất cả checks true: một transaction rename các bảng cũ, đổi tên index
   cũ để giải phóng tên schema-wide, rename staging thành bảng phục vụ, áp 002,
   ANALYZE, rồi DROP bản cũ. Commit cả nhóm bảng. Lỗi bất kỳ rollback cả nhóm.
5. Xóa staging do lần gọi tạo khi thất bại, giữ nguyên bảng đang phục vụ. Nếu
   process bị kill, staging có thể còn: người vận hành kiểm tra/xóa đúng staging lỗi
   trước retry cùng run-id; job không tự xóa staging đã tồn tại.

`--tables` là danh sách phân cách dấu phẩy; mặc định cả ba. Subset chỉ publish nhóm
được chọn, vì vậy dùng mặc định cho generation đầy đủ. Writer phải dùng quy trình
này và không có view/FK phụ thuộc bảng phục vụ: PostgreSQL view/FK bám OID bảng cũ,
không bám tên; DROP không CASCADE sẽ từ chối và rollback nếu có dependency.
Không cấp quyền riêng từng bảng trong gói này: swap tạo OID mới, người vận hành dùng cùng
owner hoặc quyền mặc định cấp ở schema. Transaction rename cần khóa bảng và có
thể chờ truy vấn đang chạy; benchmark cũng chặn ghi trong suốt phép đo.

## Chạy local và trên VM

Venv có `psycopg`; image Airflow hiện có `psycopg2-binary==2.9.10`. Module tự chọn
một trong hai driver, không cài thêm dependency. Spark cần JAR PostgreSQL 42.7.4
sẵn trong image/cache và Iceberg runtime như runner hiện có. Chưa chạy Spark hoặc
PostgreSQL thật trong sandbox.

Compose hiện chỉ mount `spark_jobs/`, không mount `sql/`. Vì brief cấm sửa Compose,
người vận hành copy hai migration vào container Airflow đang chạy; không cần sửa config:

```bash
# người vận hành chạy sau khi stack đã sẵn sàng; copy lại sau recreate container.
# SQL đã mount read-only tại /opt/sql/warehouse_lifecycle.
# DAG mới dp_order_lifecycle_warehouse: Trigger tay với generation đã validate của dp_order_lifecycle.
# {"run_id":"GOLD_GENERATION_DA_CO","cutoff":"2026-06-01T00:00:00.000000Z"}
```

DAG mới có `validate_gold >> export_warehouse`, schedule=None, một run tại
một thời điểm; không sửa DAG nguồn. SQL dir mặc định `/opt/sql/warehouse_lifecycle`, có thể
đặt Airflow Variable `platform_order_lifecycle_warehouse_sql_dir`. Receipt dưới Variable
`platform_order_lifecycle_receipt_dir`/warehouse/TS_NODASH. Trên GCP cũng copy SQL bằng lệnh trên
trong `/opt/platform`, dùng PostgreSQL trên VM và credential Connection `platform_postgres`.

CLI ngoài DAG (trong image/runtime có Spark và catalog):

```bash
spark-submit --jars /DUONG_DAN_CACHE/postgresql-42.7.4.jar \
  spark_jobs/warehouse/export_order_lifecycle.py \
  --run-id GOLD_GENERATION_DA_CO --cutoff 2026-06-01T00:00:00.000000Z \
  --receipt /tmp/warehouse-v2-export.json --sql-dir sql/warehouse_lifecycle
```

Cần cả cấu hình/JAR Iceberg/GCS/MinIO của runtime hiện tại; CLI minh họa phần JDBC
và arguments, không thay runner catalog. Kết nối từ env như job `spark_jobs/warehouse/export_order_snapshot_lines.py`:
`POSTGRES_HOST` (postgres), `POSTGRES_PORT` (5432), `POSTGRES_DB` (sources),
`POSTGRES_USER`, `POSTGRES_PASSWORD`. Không đặt secret trong arguments/log.

## Harness PostgreSQL 16 tạm — người vận hành chạy

Dùng DB **trống riêng**; harness từ chối schema warehouse_lifecycle đã tồn tại. người vận hành
chuẩn bị container `postgres:16` trên loopback hoặc Docker network riêng và đưa
URI qua env `WAREHOUSE_TEST_DSN`; không dùng DB đang phục vụ. Môi trường chạy
Python cần PySpark 3.5.3, Java và driver PostgreSQL Python sẵn có. Không tải JAR:

```bash
# Env này chứa URI DB tạm: postgresql://USER:PASSWORD@127.0.0.1:PORT/DB
# người vận hành đặt WAREHOUSE_TEST_DSN trong môi trường, không ghi vào report.
PYSPARK_SUBMIT_ARGS='--jars /DUONG_DAN_CACHE/postgresql-42.7.4.jar pyspark-shell' \
  python tests/spark/check_order_lifecycle_warehouse.py
```

Harness build fixture → Silver → Gold thật, load 1 thành công; load sai tiền
staging bị chặn và toàn bộ giá trị bảng phục vụ giữ bản 1; lỗi DDL giữa swap cũng
rollback bản 1; load hợp lệ tiếp theo vẫn có bốn index phụ. Staging sai được dọn,
DB tạm giữ bản hợp lệ cuối để kiểm thêm. người vận hành tự quản vòng đời container.

## EXPLAIN trước/sau trên cùng dữ liệu

```bash
# Env POSTGRES_* trỏ DB cần đo; chọn giá trị thực có trong Gold đang phục vụ.
.venv/bin/python scripts/evidence/measure_order_lifecycle_warehouse.py \
  --cutoff 2026-06-01T00:00:00Z --sku SELLER_SKU_THUC --dataset DATASET_VERSION_THUC \
  --event-timestamp 2026-04-01T00:00:00Z --as-of 2026-10-19T00:00:00Z \
  --receipt /tmp/warehouse-v2-explain.json
```

Script lấy advisory lock, khóa SHARE cả ba bảng, REPEATABLE READ; bỏ **mọi index
phụ** của ba bảng (giữ PK), ANALYZE rồi đo Q1–Q3 mỗi query năm lần. Sau đó tạo
002 và đo tiếp năm lần. Toàn bộ DROP/CREATE/ANALYZE được rollback cuối cùng;
index có trước phép đo được phục hồi cả khi lỗi. Không ép enable_seqscan=false.

JSON có từng plan đầy đủ, node gốc, actual rows, shared read/hit/total blocks và
median từng metric. Node gốc đã gồm child buffers, không cộng lại. So **plan và
median shared hit+read**, cùng read/hit tách riêng: pha sau có thể cache nóng hơn,
không diễn giải shared read giảm là toàn bộ lợi ích index. Không dùng timing làm
proof chính. Fixture nhỏ, range ít chọn lọc hoặc query trả 0 dòng có thể dùng
Seq Scan ở cả hai pha; ghi đúng kết quả, không khẳng định index luôn nhanh hơn.

Số đo **e2e small local ngày 2026-10-19**, sau sửa và đổi tên; nguồn 45 ngày ×
50 đơn, 100 SKU, PASS 92 s. Nguồn số và chi tiết run tại [e2e local](e2e_order_lifecycle_local.md),
được người vận hành xác nhận trong ghi nhận local (receipt không đính kèm repo).
Warehouse-checks trên PostgreSQL thật **PASS 116 s trước đổi tên**: load, chặn lệch
tiền, rollback DDL và swap giữ index; đây không phải số đo speedup.

| Query | Trước: node / buffers hit+read / rows (median n=5) | Sau: node / buffers hit+read / rows (median n=5) |
|---|---|---|
| Q1 headline ngày × Channel × currency | Aggregate / 173 / 0 | Aggregate / 2 / 0 |
| Q2 SKU return rate tại cutoff | Aggregate / 583 / 5 | Aggregate / 583 / 5 |
| Q3 SKU feature PIT | Limit / 104 / 1 | Limit / 4 / 1 |

Q3 giảm 104 → 4 buffer nhờ index; Q2 không đổi. Q1 giảm 173 → 2 nhưng cửa sổ
rỗng (0 dòng), không chứng minh lợi ích index. Medium chưa tới EXPLAIN do lần
trước sửa bị OOM ở gold_validate; chưa chạy lại sau sửa.
Đo trên GCP: `bash tests/harness/run_order_lifecycle_scale.sh --profile medium --driver-memory 6g`
trên host đã chuẩn bị; đo DB phục vụ bằng lệnh `measure_order_lifecycle_warehouse.py`
ở trên với tham số thực. Lưu JSON kèm run/cutoff và ảnh plan thật; chưa có số GCP.
