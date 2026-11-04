# Chạy đầu-cuối v2 trên nguồn scale local

> **Pipeline v2 (order lifecycle) — thiết kế và thử ở local, không phải pipeline đã nộp.** Pipeline chạy trên AWS là v1 (DP1 → DP4 + ML), xem [Data Platform evidence](../evidence.md). Các chỗ nhắc GCP dưới đây là kế hoạch cũ, chưa chạy; đích cloud hiện hành là [AWS](../../shared/aws_deploy.md).

Runner `scripts/run/run_order_lifecycle_local.py` kiểm cả chuỗi nguồn scale → Bronze → Silver → Gold →
PostgreSQL → EXPLAIN trước/sau index. Một Spark session `local[*]`, Iceberg Hadoop
catalog trong `/tmp`, không cần Lakekeeper/MinIO. Không đọc truth làm input.
Đây là kiểm chạy được trước GCP, chưa phải bằng chứng GCP hay 100 GB.

người vận hành chạy từ root repo, image/cache JAR phải có sẵn:

```bash
bash tests/harness/run_order_lifecycle_scale.sh --profile small
bash tests/harness/run_order_lifecycle_scale.sh --profile medium --driver-memory 6g
```

Small = 50 đơn/ngày × 45 ngày; medium (mặc định) = 500 × 60, seed 7.
Có thể truyền `--orders-per-day`, `--days`, `--seed`, `--sku-count`, `--sku-zipf-s`,
`--cutoff`, `--run-id` qua harness. `--source /workspace/out/nguon-co-san` dùng
nguồn đã sinh trong repo; đường dẫn này thuộc container. Nguồn được mount chỉ đọc.
Không truyền lại `--receipt` qua harness: nó đã đặt `/proof/receipt.json`.

Harness tạo Docker network internal, PostgreSQL 15 riêng không publish port,
chờ DB sẵn sàng, chạy image `platform-airflow:2.10.5-spark3.5.3`, dùng Iceberg/JDBC JAR
ở volume `platform_airflow-data:/ivy:ro`, không tải mạng. Spark chạy bằng user mặc định
image, không đổi HOME hoặc ép uid host; cleanup chown output về host và dọn đúng
container/network riêng. Log/receipt giữ tại `out/e2e-v2-<profile>.<suffix>/` kể cả
khi lỗi; catalog và nguồn tự sinh trong `/tmp` container được dọn sau chạy.

Để chạy trực tiếp trong runtime đã có Spark/JAR, đặt `WAREHOUSE_TEST_DSN` trong env
(URI PostgreSQL như harness warehouse), hoặc `POSTGRES_HOST/PORT/DB/USER/PASSWORD`:

```bash
spark-submit --master 'local[*]' --driver-memory 6g --jars /cache/iceberg-runtime.jar,/cache/postgresql.jar \
  scripts/run/run_order_lifecycle_local.py --profile small --receipt out/e2e-manual/receipt.json
.venv/bin/pytest -q
```

DB phải trống, chưa có schema `warehouse_lifecycle`; runner từ chối ghi lên schema đã tồn
tại. DSN/password không có trong receipt. Dùng DB tạm của harness để cô lập phép đo.

Receipt schema 1 gồm tham số, dataset_version, cutoff, số dòng từng bảng và từng
chặng, thời gian tường, toàn bộ checks/validation Gold, receipt export warehouse và
EXPLAIN đầy đủ. Silver ghi bốn bảng Gold cần rồi đọc lại; Gold validate trước khi ghi,
`feature_sample=50`, `sample_limit=200` mặc định của validator. Gold cũng đọc lại từ
catalog trước export. Chặng gold_build có count để đo action thật; các lineage chưa
cache có thể tính lại trong validate/write, vì vậy thời gian các chặng gồm cả tính lại.
Export có receipt riêng `receipt-warehouse.json` và đối soát count/tiền trước publish.
Chặng lỗi lưu loại exception, thời gian và số dòng đã đo; status tổng chỉ passed khi
mọi chặng hoàn tất. Kill cứng/OOM có thể chỉ để lại receipt running gần nhất, không
được coi là PASS.

SKU benchmark là SKU nhiều dòng nhất ở fact, tie-break tên SKU; event_timestamp lấy
một dòng feature thực của SKU đó, `as_of=created` của chính dòng đó. Dùng benchmark
trong `scripts/evidence/measure_order_lifecycle_warehouse.py`: 3 query × 5 lần × hai pha pk_only/indexed,
rollback index sau đo. Pha pk_only vẫn giữ primary key. So median shared hit+read,
đọc plan đầy đủ; không suy lợi ích chỉ từ wall time/read giảm vì cache nóng dần.
Cutoff mặc định = manifest.horizons.source_observation_end; với nguồn mặc định,
Q1 cửa sổ 30 ngày cuối không còn đơn mới nên có thể trả 0 dòng. Báo đúng số này,
không coi Q1 rỗng là bằng chứng index có ích; cần bộ nguồn/horizon thích hợp khi
đánh giá Q1. Q2 vẫn dùng cutoff nhãn thật; không đổi cutoff benchmark âm thầm.

`peak_rss.python_driver_kib` đo `resource.RUSAGE_SELF.ru_maxrss` (Linux KiB).
`terminated_children_kib` đo RUSAGE_CHILDREN, không phải tổng RSS Python+JVM;
JVM còn sống hoặc JVM do spark-submit quản lý có thể không nằm trong metric này.
Không dùng số đó như peak toàn bộ Spark/executors/container.

| Profile | Dataset / số dòng nguồn, fact | Source / Bronze / Silver (s) | Gold build / validate / write (s) | Warehouse / EXPLAIN (s) | RSS Python KiB |
|---|---|---|---|---|---|
| small | `dataset-15244a34…` / 40.398 bản ghi nguồn, 3.631 fact | 3,3 / 7,4 / 51,5 | 7,6 / 10,9 / 1,8 | 2,8 / 0,3 | 161.296 |
| medium | 534.853 bản ghi nguồn, 48.874 fact | 50,7 / 23,9 / 2.090,1 | 42,2 / **OOM** / — | — / — | — |

| Profile / query | Trước: node / median buffers hit+read / rows | Sau: node / median buffers hit+read / rows |
|---|---|---|
| small / Q1 | Aggregate / 173 / 0 | Aggregate / 2 / 0 |
| small / Q2 | Aggregate / 583 / 5 | Aggregate / 583 / 5 |
| small / Q3 | Limit / 104 / 1 | Limit / 4 / 1 |
| medium / Q1 | chưa đo — medium chưa tới bước EXPLAIN | — |
| medium / Q2 | chưa đo — medium chưa tới bước EXPLAIN | — |
| medium / Q3 | chưa đo — medium chưa tới bước EXPLAIN | — |

Số đo 2026-10-19, local, `--driver-memory 6g`. **small**: chạy sau khi đổi tên và sau ba bản sửa bộ nhớ/hiệu năng; mọi check Gold = 0; số dòng mọi bảng trùng lần chạy trước khi đổi tên. **medium**: lần chạy *trước* các bản sửa: Silver 2.090 s (87% ở `coverage_validity`, đã sửa bằng `collect_set`), `gold_build` qua, `gold_validate` OOM ở oracle feature (đã sửa: gom theo SKU). Chưa chạy lại medium sau sửa. EXPLAIN small: Q1 cửa sổ 30 ngày cuối không có đơn (0 dòng) nên không chứng minh lợi ích index; Q3 giảm 104 → 4 buffer nhờ index; Q2 không đổi (583). Q1/Q2/Q3 là ba truy vấn mẫu của `scripts/evidence/measure_order_lifecycle_warehouse.py`.


Chưa chạy Docker/Spark/PostgreSQL trong sandbox Codex. Local dùng CPU/đĩa/cache
máy dev, file Walmart bootstrap thay CDC/JDBC nguồn thật, không suy ra hiệu năng,
chi phí hoặc độ bền trên GCP. người vận hành giữ JSON/log thực và điền bảng sau khi chạy.
