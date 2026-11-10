# Iceberg maintenance — Iceberg tự bảo trì theo chính sách

Gói H bổ sung trigger metadata, retention generation, retry và hậu kiểm cho
[acceptance ledger](../evidence.md). Dòng `de` 20 đã có compaction thủ công
ở job `spark_jobs/maintenance/compact_amazon_order_snapshots.py`; tài liệu này không tái sử dụng ảnh/số đo đó làm bằng chứng maintenance.
Theo [Spark mục 5b](spark.md), nhiễu thời gian từng đo khoảng 14,5%; chỉ số chính
ở đây là số file, file ứng viên của probe, snapshot, số dòng và checksum.

## Thành phần và quyết định

`spark_jobs/lib/maintenance_policy.py` không import pyspark: metadata dict + cấu hình + thời gian
UTC → kế hoạch JSON với lý do cho từng hành động, snapshot được bảo vệ và generation
tag. `spark_jobs/maintenance/maintain_iceberg_tables.py` đọc `.files`, `.snapshots`, `.refs`, `.partitions`
và `SHOW TBLPROPERTIES`, gọi policy rồi preflight SQL trước khi thi hành. Session
production dùng `spark_jobs.lib.iceberg_session.build_spark`; local dùng Hadoop catalog riêng.
Không thêm dependency, không tự tải jar trong test local.

| Thứ tự | Kind | Trigger | Thi hành |
|---|---|---|---|
| 1 | compact | Partition có ít nhất min_input_files và trung bình byte nhỏ hơn small_file_ratio × target_file_size_bytes | rewrite_data_files, binpack, where chỉ các partition đạt ngưỡng |
| 2 | zorder | Có zorder_columns và chưa có mốc hoặc đủ zorder_min_new_files từ mốc | rewrite_data_files, sort, zorder(c1,c2), rewrite-all=true; ghi snapshot id mới vào platform.maintenance.last_zorder_snapshot_id |
| 3 | expire_snapshots | Snapshot quá tuổi, ngoài retain_last và không bị ref trỏ tới | expire_snapshots với older_than UTC, retain_last |
| 4 | remove_orphans | Luôn kiểm kê; older_than ít nhất 72 giờ | remove_orphan_files, dry_run=true mặc định; chỉ false tường minh mới xoá |

Đếm file mới đi theo parent_id từ snapshot main về mốc, không đếm nhánh khác.
Mốc không còn trong ancestry → z-order lại một cách bảo thủ. `rewrite-all=true`
đảm bảo bước z-order vẫn sắp file vừa được binpack; file đủ lớn không đồng nghĩa đã
được sắp theo nội dung.

**Quy tắc lập kế hoạch:** yêu cầu remove_orphans luôn có trong kế hoạch
đồng thời yêu cầu kế hoạch lần hai rỗng. Implementation giữ cuộc kiểm kê dry-run;
`no_op=true` và `reason="no-op: ..."` khi không còn hành động thay đổi bảng.
`actions` lúc đó chỉ có remove_orphans dry-run, không phải mảng rỗng. Kiểm thử ghi
rõ ngoại lệ này thay vì giả vờ hai yêu cầu có thể đồng thời được đáp ứng. Với
orphans_dry_run=false, luôn còn một hành động xoá theo luật cấu hình.

## Retention generation

Generation là tag có tiền tố mặc định `gen-`, đổi qua generation_tag_prefix.
Policy liệt kê `protected_snapshot_ids` từ **tất cả** tag/branch, không đưa chúng
vào `expire_snapshots.snapshot_ids`. `generation_tags` là danh sách tag cần kiểm
checksum. `main` cũng là một branch được bảo vệ.

Trước mọi procedure, job đọc từng generation qua `VERSION AS OF '<tag>'` và lưu
count/checksum/schema. Ngay sau expire, và lần nữa cuối job, đọc lại từng tag rồi
so với trước. Không đọc được hoặc sai checksum → fail. Iceberg vẫn là lớp thực
thi retention: procedure dùng older_than/retain_last và xử lý ref theo Iceberg;
danh sách ứng viên trong policy không được coi là số snapshot chắc chắn đã xoá.
Không tự sửa retention của tag, không xoá tag/branch. Job từ chối chạy expiry
nếu TTL ref (max_reference_age_in_ms hoặc history.expire.max-ref-age-ms) có thể
làm Iceberg tự bỏ ref đang được bảo vệ. Kiểm này chạy trước mọi procedure và
lặp lại sát CALL expiry; hậu kiểm vẫn cần thiết để phát hiện thay đổi đồng thời.

## Retry, lỗi và receipt

`execute_actions` nhận callback procedure và hàm sleep; pytest dùng callback giả.
CommitFailedException được retry. ValidationException chỉ retry khi thông điệp
cho thấy concurrent/conflicting/found new/cannot commit/changed/already been committed.
Validation schema sai không được retry. Tối đa mặc định 3 lần, backoff 1s, 2s,
giới hạn 8s. Receipt lưu kind, attempt, status, error, transient, delay, result.
Lỗi fatal hoặc hết lượt → dừng ngay, không thi hành action/bảng tiếp theo, ghi
receipt failed và exit khác 0. Không rollback các action trước đã commit.

`--inject-transient-failures 2` giả hai lỗi **trước CALL** của action đầu, rồi thử
lần ba. `--inject-fatal-failure` thất bại trước CALL đầu. Multi-table chỉ inject
trên bảng đầu. Receipt JSON ghi atomic, có UTC timestamps, sha256 hai module code,
sha256 cấu hình thực dùng, plan, SQL, từng attempt và toàn bộ số trước/sau.

## Hậu kiểm và cách đo

Trong cùng job execute, trước/sau đều đo:

- `count(*)` qua quan hệ logic, không lấy record_count metadata.
- Fingerprint: tổng decimal(38,0) của `xxhash64` trên **mọi cột theo thứ tự schema**.
  Cùng count, checksum và danh sách cột phải bằng hệt; tổng độc lập thứ tự dòng/file,
  tránh overflow bigint. Đây là checksum xác suất, có khả năng collision.
- Số data file hiện hành (`content=0`), tổng byte, số snapshot thực đọc metadata.
- Với zorder_columns và probe_predicates: số file có lower_bound/upper_bound giao
  mọi khoảng đóng AND của probe; thiếu một bound thì không dùng bound đó để loại
  file. Chỉ loại khi có chứng cứ khoảng không giao. Date/timestamp được chuẩn hoá
  theo kiểu metadata, timestamp về UTC.
- Mỗi probe chạy count một lượt warmup + 3 lượt đo; receipt giữ từng số giây, n=3.
  Không dùng thời gian để quyết định pass/fail hay khẳng định tăng tốc.

Đây là **file ứng viên theo min/max**, không phải instrumentation số file Spark
thực mở: Spark còn có thể dùng partition pruning/row group/delete files. Metrics
phải được bật cho cột liên quan; truncated bounds làm ước lượng bảo thủ. Predicate
cấu hình có dạng `{"name":"sku-0001","ranges":{"seller_sku":["sku-0001","sku-0001"]}}`.
null là không chặn đầu khoảng; schema probe hiện hỗ trợ khoảng đóng, không OR/LIKE.

`--validate-receipt <execute.json>` chạy ở Spark session mới, chỉ nhận receipt
success, kiểm before/after đã bằng nhau rồi đọc lại bảng/tag và so checksum/schema.
Không ghi đè receipt gốc. Nếu dữ liệu được writer khác thay đổi trong cửa sổ
bảo trì/validate thì checksum khác sẽ fail; cần chọn cửa sổ ít ghi để đọc bằng
chứng nhất quán. Job không khoá writer hay cung cấp transaction nhiều action.

## Airflow và vận hành

DAG mới `maintenance_iceberg` có schedule @daily, catchup=False,
max_active_runs=1, is_paused_upon_creation=True. Ba task dùng
PlatformSparkSubmitOperator: plan (--plan-only) → execute (--execute, có hậu kiểm) →
validate (--validate-receipt, session mới). Execute lập lại kế hoạch từ metadata
hiện hành; không thực thi mù một plan cũ.

Biến thêm trong `scripts/setup/bootstrap_airflow_config.py`:

- platform_maintenance_tables: JSON array tên bảng đủ catalog.namespace.table;
  mặc định chỉ demo `lakehouse.demo.maintenance_demo`.
- platform_maintenance_receipt_dir: `/opt/airflow/logs/iceberg-maintenance`, nằm trên
  volume Airflow; mỗi logical timestamp có plan.json và execute.json riêng.

Compose hiện không mount configs/. Vì vậy `scripts/setup/setup_airflow.sh` **chỉ thêm** thao tác
nạp nguyên configs/iceberg_maintenance.json qua stdin vào Variable
platform_maintenance_config. DAG truyền nội dung bằng --config-json. Thao tác này
không đổi cấu hình biến khác hay Compose. Sau khi đổi JSON cần nạp lại setup
hoặc cập nhật đúng Variable. Không đặt secret trong cấu hình maintenance.

CLI ngoài DAG có thể dùng `--config /đường/dẫn/iceberg_maintenance.json` với bảng
trong allowlist JSON. Local: `--catalog local --local-warehouse /tmp/warehouse`,
jar đưa bằng spark-submit --jars đã cache. `--help` không cần pyspark.

## Kiểm thử và số đo thật

Pytest Python thuần đã chạy; output đầy đủ nằm trong
report (ghi chú nội bộ, không nằm trong repo). Tracer đầu đỏ do module chưa có, rồi xanh;
các vòng bổ sung có đỏ do thiếu config/hàm probe và một lỗi so timestamp ISO,
sau đó đều xanh.

**Spark local: đã chạy (người vận hành) — `Ran 3 tests in 39.176s OK`.**
Receipt đầy đủ: [`docs/evidence/iceberg-maintenance-local-20261018.json`](../../evidence/iceberg-maintenance-local-20261018.json)
(sha256 hai file code trong receipt khớp file trong repo lúc lưu).

| Đo trên bảng `maintenance_normal` (8.192 dòng, 2 partition) | Trước | Sau |
|---|---|---|
| Số data file | 32 | 12 |
| Số snapshot | 32 | 4 |
| File phải đọc cho probe `seller_sku = 'sku-0001'` | 32 | 2 |
| Fingerprint toàn bảng `sum(xxhash64)` | 253838296152053650978 | 253838296152053650978 (bằng hệt) |
| Fingerprint tag `gen-1` (4.096 dòng) | 106188931843360907061 | 106188931843360907061 (bằng hệt) |

- Lần lập kế hoạch thứ hai: `no_op = true`, chỉ còn `remove_orphans` dry-run (không đổi bảng).
- Retry: `compact` lỗi tạm 2 lần (`CommitFailedException` giả lập) → lần 3 thành công, các bước sau chạy đủ.
- Fatal: lỗi ở hành động đầu → dừng, không chạy tiếp, receipt `status = failed` ghi lỗi.
- Thời gian probe có in trong receipt nhưng **không** dùng làm bằng chứng (nhiễu); bằng chứng là số file phải đọc.

`tests/spark/check_iceberg_maintenance.py` chuẩn bị catalog Hadoop dưới /tmp, extension
Iceberg CALL và jar 1.11.0 trong ivy. Ba ca độc lập tạo bảng có 32 append nhỏ,
2 partition identity, seller_sku/created_at, tag gen-1 sau append 16. Test yêu
cầu giảm file và probe-file, dữ liệu/tag bất biến, expiry giảm snapshot, lần hai
no-op mutation, retry hai lỗi rồi thành công và fatal không đổi snapshot. Validation
dùng newSession và JSON roundtrip. Retention test dùng tuổi 1 giây, chờ 1,1 giây
cho dữ liệu ban đầu cũ hơn ngưỡng, retain_last=3 để giữ append cuối + compact +
zorder; không mock clock để tạo bằng chứng idempotence.

người vận hành chạy:

```bash
docker run --rm --network none --read-only --tmpfs /tmp:rw,size=2g \
  -v platform_airflow-data:/ivy:ro \
  --mount type=bind,src="$PWD",dst=/workspace,readonly --entrypoint python \
  platform-airflow:2.10.5-spark3.5.3 /workspace/tests/spark/check_iceberg_maintenance.py
```

Suite chỉ in `DE_N2_EVIDENCE_JSON=...` sau khi tất cả test xanh. người vận hành có thể lưu
JSON thật kèm code_sha256 vào evidence mới; test không ghi vào repo mount read-only.
Số đo phụ thuộc kích thước file Spark/Parquet thật: các assertion **chưa được xác
nhận** trong sandbox. Không coi việc compile hay pytest là đã kiểm API CALL.

## Giới hạn và việc chạy trên bảng thật

Compaction hỗ trợ partition identity và time (year/month/day/hour) bằng predicate
trên cột nguồn. Bucket/truncate, tên cột nguồn lồng nhau và partition evolution
không được hỗ trợ đầy đủ: preflight từ chối mapping không phù hợp trước CALL.
File partition được thu thành JSON; identity date dùng literal chuỗi có Spark
cast. Tên bảng chỉ nhận các thành phần ASCII an toàn, phải ghi đủ catalog và
namespace. Cột map mà Spark không cho xxhash64 sẽ fail trước procedure; cần chốt
canonicalization rồi bổ sung riêng trước khi áp dụng lên schema đó.

Orphan luôn dry-run mặc định; cutoff ít nhất 72 giờ không đảm bảo đủ an toàn cho
writer chạy lâu hơn 72 giờ, nên chỉ tắt dry-run sau khi người vận hành kiểm cửa sổ ghi.
Không có thử nghiệm API Iceberg thực trong môi trường này; không tuyên bố tương
thích đã được xác thực bằng runtime. Lịch mới chưa chạy, không bật/tắt service,
không chạm Lakekeeper/MinIO và không kích hoạt DAG.

**Chạy trên bảng thật: chưa chạy — người vận hành chạy sau.**

## Trên AWS

DAG [maintenance_iceberg](../../../dags/maintenance_iceberg.py) chạy Compose EC2 trên bảng S3
`bronze.raw_shopee_orders_dp1`, run `scheduled__2026-10-27T00:00:00+00:00`.
[Receipt AWS](../../evidence/aws/data-platform-aws-20261027.json) ghi plan → execute → validate 3/3 success,
`no_op=true`: chỉ kiểm kê orphan dry-run; trước/sau đều 180 file, 4.689.957 dòng, fingerprint không đổi.

[Job maintenance](../../../spark_jobs/maintenance/maintain_iceberg_tables.py) không gộp qua ranh giới partition ngày.
Lần optimize riêng dùng [partition evolution + rewrite-all](../../../spark_jobs/maintenance/repartition_small_channel.py),
180 → 6 file và giữ fingerprint. [Storage AWS](../data_storage.md#trên-aws) ghi số đo và giới hạn;
chưa có ảnh riêng chụp compaction AWS trước/sau, không gọi no-op là optimize thành công.
