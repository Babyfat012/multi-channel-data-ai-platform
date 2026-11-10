# Bronze/Silver nguồn v2 — gói C

> **Pipeline v2 (order lifecycle) — thiết kế và thử ở local, không phải pipeline đã nộp.** Pipeline chạy trên AWS là v1 (DP1 → DP4 + ML), xem [Data Platform evidence](../evidence.md). Các chỗ nhắc GCP dưới đây là kế hoạch cũ, chưa chạy; đích cloud hiện hành là [AWS](../../shared/aws_deploy.md).

Đưa bộ nguồn v2 nhỏ của [gói B](source_order_lifecycle_fixtures.md) vào lakehouse theo
[source contract v2](source_contract_order_lifecycle.md). Đây là **fixture nhỏ** (5 Channel,
3 ngày tạo đơn, 15 Order, 30 Order Line), không phải dữ liệu nghiệm thu
100 GB. Các bảng `_dp1`/`_dp2` làm bằng chứng cũ **không bị đụng**: nguồn v2
ghi vào namespace mới `ingest_bronze` và `build_silver`.

## Thiết kế

| Tầng | Bảng | Grain | Ghi chú |
|---|---|---|---|
| Bronze | `bronze.lifecycle_raw_records` | Một dòng nguồn | **Một bảng chung** mọi feed, giữ nguyên `raw_json`, partition theo `feed`. Thêm feed hay đổi schema không phải sửa bảng. Cột `ingestion_boundary` ghi `file` hoặc `jdbc` |
| Silver | `lifecycle_stg_line_commitments` | Order Line (revision mới nhất) | Số lượng đặt/hủy, tiền đặt/hủy, `headline_product_net_minor` = đặt − hủy trước ship |
| Silver | `lifecycle_stg_allocations` | Allocation | Một line có thể có nhiều allocation (giao tách 2+1) |
| Silver | `lifecycle_stg_deliveries` | Sự kiện giao | Số lượng thực giao theo allocation |
| Silver | `lifecycle_stg_return_events` | Sự kiện return (lịch sử) | requested → approved → received |
| Silver | `lifecycle_stg_refunds` | Giao dịch refund (revision mới nhất) | Refund **tách riêng**, không trừ vào headline; refund chưa gắn line giữ `unresolved_scope_ref` |
| Silver | `lifecycle_stg_listing_versions` | Revision của Listing (lịch sử) | `is_latest` đánh dấu bản đang hiệu lực; correction thay bản sai, bản sai vẫn còn |
| Silver | `lifecycle_stg_stock_operations`, `lifecycle_stg_stock_snapshots`, `lifecycle_stg_inbound_events` | Thao tác kho / snapshot / nhập | Pool Seller chung |
| Silver | `lifecycle_stg_uncertainties`, `lifecycle_stg_coverage` | Bất định / độ phủ nguồn | Đọc từ feed phụ của producer |
| Silver | `lifecycle_stg_line_lifecycle` | Order Line | Gộp allocation/giao/return/refund **về grain line trước khi join**, nên không fan-out |
| Silver | `lifecycle_stg_native_order_lines` | Line trong đơn native | Parse dạng gốc của 5 Channel (Amazon hai schema) ra cột chung: số lượng, tiền gốc, tiền sau giảm giá. Walmart không gửi giá gốc nên `gross_minor` null |
| Silver | `lifecycle_stg_coverage_receipts` | Coverage envelope | Kết quả áp coverage của producer lên đúng tập dòng đã nhận, kèm lý do nếu thiếu |
| Silver | `lifecycle_stg_observed_evidence` | Một bản ghi quan sát | Feed nhãn + coverage + uncertainty đã qua cổng hợp đồng, **mọi revision** (chỉ bỏ retry trùng y); khóa `ev_line_id`/`ev_order_id`/`corrects_source_event_id` để Gold gom theo line. Partition `days(source_observed_at)` |
| Silver | `lifecycle_stg_coverage_validity` | Revision coverage | `complete`/`partial` theo luật `spark_jobs.lib.return_label.source_line`, kiểm một lần cho cả dataset |
| Silver | `lifecycle_stg_build_info` | Một lần dựng | `run_id`, `as_of`; Gold v2 từ chối chạy nếu `as_of` ≠ cutoff |

Quy tắc dùng chung (code ở [`spark_jobs/lib/order_lifecycle_lakehouse.py`](../../../spark_jobs/lib/order_lifecycle_lakehouse.py)):

- **Định danh sự kiện** = `(dataset_version, producer, source_epoch, source_event_id)`.
  Bản lặp cùng nội dung bị bỏ; cùng id mà khác nội dung là **xung đột**, bị loại
  khỏi Silver và làm job lỗi. Hash nội dung bỏ `recorded_at` vì đó là giờ máy
  chạy, không phải sự thật nguồn.
- **Revision lớn nhất thắng**: correction thay thế bản bị sửa, không cộng thêm.
  Ví dụ: sửa số tồn đầu kỳ 200 → 199 thì sổ kho dùng 199, không phải 399.
- **`as_of`**: chỉ dùng dòng có `source_observed_at` trước thời điểm đó, để dựng
  lại đúng những gì đã biết lúc ấy. Snapshot Order native (không có `source_observed_at`) được coi là biết ở
  **cuối ngày export** (`export_date` + 1 ngày). Bản đầu các dòng này bị lọc mất khi có `as_of` ⇒
  `seller_sku` của mọi line thành null (lỗi chỉ lộ khi DAG v2 truyền `--as-of`).

## Walmart qua JDBC

Manifest khai Walmart là DB phòng ban khác (boundary **JDBC**), file JSONL chỉ là
bootstrap để nạp DB. Vì vậy:

1. [`scripts/setup/load_order_lifecycle_walmart.py`](../../../scripts/setup/load_order_lifecycle_walmart.py) nạp file
   bootstrap vào bảng mới `walmart_lifecycle.orders` (khóa `dataset_version` +
   `purchase_order_id`). `raw_payload` kiểu **TEXT**, không JSONB, để DB không sắp
   lại khóa của chuỗi gốc. Nạp lại cùng `dataset_version` thì xóa rồi chèn trong
   một transaction, không nhân đôi. Bảng v1 `walmart.orders` không bị đụng.
2. Job `spark_jobs/bronze/ingest_order_lifecycle.py` với `--walmart-jdbc walmart_lifecycle.orders` bỏ dòng Walmart từ file, đọc qua
   JDBC bằng `json_build_object(...)::text`, và dừng lỗi nếu tập mã đơn khác
   `native_sources.walmart.snapshot_scope.order_ids` (thiếu, thừa hoặc trùng).

## Đối soát — mọi check phải bằng 0

Job `spark_jobs/silver/build_order_lifecycle.py` đối soát trên **bảng vừa ghi** (đọc lại từ
catalog), check nào khác 0 thì job lỗi.

| Check | Ý nghĩa |
|---|---|
| `event_conflicts` | Cùng id sự kiện mà khác nội dung, hoặc cùng entity/revision mà khác id |
| `missing_dependencies` | Sự kiện trỏ tới sự kiện gốc không có (`depends_on`, `corrects`) |
| `duplicate_lines`, `orphan_allocations` | Line trùng; allocation không có line |
| `quantity_mismatch_lines` | Line đã kết thúc mà đặt ≠ hủy + đã phân bổ |
| `delivery_mismatch_allocations` | Allocation đã giao mà số giao ≠ số phân bổ, hoặc giao vượt |
| `fanout_line_rows`, `fanout_headline_delta_minor` | Bảng line lifecycle lệch số dòng hoặc tổng headline so với bảng line |
| `refund_unscoped_rows` | Refund chưa gắn line mà **không** giữ vùng bất định (bị bỏ im lặng) |
| `refund_exceeds_headline_lines` | Refund sản phẩm thành công vượt headline của line |
| `stock_snapshot_mismatches` | Cộng dồn thao tác kho tới thời điểm snapshot ≠ snapshot |
| `listing_binding_orphans` | Line bán theo mapping Listing không tồn tại |
| `native_unparsed_rows` | Đơn native không parse ra được mã đơn/mã line |
| `native_lines_without_commitment`, `commitments_without_native` | Line chỉ có ở một phía: Channel nói có mà Seller không ghi, hoặc ngược lại |
| `native_quantity_mismatches`, `native_money_mismatches` | Số lượng / tiền sau giảm giá / tiền tệ của đơn native khác số **đặt ban đầu** của OMS (native là ảnh chụp lúc tạo đơn, nên không so với số sau hủy) |
| `coverage_receipt_failures` | Envelope `complete` mà tập dòng nhận được thiếu/thừa số thứ tự, sai số dòng, ngoài khoảng sự kiện, hoặc hash cả tập khác `record_set_sha256` |
| `feeds_without_complete_coverage` | Feed có sự kiện mà không có coverage nào đạt |

Coverage `unavailable` (scope pool Channel quản lý, không có quyền nguồn vận hành)
không bị kiểm mà được đếm ở `info.coverage_unavailable_scopes`; ở đó dữ liệu là
**không biết**, không phải 0.

## Kết quả

**Test Spark** (27 ca, image `platform-airflow:2.10.5-spark3.5.3`, `local[1]`, không
mạng, workspace chỉ đọc, fixture sinh mới trong `/tmp`): **27/27 OK**. Ngoài ca
sạch, test cố tình làm hỏng dữ liệu và kiểm check bắt được: bản lặp do retry,
cùng id khác nội dung, mất sự kiện gốc, mất allocation thứ hai, correction tồn
kho, tiền Walmart lệch, mất một đơn Shopee, thiếu một dòng kho (coverage báo
`missing_sequence` + `record_count_mismatch`), sửa lén nội dung một dòng giao
hàng (coverage báo `record_set_digest_mismatch`); Amazon schema 2 parse ra cùng
kết quả; dòng Walmart dạng JDBC (JSON có khoảng trắng) thay dòng file vẫn
parse và đối soát sạch; và kiểm `as_of` trước correction refund thấy **15** refund còn bất định có
giữ vùng ảnh hưởng, sau correction còn **0**.

```bash
docker run --rm --network none --read-only --tmpfs /tmp:rw,size=1g \
  --mount type=bind,src=$PWD,dst=/workspace,readonly --entrypoint python \
  platform-airflow:2.10.5-spark3.5.3 /workspace/tests/spark/check_order_lifecycle_silver.py
```

**Chạy thật vào Lakekeeper/MinIO** trên biến thể `default`
(`dataset-9b9767a38a44fce25446af2afb6836e6`):

- Nạp PostgreSQL hai lần liên tiếp: file 3 = DB 3 cả hai lần.
- Bronze: manifest **550** dòng nguồn = Bronze **550** (file 547 + **JDBC 3**), lỗi 0;
  Walmart JDBC thiếu/thừa/trùng 0; run `bronze-v2-20261018-jdbc`
  ([receipt](../../evidence/bronze-v2-jdbc-20261018.json)).
- Silver: 489 sự kiện vận hành, 0 bản lặp, **30** Order Line, **45** allocation,
  headline **750.000** minor USD (= 15 × (20.000 + 30.000)), refund chưa gắn
  line 0, bất định chưa giải quyết 0; **30** line native khớp 30 line OMS;
  coverage **9** complete, **6** scope unavailable; **19/19 check bằng 0**, run
  `silver-v2-20261018-jdbc`. Receipt: [`silver-v2-receipt-20261018.json`](../../evidence/silver-v2-receipt-20261018.json).

```bash
env -u POSTGRES_PASSWORD docker compose --profile catalog up -d postgres minio lakekeeper
export FIXTURE=out/source-v2-b-final-20261018-051930425055/default
env -u POSTGRES_PASSWORD .venv/bin/python scripts/setup/load_order_lifecycle_walmart.py --root $FIXTURE
scripts/run/run_order_lifecycle_spark.sh /opt/spark_jobs/bronze/ingest_order_lifecycle.py --root /data/order_lifecycle \
  --run-id bronze-v2-20261018-jdbc --walmart-jdbc walmart_lifecycle.orders
scripts/run/run_order_lifecycle_spark.sh /opt/spark_jobs/silver/build_order_lifecycle.py --run-id silver-v2-20261018-jdbc \
  --receipt /receipts/silver-v2-receipt-20261018.json
```

Fixture nằm trong `out/` (bị Git bỏ qua); tái tạo bằng
`python -m generator.generate_order_lifecycle_fixture --output <thư mục mới>` theo [gói B](source_order_lifecycle_fixtures.md).

## Giới hạn — chưa làm trong bước này

- JDBC Walmart đọc một lượt, chưa chia partition song song như job v1 (fixture
  chỉ 3 đơn); chưa có receipt snapshot của chính DB (LSN/txid).
- Coverage đã được kiểm trong job, nhưng chưa có bước publish để chặn; hiện job
  chỉ báo lỗi.
- Tiền native được đổi từ chuỗi thập phân theo scale 2 (USD trong fixture);
  chưa đọc bảng currency để xử lý tiền tệ có scale khác.
- Chưa có Gold, nhãn v2 (gói E), DAG Airflow, DataHub cho các bảng v2.
- Bronze ghi đè toàn bảng mỗi lần chạy, chưa nạp tăng dần theo run.
- Chạy `--as-of` sẽ ghi đè cùng các bảng Silver; bằng chứng `as_of` hiện nằm ở
  test Spark, chưa có bảng riêng theo thời điểm.
