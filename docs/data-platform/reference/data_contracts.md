# Data contracts — Data contract, schema registry và cổng cách ly Silver v2

Gói G đưa hợp đồng nguồn v2 vào `contracts/`, tự so tiến hóa schema và cách ly dữ
liệu chưa hỗ trợ trước khi parse. Màn trình diễn dùng fixture nhỏ của
[gói B](source_order_lifecycle_fixtures.md), không phải bằng chứng quy mô 100 GB.

## Vấn đề nguồn và mục tiêu

Ba lỗ hổng được ghi trong lượt rà soát:

1. Payload đơn native không mang `schema_id`; nhãn nằm trong
   `manifest.native_sources[channel]`. Nếu chỉ đọc envelope thì mất nhãn nguồn.
   Nơi producer tạo nhãn hiện tại: `generator/generate_order_lifecycle_fixture.py:625`; nơi Bronze lấy
   nhãn dự phòng sau sửa: `spark_jobs/lib/order_lifecycle_lakehouse.py:71`.
2. Silver trước gói G đoán schema Amazon bằng hình dạng payload và luôn parse
   Shopee theo v1; đổi tên field có thể ra null im lặng. Sau sửa, điểm chọn parser
   ở `spark_jobs/lib/order_lifecycle_lakehouse.py:132` dùng `contract_parser`, được cổng
   `spark_jobs/lib/order_lifecycle_lakehouse.py:341` lấy từ registry.
3. Schema chỉ tồn tại trong code Spark, chưa có dữ liệu độc lập để máy so hai
   phiên bản. Registry mới ở `contracts/registry.json`, thuật toán so ở
   `spark_jobs/lib/data_contracts/compat.py:1`. Rủi ro này từng hiện hữu ở dữ liệu v1:
   mô tả đợt thay dạng Amazon kéo dài 81 ngày nằm ở `docs/data-platform/reference/silver.md:463`.

Các dẫn chiếu code trên mô tả vị trí hiện tại sau sửa; ba lỗ hổng là trạng thái
trước gói G. Mục tiêu là giữ nguyên mọi dòng nguồn ở Bronze, đưa dòng không đạt
hợp đồng sang bảng cách ly và tiếp tục xử lý các Channel còn đủ điều kiện.

## Thiết kế

| Thành phần | Trách nhiệm |
|---|---|
| `contracts/registry.json` | Subject, compatibility, version, schema, parser, status, đường dẫn định danh native |
| `contracts/schemas/` | Tập con JSON Schema: type, properties, required, items; required là giao các trường quan sát |
| `spark_jobs/lib/data_contracts/schema.py` | Infer, validate toàn bộ bản ghi trong Python, dựng required JSON paths cho Spark |
| `spark_jobs/lib/data_contracts/compat.py` | Diff và phán quyết theo hướng đọc cũ/mới |
| `spark_jobs/lib/data_contracts/registry.py` | Tra schema, tìm thư mục contracts, kiểm metadata và luật dùng lại parser |
| `spark_jobs/lib/data_contracts/gate.py`, `spark_jobs/lib/data_contracts/__main__.py` | Cổng Python và CLI infer/diff/check-registry/gate, không cần Docker |
| `spark_jobs/lib/order_lifecycle_lakehouse.py` | Bronze gắn nhãn, cổng đầu Silver bằng biểu thức Spark, parse theo registry, đối soát |
| Job `spark_jobs/silver/build_order_lifecycle.py` | Ghi `silver.lifecycle_quarantine_records`, đối soát bảng đọc lại từ catalog và xuất receipt |

Package nằm trong `spark_jobs/` để dùng được cả `import data_contracts` trong
container và `import spark_jobs.lib.data_contracts` từ repo. Nội bộ package dùng
import tương đối và chỉ thư viện chuẩn, không có service hoặc dependency mới.

Bronze thêm `contract_schema_id`: ưu tiên nhãn trong dòng, rồi nhãn feed native
trong manifest, kể cả Walmart JDBC. Bronze không lọc vì vi phạm hợp đồng.
Silver tra schema rồi chia accepted/quarantined **trước** khi parse; không dùng
Python UDF. Amazon có hai parser riêng. TikTok v2 thêm `buyer_message` tùy chọn,
được đăng ký với parser `tiktok_shop/1`, nên không cần nhánh code cho phiên bản mới.
Shopee v2 đổi `order_sn` và `model_sku` thành `order_number` và `sku`; phiên bản
này được sinh để demo và **không có** trong registry chính thức.

Registry gồm sáu schema native ban đầu, TikTok v2, chín feed vận hành và hai feed
phụ coverage/uncertainties để cổng không cách ly nhầm chúng. Amazon dùng mode
`NONE` vì hai export đã có parser riêng từ trước; các subject khác `BACKWARD`.

Thứ tự tìm contracts dùng chung cho `Registry()` và CLI cần registry:
`--contracts-dir` → `PLATFORM_CONTRACTS_DIR` → `/opt/contracts` nếu thư mục tồn tại →
`contracts/` cạnh gốc repo tính từ vị trí package, không phụ thuộc thư mục chạy.
Job `spark_jobs/silver/build_order_lifecycle.py` truyền mặc định `None` để sử dụng cùng quy tắc.

### Luật tiến hóa schema

| Mode | Ý nghĩa | Thay đổi phá vỡ |
|---|---|---|
| `BACKWARD` | Reader mới đọc được dữ liệu cũ | Thêm field required, đổi kiểu, optional thành required |
| `FORWARD` | Reader cũ đọc được dữ liệu mới | Xóa/đổi tên field required cũ, đổi kiểu, required cũ thành optional |
| `FULL` | Đạt cả hai hướng | Hợp các thay đổi phá vỡ của hai hướng |
| `NONE` | Không kiểm tương thích | Không chặn theo compatibility |

`migrations` chỉ giải thích đường dẫn đổi tên để diff hiện `field_renamed`,
không biến đổi tên thành tương thích. Version compatible được dùng lại parser;
version breaking phải có parser khác hoặc `null` (chưa hỗ trợ). `check-registry`
trả mã khác 0 nếu version breaking dùng lại parser trước.

### Ba lý do cách ly, theo đúng thứ tự

| Reason | Điều kiện |
|---|---|
| `unregistered_schema` | Thiếu nhãn hoặc nhãn không có trong registry |
| `no_parser` | Có schema nhưng parser null hoặc Silver chưa có nhánh parser đó |
| `contract_violation` | Có parser nhưng thiếu đường dẫn required; `detail` ghi đường dẫn |

Python gate còn kiểm cả kiểu JSON và mọi phần tử mảng. Spark gate kiểm các
required path bằng `get_json_object(...).isNull()`, không kiểm đầy đủ kiểu.
Bảng cách ly giữ `dataset_version, feed, producer, contract_schema_id, reason,
detail, source_file, record_sha256, raw_json`. Receipt có `quarantine.total`,
`by_reason`, `by_schema_id`; **số cách ly không làm fail job**. Bất biến đối soát
khác 0 vẫn làm job lỗi.

### Đối soát và định danh bản ghi cách ly

`native_record_balance = native_records_total - parsed_native_records -
quarantined_native_records` phải bằng 0 để không mất dòng im lặng.
`identity_path` là metadata tùy chọn của version, nếu có phải là chuỗi bắt đầu
bằng `$.`. Các schema native đã đăng ký đều có đường dẫn của chính chúng.
`_native_order_identity(frame, registry)` chỉ dùng đường dẫn theo đúng
`contract_schema_id`, không đoán payload của schema lạ.

Nếu đọc được mã đơn, `quarantined_orders` đếm **mã đơn phân biệt** theo Channel
và tách chúng khỏi tập OMS kỳ vọng. Nếu mã đơn null, toàn scope
`(dataset_version, channel)` được loại khỏi đối soát native↔OMS; số dòng được
báo ở `info.quarantined_native_records_without_order_identity`. Không gọi số
này là số đơn. Cả hai DataFrame và registry snapshot đi qua dict `silver`; job
21 giữ snapshot khi tạo dict đọc lại bảng nên không cần đổi chữ ký `reconcile`.

Ca Shopee v2 phải báo **3 bản ghi cách ly không rõ mã đơn**, không phải ba đơn.
Ca producer giữ nhãn Shopee v1 nhưng đổi tên `order_sn` cũng không đọc được mã
đơn; kỳ vọng là một bản ghi không rõ mã đơn, scope Shopee được loại khỏi đối
soát native↔OMS. Các Channel khác vẫn phải đối soát sạch. Đây là **kỳ vọng test
Spark sau sửa**, chưa phải số đo chạy mới.

## Kết quả Python đo lại

Chạy `scripts/run/run_data_contracts_demo.sh`, không Docker:

| Fixture | Accepted | Quarantined | Reason |
|---|---:|---:|---|
| default | 550 | 0 | Không có |
| shopee=2 | 547 | 3 | `unregistered_schema` |
| tiktok_shop=2 | 550 | 0 | Không có |

`check-registry`: exit 0, không lỗi. Diff Shopee v1 → v2 theo `BACKWARD`: exit 1,
liệt kê hai field removed (`order_sn`, `model_sku`) và hai field added required
(`order_number`, `sku`) tại các đường dẫn order/item tương ứng.

Lệnh, stdout/stderr nguyên văn, mã exit, git HEAD và SHA-256 của code/registry
nằm trong [evidence Data contracts](../../evidence/contracts-validation-20261018.json). Script hiện hành tạo receipt có timestamp tại `out/data-contracts-evidence/`; giữ nguyên file evidence lịch sử khi chạy lại.

Pytest riêng `tests/unit/spark_jobs/lib/test_data_contracts.py tests/unit/generator/test_generate_order_lifecycle_fixture.py`: **23 passed**.
Toàn repo tại lúc chạy: **214 passed, 2 failed**; cả hai lỗi thuộc
`tests/unit/spark_jobs/lib/test_maintenance_policy.py` của gói làm song song (thiếu config và hàm
chưa có tại thời điểm kiểm). Không sửa file gói đó. Output đầy đủ nằm trong
báo cáo (ghi chú nội bộ, không nằm trong repo).

## Spark — người vận hành xác nhận riêng

Lượt chạy trước sửa `identity_path` bị người vận hành dừng vì đã lỗi thời (không dùng làm bằng chứng).

**Sau sửa (người vận hành): `Ran 32 tests in 2749.990s — OK`** — 27 ca Silver v2 cũ + 5 ca hợp đồng, chạy
`tests/spark/check_order_lifecycle_silver.py` trong `platform-airflow:2.10.5-spark3.5.3`, `--network none --read-only`,
`PYSPARK_SUBMIT_ARGS='--driver-memory 3g pyspark-shell'`. Thời gian dài vì máy chạy song song nhiều job (bản cũ 27 ca
chạy riêng mất 258 s); 1 GB driver từng gây `UNABLE_TO_ACQUIRE_MEMORY` khi chạy chồng nên đã nâng 3 GB.

Các test đã sửa để người vận hành chạy lại là
`test_contract_unknown_shopee_keeps_other_channels`,
`test_contract_producer_lies_about_schema`,
`test_contract_registered_but_missing_parser`. Các test default và TikTok
compatible cùng 27 ca cũ vẫn nằm trong harness; không khẳng định chúng đã pass
với bản sửa này. Lệnh cho người vận hành:

```bash
docker run --rm --network none --read-only --tmpfs /tmp:rw,size=1g \
  --mount type=bind,src=$PWD,dst=/workspace,readonly --entrypoint python \
  platform-airflow:2.10.5-spark3.5.3 /workspace/tests/spark/check_order_lifecycle_silver.py
```

## Giới hạn và phần để gói I

- Spark chỉ kiểm phần tử mảng đầu `[0]`; không suy từ cổng này rằng mọi item đều
  hợp lệ. Python validator kiểm toàn bộ phần tử; mảng rỗng và field nullable
  không bị coi là thiếu required chỉ vì không có item/null.
- Registry là file quản lý trong Git, chưa có service registry, discovery hoặc
  tự đăng ký schema mới. Infer từ fixture phản ánh mẫu đã quan sát, cần rà tay.
- Chưa có DAG/DataHub riêng cho contracts/quarantine; để gói I. Không thay DAG
  hiện có trong gói này.
- Khi bản ghi không rõ mã đơn, loại cả scope Channel khỏi đối soát native↔OMS
  làm giảm phạm vi kiểm tra; receipt nêu rõ số bản ghi để không hiểu nhầm là
  Channel đã đối soát đầy đủ.
- Chưa đo hiệu năng/quy mô lớn, chưa chạy Spark sau sửa, chưa ghi bảng Iceberg
  thật cho bản sửa identity_path. Demo Python không thay thế các bằng chứng đó.
