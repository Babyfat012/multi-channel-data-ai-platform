# Source v2 — fixture nhỏ của gói B

> **Pipeline v2 (order lifecycle) — thiết kế và thử ở local, không phải pipeline đã nộp.** Pipeline chạy trên AWS là v1 (DP1 → DP4 + ML), xem [Data Platform evidence](../evidence.md). Các chỗ nhắc GCP dưới đây là kế hoạch cũ, chưa chạy; đích cloud hiện hành là [AWS](../../shared/aws_deploy.md).

Triển khai local của [source contract v2](source_contract_order_lifecycle.md), giữ scope
Q52–Q61. Entry point riêng: [generator/generate_order_lifecycle_fixture.py](../../../generator/generate_order_lifecycle_fixture.py).
Không thay engine Batch/Stream v1 và không dùng root nguồn/evidence lịch sử.

```bash
.venv/bin/python -m generator.generate_order_lifecycle_fixture --output out/source-v2-MOT-ROOT-CHUA-TON-TAI
.venv/bin/pytest -q
```

API `generate_fixture(root, FixtureConfig(...), delivery_parts=1, native_schema=1)`
chỉ nhận đường dẫn local mới, kể cả thư mục rỗng đã tồn tại cũng bị từ chối.
Không có resume/restart, Cloud IO, DB load, service startup hoặc sinh full-scale.
Creation mặc định 3 ngày, giới hạn 2–7 ngày; 2 SKU, 1 Order/Channel/ngày,
2 line/Order. Năm Channel: Amazon, Walmart, Shopify, TikTok Shop, Shopee.
Fixture là bộ tình huống xác định, không phải phân bố kinh doanh để benchmark.

## Nguồn và identity

`identity.json` được ghi trước các file dữ liệu: pin config, seed, population/world
và physical outcome digest, contract/code/Python digest/version, module versions,
required/optional source profiles, renderer config và ba horizon UTC exclusive.
`manifest.json` chốt danh sách file, SHA-256/bytes/counts, authority/capability/scope,
source snapshots, coverage và diagnostics. Layout operational theo contract:
`dataset_version=.../producer=.../feed=.../export_date=.../part-00000.jsonl`.
Coverage do đúng producer sở hữu feed xuất; không cấp quyền nguồn vận hành cho
pool Channel-managed chỉ vì engine biết world.

Các native Orders là **synthetic native-shaped fixture schemas**
`sim-native-v2/<channel>/<version>`, không là chứng cứ gọi API thật hoặc dữ liệu
byte-compatible với renderer v1. Đây là frozen creation observations, không claim
status refresh. Native chỉ chứa fields Orders; delivery/approval/refund/stock xuất
ở feed riêng. Fixture dùng USD/scale2 công bố trong currency table; không FX hoặc
cộng các currency. Walmart là JSONL bootstrap có cột `purchase_order_id`,
`raw_payload`, `fetched_at`; manifest vẫn pin boundary **JDBC**. Chưa nạp DB,
chưa có receipt source snapshot của DB/JDBC, không thay boundary đó bằng file.

Envelope operational giữ revision, producer epoch/sequence, causal refs,
effective/observed business clock và recorded execution clock. Sequence theo
observation của từng producer. Native/account identity và stable units không phụ
thuộc SKU mapping hay số lần renderer chia record. Business line key dùng tuple
JSON versioned có dataset/seller/Channel/account/native Order/full native line key.
Hash fact loại riêng execution-only `recorded_at`; checksum file/coverage hash toàn
bộ emitted record, nên hai lần tái tạo cùng fact vẫn dedup dù execution time khác.
`require_same_world` bắt buộc cùng world ID **và** physical outcome digest;
seed bằng nhau không đủ.

## Các tình huống đã xuất và kiểm

- Line A: ordered3, cancel trước ship1, delivered2, approve/receive1; gross330,
  discount30, ordered300, canceled100 → headline200. Product Refund100 và
  shipping Refund10 riêng cùng transaction; không trừ refund khỏi headline.
- Line B: ordered3, hai allocation/shipment 2+1, giao hai ngày khác nhau;
  terminal sau phần giao cuối. Delivery renderer có thể chia record cùng unit/time.
- Refund ban đầu biết Order nhưng thiếu allocation, giữ cả line A/B trong scope
  uncertainty; correction liên kết A đến muộn, không sửa record cũ. Coverage
  refund revision1 complete về transport nhưng còn uncertainty; revision2 sạch
  sau observation của correction. Interval refund bao phủ quá deadline30d của
  mọi allocation, còn các feed khác close theo follow-up horizon.
- Listing opening trước sale; business change có effective trước observed,
  correction đổi attribution; sale binding, line ID và pool reservation cũ giữ
  nguyên. Năm Storefront dùng chung một pool: opening200/SKU chỉ ghi một lần,
  không cộng thành1000. Reserve/release/ship/receive/quarantine/disposition,
  inbound promise5/receipt2/close thiếu3 và snapshot đối soát đều là feed riêng.
- `fixture.probes` có role `diagnostic_fixture`, tách khỏi nguồn complete:
  retry, hash conflict, release-before-reserve, missing dependency/revision,
  correction stock đảo thứ tự, native current_quantity2 rồi full snapshot mất
  line. Đây là đầu vào âm cho C/D/E; không ingest probes như feed production.

`inspect_records` chỉ kiểm identity/revision/reference, trả ready/pending/conflict,
không apply stock hay tính nhãn. `coverage_receipt` đối chiếu emitted set với
expected partitions/sequences/count/digest và cutoff strict observed<C; thiếu
capability trả unavailable, thiếu dữ liệu trong capability đã claim trả partial.
Complete không xóa uncertainty hay tự chứng minh official negative.

Ba horizon mặc định là creation `D0,D3)`, follow-up `[D0,D90)`, observation
`[D0,D105)`. Physical fulfillment tối đa4d, qualification30d inclusive, allowance15d,
source observation delay không vượt15d. Source event overflow hoặc tail cấu hình
quá ngắn bị reject trước khi tạo root; không truncate rồi cấp complete. Manifest
công bố counts terminal/maturity-deadline/censored theo cohort, actual max delay,
tail bytes và runtime. Counts maturity không phải label eligibility.

## Artifact bàn giao

Root mới: `out/source-v2-b-final-20261018-051930425055/`, xem
[index `out/source-v2-b-final-20261018-051930425055/index.json` (local, không commit).
Ba biến thể `default`, `render-split` (Amazon schema2/delivery split),
`approve-d31`; mỗi biến thể 15 Orders, 30 lines, 2 SKU, đủ5 Channel;
tổng source JSONL **2.511.009 byte**, 62 file/biến thể. Default/split cùng
physical world; D29→D31 cùng intent digest nhưng khác outcome/world, dùng để
kiểm qualification, không kết luận defect thay đổi. Output `out/` được git-ignore.
Một root thử trước khi siết delay cap cũng được giữ nguyên, không resume/restart;
chỉ root `source-v2-b-final-...` trên là artifact bàn giao của code hiện tại.

Test [tests/unit/generator/test_generate_order_lifecycle_fixture.py](../../../tests/unit/generator/test_generate_order_lifecycle_fixture.py) đi đỏ→xanh theo từng
lát, assertions từ frozen emitted source và đối soát độc lập quantity/stock/money;
không đọc oracle truth để dựng expected nhãn. Phần tiếp theo là C/D/E/J:
Bronze/Silver/JDBC ingestion, projections/reconciliation, label v2/eligibility,
stream recovery và publication receipts. Chưa claim runtime các phần này,
full-scale, Cloud hoặc benchmark năm Channel cho nhãn/stock.
