# Nguồn v2 ở quy mô lớn — gói B+ (2026-10-19)

> **Pipeline v2 (order lifecycle) — thiết kế và thử ở local, không phải pipeline đã nộp.** Pipeline chạy trên AWS là v1 (DP1 → DP4 + ML), xem [Data Platform evidence](../evidence.md). Các chỗ nhắc GCP dưới đây là kế hoạch cũ, chưa chạy; đích cloud hiện hành là [AWS](../../shared/aws_deploy.md).

Mở rộng bộ nguồn v2 để đo Spark baseline/skew, thử tối ưu lakehouse/warehouse,
stream và bảo trì Iceberg với dữ liệu đa dạng. Đây là nguồn giả lập có seed;
chưa phải bằng chứng nghiệm thu 100 GB hay kết quả Spark.

## Thiết kế

API `generate_scaled(output_root, config: ScaleConfig) -> manifest` trong
[`generate_order_lifecycle.py`](../../../generator/generate_order_lifecycle.py). `ScaleConfig` là frozen
dataclass; `validate()` kiểm tham số trước khi tạo thư mục, `estimate()` trả số
bản ghi/byte dự kiến và trần worst case. Chỉ stdlib, RNG riêng `random.Random(seed)`.

| Tham số | Mặc định | Miền / ý nghĩa |
|---|---|---|
| `seed` | 7 | Số nguyên, tái lập riêng từng world |
| `start` | 2026-01-01 00:00 UTC | Timestamp có timezone UTC |
| `days` | 10 | 1..180 ngày tạo đơn, khoảng exclusive |
| `orders_per_day` | 200 | 1..20.000, tổng mọi Channel |
| `sku_count` | 100 | 2..5.000, catalog dùng chung Seller |
| `sku_zipf_s` | 1,2 | 0 là đều, >0 cho SKU nóng theo rank |
| `channel_weights` | (1,1,1,1,1) | Amazon, Walmart, Shopify, TikTok Shop, Shopee; trọng số không âm, tổng >0 |
| `lines_per_order` | (0,55; 0,30; 0,12; 0,03) | Trọng số cho 1, 2, 3, 4 line, cấu hình qua API |
| `followup_days` | 270 | Mốc từ start, ≥ days + 50 |
| `observation_days` | 285 | Mốc từ start, ≥ followup_days + 9 |
| `native_schemas` | () | Tuple `(Channel, version)`; override giống fixture |

Ba horizon có cùng ngữ nghĩa với fixture. Mặc định 270/285 thay 90/105 để
bao phủ cả miền 180 ngày. Thời gian tạo đơn rải tới microsecond trong ngày;
số lượng đặt mỗi line vẫn 3 và tiền dùng minor integer/chuỗi decimal như fixture.

Trần **20 triệu bản ghi nguồn** được chặn trước bằng bound bảo thủ:
`days × orders_per_day × (24 × max_lines_supported + 1) + sku_count × 11 + 32 + 9 × ceil(followup_days / 7)`.
`max_lines_supported` xét trọng số khác 0. Bound có thể từ chối cấu hình mà
kỳ vọng lấy mẫu dưới 20 triệu. Truth không tính là nguồn.

## Kịch bản

Mỗi line chọn đúng một kịch bản; các dòng lifecycle luôn dùng bộ phát chung
`_emit_sources`. Cửa sổ dùng UTC chính xác 30 × 24 giờ theo
[contract nhãn](return_label_contract.md). Không có label trong cây nguồn.

| Kịch bản | Tỉ lệ | Sự kiện / kết quả vật lý |
|---|---:|---|
| `delivered` | 30% | Giao đủ 3, không hậu mãi |
| `partial_cancel` | 10% | Hủy 1 trước ship, giao 2 |
| `split_delivery` | 12% | Hai allocation 2+1, giao ở created+3 và +4 ngày |
| `return_with_refund` | 7% | Requested → approved ở delivery+29 ngày → received, **kèm** product refund thành công (ca phổ biến nhất ngoài đời) |
| `return_only` | 3% | Như trên nhưng **không** refund (đổi hàng / voucher / bị từ chối sau kiểm) |
| `return_after_window` | 7% | Approved ở delivery+31 ngày; không refund trong cửa sổ |
| `product_refund` | 8% | Product refund success 10.000 minor ở delivery+29 ngày, gắn allocation; không Return |
| `refund_retry` | 5% | Product refund failed rồi success cùng transaction, revision 1→2 |
| `late_observation` | 8% | Giao đủ; quan sát carrier trễ ngẫu nhiên 1 giờ..4 ngày, lan truyền qua dependency |
| `correction` | 6% | Return approved; refund ban đầu chưa gắn line, correction revision 2 và uncertainty resolved; shipping refund riêng |
| `right_censored` | 4% | Commitment/reservation/planned allocation còn mở đến hết horizon; không tự suy đã giao |

`failed` là giá trị của trường `status` có sẵn; fixture cũ chỉ phát `success`.
Không thêm event type hay trường payload. Retry dùng `state_revision`, correction
thật dùng `operation=correction` và `corrects_source_event_id`.

Manifest ghi trọng số, seed, toàn bộ tham số, số lượng và tỉ lệ thực tế. Tỉ lệ là
lấy mẫu; test 5.000 line chấp nhận ±3 điểm %. Zipf kiểm top SKU >10% ở s=1,2;
s=0 kiểm top SKU <2 lần trung bình.

## Ghi nguồn và bộ nhớ

Pass đầu hash world từng ngày để chốt identity; pass sau dùng cùng seed, phát một
ngày rồi ghi vào SQLite tạm trong output root. SQLite là stdlib, cache giới hạn
8 MiB và temp sort trên đĩa. Sau đó duyệt theo known-time, gán sequence liên tục
cho từng producer, ghi JSONL theo `export_date=source_observed_at[:10]`. SQLite
được xóa sau export. Không giữ world hoặc event của mọi ngày trong RAM.

Opening stock một lần cho mỗi SKU, cùng pool Seller; lượng mở đủ cho worst case.
Listing phát một lần cho mỗi `(Channel, SKU)`. Inbound và snapshot ở cuối follow-up,
sau mọi movement đã phát; reserve của line còn mở được giữ trên snapshot.
Coverage hash JSON array canonical sắp theo event ID qua iterator trên SQLite;
sequence ranges được nén khi liên tiếp. Bộ nhớ còn phụ thuộc kích thước một ngày,
catalog, snapshot order IDs và metadata/ranges coverage. Disk tạm cùng filesystem
với output cần khoảng dung lượng nguồn cộng spool và chỉ mục.

Coverage phát **hàng tuần nối tiếp**, mỗi feed, không cộng dồn. Tuần cuối phủ đến
hết follow-up; observed sau cuối tuần +1 ngày hoặc sau dòng trễ cuối cùng +1µs,
lấy mốc muộn hơn. SQLite index `(feed,effective)` chọn tập mỗi tuần rồi hash theo
ID, nên mỗi dòng chỉ hash một lần. Empty week có count0/digest `[]`, các ranges
có thể rời nhau vì sequence producer liên tục dùng chung nhiều feed.

Silver kiểm interval trước digest, join bucket ngày UTC để tránh so mọi dòng với
mọi tuần. Policy nhãn ghép các tuần liền nhau, gap/partial vẫn unknown. Gold chỉ
đưa coverage giao cửa sổ delivery+30d vào input line và giữ revision liên quan.
Đo seed7 `days=60, orders_per_day=4, sku_count=3`: 367 line, **131 target thật**
có history PIT, tối đa 30 coverage/line; [chi tiết và lệnh Spark](gold_order_lifecycle.md).
Giá phải trả: nhiều envelope/file hơn, nhãn chờ đóng tuần và độ trễ, thêm phép
union interval; ADR-0013 ghi lựa chọn.

Layout và envelope giữ tương thích fixture: 9 operational feeds, coverage,
uncertainties và 5 native feeds. Feed không có sự kiện vẫn có JSONL rỗng. Native
snapshot manifest giữ full order scope; Walmart là JSONL bootstrap để nạp DB,
chưa phải bằng chứng JDBC. Shopee schema 2 vẫn là override phá vỡ như fixture;
Amazon 2 và TikTok Shop 2 dùng đúng render tương ứng.

`_truth/lines.jsonl` nằm ngoài `dataset_version=*/`; manifest ghi
`truth.not_a_source=true`, số dòng và hash. Mỗi dòng chứa line identity, SKU,
kịch bản, lượng đặt/hủy/giao/trả, approved/refund-success time. Chỉ test đọc truth.
Pytest quét job Data Platform để chặn tham chiếu đường dẫn `_truth/`; tên `ground_truth`
của job v1 hiện có không phải thư mục truth của scale.

## Giữ fixture mặc định không đổi và tái lập

Trước refactor đã lưu fixture `/tmp/x-before`, physical digest, dataset ID và
SHA-256 mọi file trong manifest. Bộ regression ghim toàn bộ digest tại
[`tests/unit/generator/order_lifecycle_fixture_digests.json`](../../../tests/unit/generator/order_lifecycle_fixture_digests.json).
Fixture có execution `recorded_at`; test cố định đồng hồ ở
2026-10-19T00:00:00Z để so byte có ý nghĩa. Hash fact và dataset fixture mặc định giữ nguyên; `contract_sha256` fixture ghim
baseline lịch sử, không đổi theo tài liệu mới mô tả coverage tuần của scale.
Runtime giây và manifest execution timestamp vốn thay đổi theo lần chạy.

Nguồn scale đóng băng `recorded_at` tại mốc `source_observation_end` để cùng seed,
tham số, code và Python tái tạo cùng file SHA-256. Đây là timestamp của export
mô phỏng; manifest vẫn ghi giờ thực thi thật, chính sách này được ghi riêng.
Native schema override đổi dataset identity nhưng giữ physical digest.

```bash
.venv/bin/python -m generator.generate_order_lifecycle \
  --output /tmp/scale-v2-new --orders-per-day 200 --days 10 --seed 7 \
  --sku-count 100 --sku-zipf-s 1.2 \
  --channel-weights amazon=3,walmart=2,shopify=1,tiktok_shop=1,shopee=1 \
  --native-schema-for amazon=2
.venv/bin/pytest -q tests/unit/generator/test_generate_order_lifecycle_fixture.py tests/unit/generator/test_generate_order_lifecycle.py
```

Thư mục đích phải mới. CLI in estimate trước khi sinh. Estimate dùng pilot tối đa
100 đơn qua chính renderer, cộng chi phí inventory và kỳ vọng số Listing; không
phải cam kết tuyệt đối cho mọi seed cực nhỏ.

## Kết quả đo

**Baseline trước coverage tuần** (số byte/file/dataset dưới đây thuộc code cũ;
chưa đo lại throughput/RSS sau bản sửa coverage theo ADR-0013). Lần chạy `2.000 đơn/ngày × 30 ngày`, seed 7, SKU=100, s=1,2:

| Chỉ số | Đo được |
|---|---:|
| Order / Order Line | 60.000 / 97.890 |
| Bản ghi nguồn / file | 1.061.702 / 447 |
| Byte nguồn | 1.546.066.142 (khoảng 1,55 GB thập phân) |
| `resource.getrusage(...).ru_maxrss` | 128.536 KiB = 125,52 MiB |
| Thời gian `generate_scaled` | 421,738301 giây |
| Right-censored / terminal | 3.905 / 93.985 |
| Sai lệch estimate số dòng / byte | 2,58% / 4,07% |

Lệnh: `.venv/bin/python -m generator.generate_order_lifecycle --output
out/codex-04-scale-final --orders-per-day 2000 --days 30 --seed 7`.
Thư mục đầu ra (1,5 GB) đã xoá sau khi đo để giải phóng đĩa; ở phiên bản baseline lệnh trên sinh
ra dataset `dataset-e16b9a828d69a3f6fb9cf507f2c9087d` (cùng seed ⇒ cùng digest). Hash code đã đối chiếu bản cuối;
447 file nguồn và truth đã được kiểm lại SHA-256. RSS của riêng tiến trình
Python, không gồm OS page cache. Máy có các tác vụ cùng chạy và lần đo sơ bộ
chồng một phần; thời gian này không phải benchmark CPU/IO cô lập.

Pytest: **41 passed in 31.68s** cho hai file nguồn, gồm kiểm fixture bit-identical,
5.000 line, hai phân phối SKU, gate/coverage và biên 180 ngày.


Spark: **chưa chạy trong sandbox**. Bộ unittest tại
[`tests/spark/check_order_lifecycle_silver_scale.py`](../../../tests/spark/check_order_lifecycle_silver_scale.py) sinh
200 đơn/ngày × 10 ngày, gọi `build_silver` bằng tham số tên với
`contracts_dir="/workspace/contracts"`; kiểm mọi reconcile check bằng 0,
quarantine bằng 0 và tập line Silver bằng truth.

Kết quả (người vận hành, 2026-10-19): **`Ran 3 tests in 980.842s — OK`** (`tests/spark/check_order_lifecycle_silver_scale.py`, 200 đơn/ngày ×
10 ngày, image `platform-airflow:2.10.5-spark3.5.3`, `--network none --read-only`, driver 3 GB): mọi check `reconcile` = 0,
quarantine = 0, tập line Silver = `_truth`. Thời gian dài vì máy chạy song song job khác; không dùng làm số hiệu năng.

```bash
docker run --rm --network none --read-only --tmpfs /tmp:rw,size=2g \
  --mount type=bind,src=$PWD,dst=/workspace,readonly --entrypoint python \
  platform-airflow:2.10.5-spark3.5.3 /workspace/tests/spark/check_order_lifecycle_silver_scale.py
```

## Giới hạn

- Không nạp PostgreSQL, chạy Data Platform hay ghi nhãn; benchmark nguồn không suy ra tốc độ Spark.
- Tồn kho và tiền USD vẫn đơn giản; chưa có price variation, many-seller hay nhiều pool khả dụng.
- Coverage chứng minh tập nguồn đã phát, không chứng minh mọi line đã terminal;
  line censored cần được loại/giữ immature bởi label policy.
- Không resume output dở dang. Khi lỗi, output có thể còn một phần; dùng thư mục mới.
- Hash code và Python được pin trong identity; đổi môi trường có thể đổi dataset ID.
