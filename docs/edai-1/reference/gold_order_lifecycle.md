# Gold nguồn v2 — gói E (fixture nhỏ)

> **Pipeline v2 (order lifecycle) — thiết kế và thử ở local, không phải pipeline đã nộp.** Pipeline chạy trên AWS là v1 (DP1 → DP4 + ML), xem [Data Platform evidence](../evidence.md). Các chỗ nhắc GCP dưới đây là kế hoạch cũ, chưa chạy; đích cloud hiện hành là [AWS](../../shared/aws_deploy.md).

Gold mới dùng namespace `build_gold` và **chỉ đọc Silver v2** (từ 2026-10-19; trước đó đọc thêm Bronze). Không dùng ground truth, probe hoặc nhãn v1. Policy duy nhất nằm ở
[`spark_jobs/lib/return_label.py`](../../../spark_jobs/lib/return_label.py), theo
[hợp đồng Return v2](return_label_contract.md) và ADR-0010.
Đây là fixture nhỏ, chưa phải bằng chứng chạy 100 GB.

## Thiết kế

| Bảng | Grain | Nội dung |
|---|---|---|
| `fact_order_lines` | dataset × Order Line | Identity đầy đủ của Channel/account/Order/line, seller_sku native, created_at, lượng đặt/hủy/giao, headline và product refund success tách riêng, currency; tiền minor bigint |
| `label_return` | dataset × Order Line × cutoff × generation | state, label nullable, cohort_date, maturity_end, qualifying_event_id, `positive_kind` (return_and_refund / refund_only / return_only khi label = 1 — xem [Return ≠ Refund](return_label_contract.md#return--refund--ba-ca-và-cách-nhãn-xử-lý-chốt-2026-10-19)), reasons, label_version, cutoff, generation_id, computed_at; bằng chứng gồm source_event_refs, decision_observed_at và input_json bất biến |
| `feat_sku_return_rate_90d` (tên bảng giữ nguyên; cột đổi thành `return_or_refund_rate_90d` 2026-10-19) | dataset × Order Line | event_timestamp = created_at, created = lúc tính, labeled_count_90d, positive_count_90d, return_or_refund_rate_90d, label_version, generation_id |
| `label_segments` | dataset × Order Line × đoạn cutoff | Mỗi đoạn `(start_exclusive, end_inclusive]` trên đó line ở trạng thái `labeled`: label, cohort_date, maturity_end, decision_observed_at. Feature PIT đọc bảng này |
| `label_inputs` | dataset × Order Line | Input quan sát được, chuẩn hóa; phục vụ đối chiếu Python/Spark |

Fact và feature partition `days(created_at)`. Nhãn append theo generation, không
cho chạy lại cùng `generation_id`; các bảng phục vụ hiện tại thay thế theo run.
Các snapshot nhãn cũ vẫn giữ nguyên cùng input_json, không bị correction mới sửa.
Receipt có trạng thái passed mới là bằng chứng hoàn tất; việc ghi nhiều bảng chưa
là một transaction chung.

## Nhãn và bằng chứng

Luật nhãn không được viết lại bằng biểu thức Spark. Spark gom observations theo
line rồi RDD gọi `label_line(line, cutoff)`; không cần pandas. Hàm Python trả
`LabelResult`, có policy ID `return-or-refund-line-v2`.

Cửa sổ theo allocation, biên bao gồm, UTC, cutoff exclusive; fulfillment terminal,
maturity và coverage đều bắt buộc cho cả label0/1. Correction chỉ có hiệu lực từ
lúc quan sát; cancellation/reversal nghiệp vụ giữ historical event. Xem chi tiết
và định nghĩa chính thức tại hợp đồng, không dùng ngày giao cuối để thay cửa sổ
riêng của từng allocation.

**Chỉ đọc Silver, gom bằng chứng theo khóa (refactor 2026-10-19).** Bản đầu đọc thêm
`bronze.lifecycle_raw_records`, gửi **toàn bộ** bằng chứng của dataset cho **từng** line và tính nhãn lại cho mọi cặp
(line lịch sử × thời điểm đặt đơn) — đúng trên fixture nhưng bậc hai, không chạy nổi nguồn lớn (OOM ngay cả ở
nguồn scale nhỏ). Bản hiện tại:

- Silver giữ lịch sử: `lifecycle_stg_observed_evidence` (mọi revision, chỉ bỏ retry trùng y, kèm khóa `ev_line_id`,
  `ev_order_id`, `corrects_source_event_id`) và `lifecycle_stg_coverage_validity` (digest/count/sequence/partition/khoảng
  của mỗi revision coverage, kiểm **một lần**).
- Gold gom bằng chứng theo khóa: theo line, theo Order (scope chưa rõ line), correction của bản thuộc line, và vài
  bản không khóa gửi một lần tới worker; coverage được lọc theo interval của line trước input_json. `spark_jobs.lib.return_label.visible_events` vẫn lọc chính xác.
- Feature PIT dùng `spark_jobs.lib.return_label.label_segments`: nhãn chỉ đổi ở thời điểm quan sát của chính line hoặc lúc qua
  maturity ⇒ mỗi line chấm O(số bản ghi) lần; đếm theo SKU bằng sweep trên cửa sổ nửa mở `[a, b)`.
- Bằng chứng tương đương: `tests/unit/spark_jobs/lib/test_return_label_segments.py` (so `label_line` ở mọi biên ±1µs) và
  `tests/spark/check_order_lifecycle_gold_equivalence.py` (fact/nhãn/feature **bằng hệt** bản cũ — bản chụp ở
  `tests/spark/reference_order_lifecycle_gold.py`). Kết quả 2026-10-19: diff = 0 ở fact/nhãn/feature trên fixture và nguồn scale 60 ngày, cả trước và sau đổi tên (196 s). Bản tham chiếu cũ hết bộ nhớ ở nguồn 90 ngày nên phép so dừng ở 60 ngày; feature khác 0 được kiểm riêng bằng `tests/spark/check_order_lifecycle_weekly_gold.py`.
- Job `spark_jobs/gold/build_order_lifecycle.py` chỉ chạy khi `silver.lifecycle_stg_build_info.as_of` = cutoff (job `spark_jobs/silver/build_order_lifecycle.py` ghi) và không có bằng chứng quan sát
  ≥ cutoff; sai thì dừng trước khi ghi.

Validator thay lineage từng cặp (quá lớn ở quy mô thật) bằng: `pit_segment_before_known` (đoạn không được mở trước
khi biết quyết định và qua maturity) và `pit_feature_mismatch` — tính lại feature cho mẫu 50 line bằng `label_line`
tại đúng thời điểm đặt đơn trên mọi line cùng SKU, không dùng code segment/sweep.

## Bản ghi keyless và bộ nhớ

Bản ghi không gắn line/Order (keyless, chủ yếu coverage) áp dụng cho mọi line.
Phép nhân bản chúng trong Spark từng gây OOM ngay ở nguồn small: 3.631 line ×
15 bản × khoảng 43 KB ≈ 2,3 GB; riêng `source_sequence_ranges` có thể tới
313 KB/bản. Vì vậy không thể coi chi phí bộ nhớ này là tuyến tính chỉ theo số line.

`keyless_evidence` thu tập này cùng correction liên quan về driver một lần;
`_slim` bỏ trường trong `VERIFICATION_ONLY_FIELDS` (`source_sequence_ranges`),
rồi gửi tập đã thu gọn cho worker qua closure, tránh join line × keyless trong Spark.
`MAX_KEYLESS_EVIDENCE = 10_000` chặn số bản keyless trước khi thu về driver;
vượt ngưỡng thì dừng lỗi vì có thể trích scope sai. Đây là ngưỡng số bản,
không phải giới hạn byte hoặc tổng số correction liên quan.

Sau bản sửa coverage theo ADR-0013, `source_line` lọc coverage theo cửa sổ delivery+30d trước `input_json`,
nên số tuần tail không làm input từng line lớn hơn. Driver và worker vẫn giữ tập
keyless; cần đo lại khi tăng số envelope/kích thước payload, kể cả dưới ngưỡng.
Bỏ `source_sequence_ranges` không đổi nhãn: Silver `coverage_validity` đã dùng
trường này để kiểm coverage một lần; `spark_jobs.lib.return_label.source_line` chỉ đọc nó ở nhánh
không có `coverage_status`, trong khi Gold luôn truyền map `coverage_status` từ
Silver. Test thuần Python: `tests/unit/spark_jobs/lib/test_return_label_keyless.py` so toàn bộ kết quả
`label_line` khi coverage có/không có trường này, cùng trạng thái Silver.

Số e2e sau sửa OOM (2026-10-20, local, 6g, coverage theo tuần): small 45 ngày × 50 đơn — 3.631 line, `gold_build` 13,7 s, `gold_validate` 13,9 s, mọi check = 0, 357 bản ghi `coverage_validity` (15 trước khi chia tuần). Ca Gold riêng 60 ngày × 4 đơn × 3 SKU: **131 line do generator sinh có `labeled_count_90d > 0`** (85 s), khớp số Python thuần. Medium (48.874 line) chưa chạy lại sau sửa — chạy trên GCP.

## Point-in-time

Với mỗi thời điểm đặt T, tính lại nhãn lịch sử bằng đúng `label_line(..., T)` trên
observations `source_observed_at < T`. Vì thế cả late event lẫn future correction
đều không lọt vào lịch sử tại T. Chỉ đếm cùng dataset/seller_sku, khác line đích,
state labeled, maturity_end < T, decision_observed_at < T, cohort_date thuộc
`[ngày UTC(T) − 90, ngày UTC(T))`. Count bằng 0 thì rate null.

`decision_observed_at` lấy max trên **toàn bộ observation liên quan nhìn thấy**,
bao gồm terminal/coverage và correction; đây là cách bảo thủ, có thể trì hoãn
availability thay vì đoán một tập chứng cứ tối thiểu. Job validate join độc lập
snapshot nhãn với feature, so lineage và tử/mẫu; đồng thời explode references rồi
join lại timestamp của từng observation trong input. Không gọi lại hàm feature
để tự chứng minh PIT.

## Vì sao feature có thể bằng 0 — scale đã sửa bằng coverage tuần

Lần chạy nguồn cũ đo nguồn scale cũ: coverage chỉ phát tại `D(observation_days−1)`,
sau mọi Order. Scale45 có 1.434 line, 1.373 labeled nhưng mọi segment chỉ
mở 12/10/2026, khi cohort 90 ngày đã hết hạn; không có target gốc có history.
Đây là giới hạn nguồn cũ, được **bản sửa coverage theo ADR-0013 thay thế** bằng envelope tuần nối tiếp
(ADR-0013). Fixture giữ nguyên byte.

Nguồn scale hiện phát từng `[week_start, week_end)`, không cộng dồn, đến hết
follow-up. Coverage biết sau cuối tuần ít nhất một ngày, và sau dòng trễ cuối
cùng trong tuần ít nhất 1µs. Nhãn cần chuỗi envelope complete có receipt hợp lệ,
không unresolved scope, phủ liên tục từ delivery đầu đến quá deadline cuối.
Hở tuần, partial hoặc receipt không đủ vẫn unknown; không lấy maturity thay coverage.

Đo Python thuần seed7, UTC, `ScaleConfig(days=60, orders_per_day=4, sku_count=3)`,
chỉ đọc nguồn, kiểm receipt một lần rồi `source_line` → `label_segments` → `feature_window`:

| Chỉ số | Kết quả |
|---|---:|
| Order / line do generator sinh | 240 / 367 |
| Segment labeled / window PIT không rỗng | 382 / 382 |
| Target **do generator sinh** có `labeled_count_90d > 0` | **131** |
| Coverage tối đa trong input một line | 30 |

Số segment có thể lớn hơn số line: label/decision thay đổi theo observation.
Các target thật đầu/cuối có history được đối chiếu với `sku_return_feature` as-of.
Vẫn có thể count0 với nguồn ngắn, SKU chưa có history, hoặc trước khi coverage
đủ; kết quả đó hợp lệ. Fixture vẫn có 30 line labeled nhưng 0 target gốc có
history; probe `sku-A` ngày 03/04 có count15/positive15/rate1. Minimal-scale một
line có segment mở 06/02/2026, probe +1µs count1/positive0, đúng biên count0.

Gold gửi keyless evidence một lần tới worker; `source_line` chỉ giữ coverage có
interval giao `[min(actual_delivered_at), max(actual_delivered_at)+30d]`, lấy
superset từ **mọi revision** delivery. Giữ cả nhóm revision/correction của envelope
liên quan để không làm sống lại claim cũ khi revision mới thu hẹp interval.
Envelope ngoài khoảng không góp phần lấp chuỗi coverage nên không đổi state/value;
loại nó cũng tránh observation không liên quan kéo lùi thời điểm biết nhãn.
Authority vẫn kiểm trên đầy đủ nguồn trước lọc, cùng feed/producer/epoch và scope.

`input_json` không tăng theo số tuần tail: khoảng giao hữu hạn theo line (scale
split delivery cách một ngày, nên tối đa 6 tuần × 5 feed = 30 envelope).
Fixture interval đơn vẫn được giữ. Revision/correction có thể tăng input theo lịch
sử thay đổi của chính line. `source_sequence_ranges` vẫn bỏ tại driver sau Silver
verification. Giữ `MAX_KEYLESS_EVIDENCE=10_000`: mặc định 39 tuần × 9 feed =351
complete envelope +6 unavailable, còn dư cho uncertainty; đây là giới hạn tổng
keyless thu về driver, không phải số envelope lặp trong input từng line.

Tái lập Python thuần:

```bash
.venv/bin/pytest -q -s tests/unit/spark_jobs/lib/test_return_feature_generated.py
```

người vận hành chạy regression scale và ca Spark Gold nhỏ riêng (240 Order, mục tiêu
≤10 phút; chưa đo Spark trong sandbox):

```bash
bash tests/harness/run_order_lifecycle_scale.sh --profile small --driver-memory 6g
docker run --rm --network none --read-only --tmpfs /tmp:rw,size=2g \
  --mount type=bind,src="$PWD",dst=/workspace,readonly \
  --entrypoint spark-submit platform-airflow:2.10.5-spark3.5.3 \
  --master 'local[1]' --driver-memory 6g \
  /workspace/tests/spark/check_order_lifecycle_weekly_gold.py \
  WeeklyGoldChecks.test_generated_orders_have_positive_pit_history
```

Kỳ vọng Spark Gold: **131 line** có count >0, đúng phép đo Python thuần, không
chèn later-order/probe. Test fixture + late-label cũ vẫn ở
`tests/spark/check_order_lifecycle_gold.py`.

## Ca hợp đồng và kiểm tra

Expected của L01–L17 chép từ hợp đồng, dữ liệu pytest dựng tay, không từ simulator.

| Ca | Expected | pytest |
|---|---|---|
| L01 | 1, cohort D20, maturity D50 | PASS |
| L02 | 0, cửa sổ allocation D0 hết hạn | PASS |
| L03 | 0, approved D31 | PASS |
| L04 | 1, product success đúng D30 | PASS |
| L05 | 0, pending/failed không qualify | PASS |
| L06 | 0, shipping-only | PASS |
| L07 | 0, giao1/hủy2 | PASS |
| L08 | immature | PASS |
| L09 | unknown, thiếu allocation | PASS |
| L10 | unknown/provisional → chọn unknown | PASS |
| L11 | 1, event biết D35, coverage D39, C D40 | PASS |
| L12 | 1 sau buyer cancel, không correction | PASS |
| L13 | trước correction 1; sau correction 0 | PASS; giả định coverage biết D31, invalidation biết D40 |
| L14 | ineligible/null | PASS |
| L15 | immature khi C = maturity_end | PASS |
| L16 | A/B unknown; resolve A1/B0 | PASS; giả định chung Order, resolution biết D65 |
| L17 | snapshot 0/1/immature; checksum export cũ giữ nguyên | PASS; coverage D31, late event D35, corrected delivery D45 biết D50 |

Có thêm biên ±1 microsecond, exclusive cutoff, missing/unavailable coverage,
conflict identity, reversal tiền, cohort 90 ngày, SKU khác, PIT correction và kiểm
coverage digest từ envelope dựng tay. Kết quả pytest: **31 passed**.

Harness Spark sinh fixture default trong `/tmp`, local[1], không catalog. Kiểm
mọi invariant job validate, parity tối đa 200 line, count/headline cố định; thêm
một Order quan sát sau follow-up để feature có lịch sử thực sự (15 positive), rồi
chèn nhãn snapshot June vào feature April để buộc PIT check bắt lỗi. Có cả ca fan-out,
unknown có label và labeled chưa mature.

Kết quả Spark (người vận hành, 2026-10-19): **`Ran 5 tests in 587.128s — OK`** (`tests/spark/check_order_lifecycle_gold.py`, image
`platform-airflow:2.10.5-spark3.5.3`, `--network none --read-only`, driver 3 GB). Gồm: số chính thức fixture + lịch sử rỗng;
PIT có lịch sử dương và bắt nhãn biết muộn được tiêm vào; validate bắt fan-out; validate bắt `unknown` có label và
`labeled` chưa mature. Driver 1 GB từng `OutOfMemoryError` ở bước Gold → warehouse.

## Lệnh chạy

```bash
.venv/bin/pytest -q tests/unit/spark_jobs/lib/test_return_label.py

docker run --rm --network none --read-only --tmpfs /tmp:rw,size=1g \
  --mount type=bind,src=$PWD,dst=/workspace,readonly --entrypoint python \
  platform-airflow:2.10.5-spark3.5.3 /workspace/tests/spark/check_order_lifecycle_gold.py

# Trước tiên publish Silver đúng observation snapshot bằng spark_jobs/silver/build_order_lifecycle.py --as-of.
scripts/run/run_order_lifecycle_spark.sh /opt/spark_jobs/gold/build_order_lifecycle.py \
  --cutoff 2026-06-01T00:00:00.000000Z --run-id gold-v2-example \
  --receipt /receipts/gold-v2-example.json
scripts/run/run_order_lifecycle_spark.sh /opt/spark_jobs/gold/validate_order_lifecycle.py \
  --cutoff 2026-06-01T00:00:00.000000Z --run-id gold-v2-example \
  --receipt /receipts/gold-v2-validate-example.json
```

## Giới hạn

- Môi trường Codex không có pyspark/pandas, Docker socket từ chối quyền; Spark và
  Iceberg catalog chưa chạy; số liệu Spark ở trên do người vận hành chạy local 2026-10-19/20.
- Coverage keyless vẫn lặp trong `input_json` từng line và tập keyless nằm trên
  driver/worker; xem giới hạn bộ nhớ ở trên. Chưa có bằng chứng e2e cho dữ liệu lớn.
- Một allocation có các phần giao ở nhiều thời điểm: unknown vì contract chưa
  định nghĩa d[a] duy nhất. Refund có nhiều allocation money rows cũng unknown
  thay vì đoán phân bổ; một row chỉ tin khi khớp toàn bộ product amount.
- `capability`/`profile_id` của coverage operational là bằng chứng authority trong
  fixture; chưa tích hợp registry authority riêng cho nguồn ngoài.
- Không có coverage hợp lệ trước thời điểm đặt fixture đầu kỳ nên feature default
  có count0/rate null; harness thêm Order sau follow-up để kiểm cả mẫu số khác0.
- Training export trong L17 là file pytest giữ checksum, chưa có pipeline training,
  MLflow, serving v2 hay migration comparison v1/v2 trong gói này.
- Facts/features/audit phục vụ generation hiện tại; nhãn cũ append-only. Chưa có
  atomic publication đa bảng hoặc retention/rollback toàn bộ generation.
