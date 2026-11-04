<!-- Mô tả stream vận hành v2, oracle độc lập và harness do người vận hành chạy. -->
# Stream nguồn v2 — Kafka → Flink

> **Pipeline v2 (order lifecycle) — thiết kế và thử ở local, không phải pipeline đã nộp.** Pipeline chạy trên AWS là v1 (DP1 → DP4 + ML), xem [Data Platform evidence](../evidence.md). Các chỗ nhắc GCP dưới đây là kế hoạch cũ, chưa chạy; đích cloud hiện hành là [AWS](../../shared/aws_deploy.md).

Gói D triển khai bốn feed nguồn Seller quan sát được. Bản sửa 10b đã kiểm local,
đang chờ người vận hành chạy lại Kafka/Flink harness. Run 10 trước đó có kiểm late
quá rộng nên không được dùng làm proof số late của bản này. Không dùng nguồn truth, không tạo label hoặc
headline sales. Giữ nguyên job v1 trong `flink_jobs/`.

| Topic | Kafka key | Event time |
|---|---|---|
| `v2.carrier.delivery_events` | `entity_id` (delivery operation) | `payload.actual_delivered_at` |
| `order_lifecycle.seller_postsales.return_events` | `entity_id` (Return) | `effective_at` của requested/approved/received; chỉ approved vào window |
| `order_lifecycle.seller_postsales.refund_events` | `entity_id` (transaction + component) | `effective_at` của failed/success; chỉ success product vào window |
| `v2.seller_wms.stock_operations` | `entity_id` (stock operation) | `effective_at` |

Known-time là `source_observed_at`; không thay event time bằng known-time.
Producer sắp theo datetime UTC, tie-break feed/sequence/event-ID. Retry được phát
ngay sau bản gốc, cùng key, envelope và bytes. Chọn chính xác
`floor(N nguồn × duplicate_rate)` bản để retry một lần bằng RNG riêng với seed;
mặc định 3% **so với số nguồn**, không phải so với tổng sau retry.

Export fixture/scale hiện hành chưa có `record_sha256`. Trong trường hợp thiếu,
producer bổ sung đúng một trường: SHA256 của dòng nguồn JSONL không có LF, tương
thích cách hash raw JSON của Bronze. Nếu đã có trường thì giữ nguyên. Mọi trường
khác giữ nguyên; không sửa nguồn trên đĩa. Oracle tự tính fallback hash từ dòng
nguồn và kiểm SHA256 file theo source manifest, không import producer.

Header Kafka `epoch` là số nguyên mã hóa UTF-8; không phải `source_epoch` trong
envelope (identity của nguồn). Replay phải chọn epoch mới và manifest/log mới.
Dùng một producer tại một thời điểm trong proof; không có cơ chế khóa epoch giữa
nhiều producer độc lập. Epoch được cô lập **trước watermark bằng offset Kafka**;
header không dùng để lọc. SQL xuất epoch đọc từ header, comparator yêu cầu mọi
bản ghi có epoch đúng; header khác hoặc thiếu làm FAIL thay vì âm thầm mất tin.

`stream_manifest.json` (đường dẫn qua `--manifest`) ghi dataset, hash source
manifest, seed/rate, số nguồn/phát/retry theo topic, inclusive first/last offset
và số record từng partition. Ledger ghi từng SHA, partition, offset, cờ retry
theo thứ tự gửi; callback Kafka xác nhận offset thực. Digest tổng là SHA256 của
canonical envelope + LF theo thứ tự phát, gồm retry. File sink mô phỏng partition
0 và offset riêng mỗi topic; không coi đây là bằng chứng offset Kafka.
Không ghi manifest thành công khi flush timeout hoặc delivery callback lỗi.

| Job | Chế độ và cơ chế | Đích dưới `/opt/flink/output/order_lifecycle/` |
|---|---|---|
| `flink_jobs/order_lifecycle/baseline.sql` | Bounded batch, COUNT/MIN/MAX theo topic × epoch × partition | `baseline/<epoch>/counts` |
| `flink_jobs/order_lifecycle/late_arrival.sql` | Streaming bounded, watermark event-time; một CASE tách main/late | `late/<epoch>/routes/route=main`, `route=late` |
| `flink_jobs/order_lifecycle/dedup.sql` | Streaming ROW_NUMBER theo SHA, PROCTIME ASC giữ đầu; audit input và kept; batch đếm cuối sau DML sync | `dedup/<epoch>/{input,kept,counts}` |
| `flink_jobs/order_lifecycle/windows.sql` | Bounded batch ROW_NUMBER loại retry, TUMBLE một ngày UTC theo event-time | `windows/<epoch>/daily` |

Mọi source dùng `scan.startup.mode = specific-offsets` và
`scan.bounded.mode = specific-offsets`, lấy từ manifest đúng epoch. Receipt ghi
first/last inclusive; renderer truyền start=first và stop=last+1 (exclusive).
Renderer kiểm số nguyên không âm, partition hợp lệ, thứ tự ledger và offset
liên tục, không cho một epoch xen kẽ epoch khác trong partition. Topic rỗng
không có boundary bị từ chối; proof yêu cầu bốn topic có dữ liệu. Producer phải
chạy lần lượt, không có writer khác xen vào dải offset. Không phát thêm khi
đang chạy proof. Job dedup đọc lại đúng boundary trong truy vấn batch thứ hai.

Specific-offsets của connector chỉ mô tả partition/offset, nên renderer tạo
bảng Kafka riêng từng topic rồi UNION ALL thành source chung. Nhờ đó partition
0 của topic A không dùng nhầm boundary của partition 0 topic B. Cả streaming
source và batch count source của dedup đều dùng cùng receipt. Renderer bắt buộc
`--manifest`; epoch khác manifest hoặc boundary sai bị từ chối trước khi ghi SQL.

Late dùng `event_ts <= CURRENT_WATERMARK(event_ts)`; watermark NULL vào main.
Filesystem partition sink có thể bỏ cột route trong JSON; comparator lấy lại từ
đường dẫn. Đây là hai nhánh bằng SQL/file partition, không phải DataStream
OutputTag. Không có filter bỏ late và không gán kết quả chưa biết thành 0.

Oracle mô phỏng watermark trước mỗi record, riêng topic/partition:
`max(event_ts trước record) - delay`, timestamp cắt precision millisecond như SQL.
Số này chỉ là mô hình cho bản ba partition: minimum upstream và chu kỳ phát
watermark có thể làm lệch số late. Bản ba partition **chỉ yêu cầu bảo toàn input
bằng exact multiset offset/hash**; số late thực đo vẫn được báo riêng.

Proof số late chạy riêng trên `v2.late.<feed>`, **mỗi topic đúng một partition**.
Producer `--late-topics` phát thêm bản sao với cùng envelope, thứ tự và retry,
ghi manifest `stream-<epoch>-late.json` cạnh manifest chính; file sink có log
`<file-out>.late`. Kafka producer kiểm metadata topic có duy nhất partition 0.
Oracle đọc receipt riêng này và nguồn quan sát, không đọc output Flink. Renderer
`--late-topics` sinh bốn SQL job từ `flink_jobs/order_lifecycle/late_arrival.sql`, mỗi job chỉ đọc một topic, parallelism 1,
output `late-proof/<epoch>/routes`. Tách job tránh watermark của feed này bị
minimum watermark của feed khác chặn lại.

Proof đặt `scan.watermark.emit.strategy = on-event` (watermark pushdown của
Kafka connector trên Flink 1.20), giữ auto-watermark interval 1ms. Watermark
được phát sau từng event nên record tiếp theo được so với max trước nó đúng
oracle; record đầu với watermark NULL vào main. Không dùng khoảng dung sai
chu kỳ. Comparator bắt buộc `late == oracle.late` từng topic, bảo toàn toàn bộ
input proof, cùng multiset envelope/retry với bản ba partition, và oracle có
late > 0 ở ít nhất hai topic. `late_within_periodic_range` đã bị xóa. Nếu runtime
không áp dụng được on-event hoặc kết quả lệch một record, proof FAIL; không nới
sang khoảng [0,input]. người vận hành cần xác minh khả năng pushdown và output thực
bằng harness, không suy từ unit test thành runtime pass.

Với nguồn scale 200 đơn/ngày × 10 ngày, seed 7, delay 3600 giây không đổi,
chạy producer file sink trên nguồn quan sát người vận hành đã sinh cho oracle proof:
delivery 3412, return 706, refund 566, stock 0 (gồm retry). Đây là kỳ vọng một
partition, không phải số đo Flink. Fixture unit nhỏ chỉ có một topic late ở
delay 3600; test comparator cần hai topic dùng delay 0, có test riêng delay
3600 để kiểm lifecycle/window. Harness vẫn dùng delay 3600.

Window batch bounded giữ toàn bộ late đến EOF, tránh đánh rơi correction có
event-time thuộc ngày đã qua. Hai cột có nghĩa **đếm observed event revisions**:
`approved_return_events` (approved, quantity > 0) và
`successful_product_refund_events` (success/product/amount_minor > 0). Cùng entity
có correction khác SHA vẫn đếm một event revision mới; không collapse latest,
không tính số Return/Refund entity cuối cùng hay official label. Shipping,
failed, requested, received không tăng hai phép đo này. So sánh windows không
phụ thuộc nhịp watermark realtime.

Dedup summary gồm `input`, `dup_removed`, `kept`, `revisions_kept`; revisions_kept
là số SHA duy nhất trừ số entity duy nhất, theo topic/epoch. Comparator còn tính
lại các số đó từ output audit/kept, kiểm giữ đúng offset đầu của SHA, nên số đếm
batch không che được lỗi dedup/restart. Không bật TTL có thể làm quên retry.

## Chạy local không Kafka

Dùng thư mục đích mới ở mỗi lần chạy:

```bash
.venv/bin/python -m generator.generate_order_lifecycle_fixture --output /tmp/v2-source-new
.venv/bin/python -m generator.publish_order_lifecycle_events --source /tmp/v2-source-new \
  --file-out /tmp/v2-stream-1.jsonl --manifest /tmp/stream_manifest.json --epoch 1
.venv/bin/python -m generator.build_order_lifecycle_stream_oracle --source /tmp/v2-source-new \
  --manifest /tmp/stream_manifest.json --output /tmp/v2-oracle-1.json
.venv/bin/pytest -q tests/unit/generator/test_publish_order_lifecycle_events.py
```

Producer/oracle giữ nguồn và ledger trong RAM; đây là harness bounded nhỏ,
chưa được benchmark bộ scale cực đại 20 triệu record.

## Harness Kafka/Flink — người vận hành chạy

Chuẩn bị image `apache/kafka:3.9.1` và `platform-flink:1.20.3-kafka3.4.0` (build từ
`docker/Dockerfile.flink`) trước. Harness dùng `--pull never`, không đổi Compose,
không đụng service sẵn có, tạo network/container riêng theo PID. Broker KRaft
có ba partition/topic; cổng host localhost ngẫu nhiên. Source scale
200 đơn/ngày × 10 ngày, seed 7; thêm bốn topic `v2.late.*` một partition.
Phát hai epoch vào cùng các topic trước khi chạy,
so từng epoch độc lập. Trap dọn container/mạng kể cả khi lỗi; giữ output/log,
checkpoint, oracle, compare JSON và restart evidence trong run dir.

```bash
bash tests/harness/run_order_lifecycle_stream.sh "$PWD/out/order-lifecycle-stream-proof-new"
```

Checkpoint mặc định template 1000ms, harness 100ms. Cả JM/TM dùng chung bind
checkpoint/output. Ca dedup epoch 2 giảm fetch Kafka, REST đợi checkpoint có
`state_size > 0` khi job còn RUNNING, dừng rồi khởi động **TaskManager tạm**.
JobManager giữ nguyên và fixed-delay restart từ checkpoint đã hoàn tất. REST
`latest.restored` bắt buộc tồn tại, ghi `restart.json`; nếu job quá nhanh trước
checkpoint hoặc không phục hồi thì harness thất bại, không công nhận proof.
Output sau restart vẫn phải qua toàn bộ comparator. Đây là TaskManager failure
recovery, chưa chứng minh JobManager HA hoặc thảm họa mất disk.

Để chạy từng job trên môi trường tự chuẩn bị:

```bash
.venv/bin/python scripts/run/render_order_lifecycle_stream.py --output /tmp/v2-sql-new \
  --epoch 1 --manifest /tmp/stream_manifest.json \
  --bootstrap kafka:9092 --delay-seconds 3600 --checkpoint-ms 1000
# Proof: producer phát --late-topics; tính oracle từ manifest *-late.json rồi:
.venv/bin/python scripts/run/render_order_lifecycle_stream.py --output /tmp/v2-proof-sql-new \
  --epoch 1 --manifest /tmp/stream_manifest-late.json --late-topics
# Mount SQL, output và checkpoint vào các đường tương ứng rồi chạy sql-client.sh -f.
.venv/bin/python scripts/evidence/compare_order_lifecycle_stream.py --flink-output out/flink/v2 \
  --oracle /tmp/v2-oracle-1.json --late-oracle /tmp/v2-oracle-1-late.json \
  --output /tmp/v2-compare-1.json
```

GCP: người vận hành bổ sung checklist triển khai sau; lượt này không gọi Cloud hoặc
thay checklist triển khai riêng.

## Số đo

người vận hành chạy `bash tests/harness/run_order_lifecycle_stream.sh` ngày 2026-10-19 (Kafka 3.9.1 + Flink 1.20.3 trong container tạm,
nguồn `generator.generate_order_lifecycle` 200 đơn/ngày × 10 ngày, seed 7): **exit 0, cả 2 epoch `passed`, 190 s**. Receipt:
[`evidence/stream-v2-local-20261019.json`](../../evidence/stream-v2-local-20261019.json).

| Đo | Kết quả runtime (epoch 1; epoch 2 giống hệt) | Đối chiếu |
|---|---|---|
| Baseline topic/epoch/partition/offset | khớp ledger producer | ledger producer |
| Main + late, 3 partition | main + late = input mọi topic | exact multiset input |
| Late, 1 partition/topic | delivery **3.412**, return **706**, refund **566**, stock **0** | oracle tuần tự: 3.412 / 706 / 566 / 0 — **bằng hệt** |
| Dedup + revisions | delivery 3.610 → 3.494 (116 retry), return 2.113 → 2.052 (61 retry, 1.368 revision giữ), refund 1.121 → 1.092 (29 retry, 328 revision giữ), stock 8.874 → 8.623 (251 retry) | SHA, first offset, summary và derived counts |
| Daily windows | `daily_event_windows` pass | oracle nguồn, retry chỉ đếm một lần |
| TaskManager restart | checkpoint có state sau 11,0 s → stop/start TM → restore checkpoint **28** sau 3,1 s → kept commit **15.261 = oracle** → REST cancel `CANCELED` | checkpoint state/restore REST + output exact |

người vận hành sửa harness/SQL trong lúc chạy (đều có comment tại chỗ): cổng Kafka chọn trước khi start (KRaft cấm đổi
advertised listener), `kafka_partition`, watermark `INTERVAL 'H:MM:SS' HOUR TO SECOND`, batch mode đặt trước `CREATE TABLE`,
quote `` `day` ``, `sql-client` chạy user `flink`. Ca restart bản bounded từng fail 2/2 vì job xong trước checkpoint đầu ⇒ đổi
sang unbounded + cancel qua REST (không còn phụ thuộc tốc độ máy).
