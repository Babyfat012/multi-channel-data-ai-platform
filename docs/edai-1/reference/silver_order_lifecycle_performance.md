# Vì sao Silver v2 chậm siêu tuyến tính

> **Pipeline v2 (order lifecycle) — thiết kế và thử ở local, không phải pipeline đã nộp.** Pipeline chạy trên AWS là v1 (DP1 → DP4 + ML), xem [Data Platform evidence](../evidence.md). Các chỗ nhắc GCP dưới đây là kế hoạch cũ, chưa chạy; đích cloud hiện hành là [AWS](../../shared/aws_deploy.md).

Phân tích hiệu năng Silver. Chỉ tài liệu và diff đề xuất, chưa áp dụng thay đổi Spark.

## Kết luận và phạm vi bằng chứng

**Ưu tiên sửa phép xử lý tập sequence trong `coverage_validity`, sau đó giảm tính lại lineage.**
Medium dành **1.815,877 s** cho ResultStage 100/job 53, trong chặng Silver **2.090,103449 s**:
**86,88%** thời gian Silver. Small tương ứng ResultStage 103/job 52 chỉ **8,706 s**.
Nguồn tăng **13,24 lần**, Silver tăng **35,67 lần**, riêng stage này tăng **208,58 lần**.
Các tỷ lệ là phép chia số trong nguồn dưới đây, không phải benchmark mới.

- `M` = `out/e2e-v2-medium.G5BI4k/spark.log` (log local, không commit).
- `S` = `out/e2e-v2-small.bpqNq2/spark.log` (log local, không commit).
- `MR` = receipt medium `out/e2e-v2-medium.G5BI4k/receipt.json` (local, không commit), dòng 20–35, 55–87.
- `SR` = receipt small `out/e2e-v2-small.bpqNq2/receipt.json` (local, không commit), dòng 20–35, 88–103.
- Dạng `M:9173` là số dòng vật lý trong đúng file trên (có thể kiểm bằng `nl -ba`).

| Chỉ số | Small | Medium | Nguồn |
|---|---:|---:|---|
| Bronze rows | 40.398 | 534.853 | SR:21–23; MR:21–23; commit M:4406 |
| line_lifecycle / native_order_lines rows mỗi bảng | 3.631 | 48.874 | SR:24–29; MR:24–29 |
| observed_evidence rows | 27.646 | 374.574 | SR:30–32; MR:30–32 |
| coverage_validity rows | 15 | 15 | commit S:7217; M:9180; SR/MR:33–35 |
| Silver (giây tường, gồm build, ghi và count bảng đã ghi) | 58,602433 | 2.090,103449 | SR:95–103; MR:62–70 |
| Final stage coverage_validity (giây) | 8,706 | 1.815,877 | S:7210; M:9173 |
| Job ghi cuối coverage_validity (giây) | 8,706962 | 1.815,890594 | S:7213; M:9176 |

Receipt medium còn `status=running`, `gold_validate=failed/Py4JJavaError` (MR:3,83–87).
Chỉ xác nhận Silver đã hoàn tất; không coi medium là E2E PASS. Không lấy lỗi Gold sau Silver
làm nguyên nhân của stage đã kết thúc. Hai profile là hai dataset khác nhau, số ngày khác nhau;
hai điểm đo không chứng minh toàn pipeline có độ phức tạp bậc hai và không ngoại suy thời gian 100 GB.

## Cách khoanh chặng và đọc thời gian

Runner `scripts/run/run_order_lifecycle_local.py:160–167` ghi Bronze rồi gọi `build_silver`, ghi lần lượt
`line_lifecycle`, `native_order_lines`, `observed_evidence`, `coverage_validity`; mỗi lần ghi
được count **từ bảng Iceberg đã materialize** (`:94–105`), cuối Silver mới `clearCache()`.

Khoanh M từ dòng 4479 (scan operational cho `_dedup`) đến 9244 (job count cuối coverage);
S từ 3511 đến 7278. Thời gian tạo logical plan trước scan đầu vẫn nằm trong receipt.
Xếp hạng **stage thực thi**, không cộng job chứa stage với stage đó. Các map-stage job AQE
không có dòng `Job N finished … took` tương ứng trong log này: chỉ nhìn các dòng job finished
sẽ bỏ mất nhiều scan tốn thời gian. `Final stage`, `Submitting … missing tasks`, dòng stage
finished là ba tín hiệu dùng để lấy ID, số task và thời gian. Stage chạy đồng thời nên tổng
các thời gian stage không bằng thời gian tường Silver.

Callsite Spark ghi thực tế chỉ có `NativeMethodAccessorImpl.java:0` cho count/create và
`FutureTask.java:264` cho broadcast; **không có file Python:dòng** cho phần lớn job. Bảng dưới
phân biệt callsite nguyên gốc và vị trí Python suy từ thứ tự action/cấu trúc code:

- `D`: `_dedup`, `spark_jobs/lib/order_lifecycle_lakehouse.py:426–441`; count `:437–438`, collect `:439`.
- `L`: `build_silver:522–523`, `line_lifecycle:646–668`, write `scripts/run/run_order_lifecycle_local.py:100`.
- `N`: `native_order_lines:147–243`, write runner `:100`.
- `E`: `observed_evidence:317–353`, write runner `:100`.
- `V`: `coverage_validity:356–412`, write runner `:100`. V bao gồm các nhánh phụ huynh
  chạy trước thông báo bắt đầu ghi ở M:9094/S:7132; không chỉ stage ghi cuối.

## 20 stage Silver medium tốn thời gian nhất, đối chiếu small

`C` = `create at NativeMethodAccessorImpl.java:0`; `K` = `count at NativeMethodAccessorImpl.java:0`;
`P` = `collect at /workspace/spark_jobs/lib/order_lifecycle_lakehouse.py:439`.
`Sh` = ShuffleMapStage, `R` = ResultStage. Cột dòng ghi `submit/finished`: dòng submit có số task.
So cùng vai trò/thứ tự nhánh, **không** ghép theo ID bằng nhau. AQE thay đổi DAG giữa hai run;
những nhánh không ghép chắc chắn được ghi rõ. Nhãn root V theo thứ tự submit chỉ là đối chiếu
vị trí, chưa có physical plan để khẳng định từng root là `cov`/`rows`/`declared`.

| # | Medium stage / loại | Vai trò / callsite | s medium | Tasks | M submit/finished | Small stage: s / tasks / S submit/finished | Shuffle |
|---|---|---|---:|---:|---|---|---|
| 1 | 100 / R | V: hoàn tất aggregate/so sequence + ghi / C | 1815.877 | 4 | 9106:9173 | 103: 8.706 / 5 / 7144:7210 | Xem fetch cuối bên dưới |
| 2 | 84 / Sh | V: đọc/gate/dedup nhánh coverage / C | 69.159 | 14 | 8457:8997 | 87: 5.112 / 6 / 6592:6813 | Không có metric gắn stage |
| 3 | 83 / Sh | V: đọc/gate/dedup nhánh coverage / C | 61.250 | 14 | 8444:8832 | 86: 5.120 / 6 / 6557:6781 | Không có metric gắn stage |
| 4 | 82 / Sh | V: đọc/gate/dedup nhánh coverage / C | 38.178 | 14 | 8405:8627 | 85: 4.992 / 6 / 6531:6720 | Không có metric gắn stage |
| 5 | 23 / Sh | L: materialize cache events / C | 29.102 | 13 | 5358:5454 | 23: 5.065 / 6 / 4177:4233 | Không có metric gắn stage |
| 6 | 17 / Sh | D: collect conflict IDs / P | 25.699 | 13 | 5068:5154 | 17: 4.709 / 6 / 3967:4026 | Không có metric gắn stage |
| 7 | 11 / Sh | D: unique.count / K | 25.087 | 13 | 4819:4922 | 11: 4.497 / 6 / 3786:3847 | Không có metric gắn stage |
| 8 | 5 / Sh | D: ops.count lần đầu / K | 24.986 | 13 | 4510:4602 | 5: 5.478 / 6 / 3542:3598 | Không có metric gắn stage |
| 9 | 8 / Sh | D: ops.count lần hai / K | 24.491 | 13 | 4669:4752 | 8: 4.327 / 6 / 3667:3719 | Không có metric gắn stage |
| 10 | 76 / Sh | E: đọc/gate/dedup evidence / C | 23.235 | 14 | 7981:8108 | 79: 3.521 / 6 / 6180:6237 | Không có metric gắn stage |
| 11 | 87 / Sh | V: aggregate/shuffle trung gian / C | 20.134 | 8 | 8707:8920 | V trung gian: 89/91/93, xem dưới; chưa ghép duy nhất | Không có metric gắn stage |
| 12 | 86 / Sh | V: aggregate/shuffle trung gian / C | 19.328 | 8 | 8655:8798 | V trung gian: 89/91/93, xem dưới; chưa ghép duy nhất | Không có metric gắn stage |
| 13 | 72 / R | N: ghi native (union parser) / C | 8.672 | 84 | 7058:7874 | 75: 1.261 / 36 / 5664:6071 | Không có metric gắn stage |
| 14 | 78 / R | E: ghi observed_evidence / C | 8.030 | 8 | 8130:8214 | 81: 0.692 / 8 / 6259:6342 | Không có metric gắn stage |
| 15 | 94 / Sh | V: aggregate/shuffle trung gian / C | 5.327 | 8 | 9015:9079 | 98: 0.536 / 8 / 7044:7119 | Không có metric gắn stage |
| 16 | 89 / Sh | V: aggregate/shuffle trung gian / C | 5.172 | 8 | 8864:8951 | V trung gian: 89/91/93, xem dưới; chưa ghép duy nhất | Không có metric gắn stage |
| 17 | 28 / R | L: nhánh lifecycle / C | 4.278 | 8 | 5552:5928 | 28: 0.530 / 8 / 4330:4601 | Không có metric gắn stage |
| 18 | 29 / R | L: nhánh lifecycle / C | 4.255 | 8 | 5587:5925 | 29: 0.508 / 8 / 4365:4578 | Không có metric gắn stage |
| 19 | 30 / R | L: nhánh lifecycle / C | 4.218 | 8 | 5632:5920 | 30: 0.519 / 8 / 4409:4688 | Không có metric gắn stage |
| 20 | 31 / R | L: nhánh lifecycle / C | 4.190 | 8 | 5653:5931 | 31: 0.500 / 8 / 4430:4708 | Không có metric gắn stage |

Small V trung gian: stage 89 = 0,153 s / 8 task (S:6739–6804),
91 = 0,336 s / 8 task (S:6866–6977), 93 = 0,224 s / 8 task (S:6921–7001).
M có shuffle stage 86/87/89 ở vị trí tương ứng; S có nhánh broadcast ResultStage 93.
Không suy ra tỷ số từng operator từ các cặp AQE này.

Shuffle log có fetch nhưng không có event-log/task-metrics tổng read/write từng stage.
Các dòng fetch trong stage cuối V đều local, remote = 0:

| Run / stage | Fetch quan sát được (mỗi dòng là một lần fetch, không phải tổng task) | Dòng |
|---|---|---|
| M / 100 | 67,1 MiB; 13,9 MiB; 110,5 MiB; 15,9 MiB | M:9122–9123,9131–9132 |
| M / 100 | 312,6 KiB; 1.385,1 KiB; 503,4 KiB; 2,5 MiB, ngoài các block nhỏ | M:9139,9142,9150,9153 |
| S / 103 | 5,4 MiB; 3,7 MiB; 2,9 MiB; 1.949,1 KiB; 1.157,6 KiB | S:7168–7174 |

Không gán các dòng fetch xen kẽ của stage chạy song song cho một stage cụ thể;
không cộng các số đã làm tròn thành “shuffle chính xác”.

Kiểm chéo với job finished: M job 53/V = 1.815,890594 s (M:9176),
38/N = 8,679103 s (M:7877), 42/E = 8,033009 s (M:8217);
S job 52/V = 8,706962 s (S:7213), 37/N = 1,266062 s (S:6074),
41/E = 0,693781 s (S:6345). Job ghi `L` cuối chỉ 0,285210 s (M:6798)
vì phần scan/dedup/join đã chạy trong các map/broadcast stage trước đó.
Job count cuối V = 0,409205 s (M:9244), chứng minh count bảng sau write
không lặp lại toàn lineage V.

## Nguyên nhân, giả thuyết và phép bác bỏ

### H1 — So tập sequence bằng `array_distinct(array<struct>)`: ưu tiên cao nhất

`coverage_validity:391,398–406` gom hai mảng struct `(partition,seq)`, sắp xếp,
rồi `array_distinct` từng mảng trước khi so bằng. Mảng tăng theo số sự kiện của
coverage, không theo số dòng đầu ra (chỉ 15). Đây là ứng viên cho chi phí bậc hai
nếu runtime xử lý distinct struct bằng tìm tuyến tính trong phần tử đã gặp.
**Chưa kiểm implementation Scala của runtime này; đây là giả thuyết, không phải
kết luận profiler.** Không tải source mạng theo luật brief.

Bằng chứng phân biệt với scan/I/O: bốn task M stage 100 bắt đầu cùng lúc M:9108–9115;
kết thúc lần lượt **31.996 / 79.025 / 170.964 / 1.815.838 ms**
(M:9159,9163,9167,9171). Small năm task kết thúc **431 / 636 / 1.323 / 3.798 / 8.675 ms**
(S:7192,7196,7200,7204,7208). M fetch bắt đầu 13:34:27–29, nhưng task chậm nhất
kết thúc 14:04:43. Chỉ một task giữ phần lớn thời gian tường: phù hợp xử lý mảng
lớn trong một group, không phù hợp kết luận mọi task phải chờ remote shuffle.

Dự đoán có thể bác bỏ: chỉ áp diff A, giữ nguyên digest/nguồn/tài nguyên, đo lại V;
nếu stage cuối không giảm rõ rệt, H1 không giải thích được nút thắt. Phải dùng
write/collect chứa `receipt_status`; chỉ `count()` có thể bị optimizer loại biểu
thức cần đo. Lấy thread dump/profiler task chậm để phân biệt `ArrayDistinct` với
sort/hash JSON nếu A không đủ. Không tuyên bố đã chứng minh H1 bằng pytest Python.

### H2 — Gom toàn payload JSON và sort/hash theo coverage: chắc chắn có, phần thời gian chưa tách

`:385–394` join theo dataset/feed/producer/epoch và thời điểm quan sát, rồi
`collect_list(struct(sid,raw_json))`, sort, dựng nguyên chuỗi JSON và SHA-256.
Coverage revisions làm số cặp matched tăng theo số envelope có cùng scope.
Không join theo từng Order Line; nhưng một coverage lớn vẫn gom nhiều raw JSON
vào một task. Fetch M:9122–9132 và chênh task ở trên phù hợp với group lệch kích thước.
Chưa có metric số matched/group hay heap/GC nên chưa chứng minh fan-out hoặc OOM.

Dự đoán: sau A, profile phần sort/concat/SHA và đo số matched theo evidence_event_id.
Nếu CPU/memory còn chủ yếu ở digest, cần thiết kế hash theo iterator/stream theo đúng
thứ tự canonical, không dùng `collect_list` chuỗi khổng lồ. Không thay bằng XOR/hash
của hash con: sẽ đổi `record_set_sha256` và hợp đồng nguồn. Đề xuất dài hạn này chưa
có diff an toàn vì cần chứng minh tương đương serialization và giới hạn kích thước.
Tăng shuffle partitions không chia được một group coverage thành nhiều task.

### H3 — Tính lại gate/dedup do chỉ cache `events`: chắc chắn có

`_dedup:437–439` thực hiện hai `ops.count()`, một `unique.count()` và collect conflict;
`events.cache()` chỉ được đăng ký **sau** tất cả action này (`build_silver:522–523`).
Gate `:464–492` có nhiều biểu thức `get_json_object` để kiểm required; không có UDF Python.

M stage 5/8/11/17 đều đọc operational Bronze với 13 task, khoảng 25 s mỗi stage
(M:4479,4640,4788,5037 và bảng). Stage 23 lại đọc cùng nhánh để materialize events
(M:5358–5454). Cache events có ích cho lifecycle, nhưng không giúp các action đã chạy
trước cache, native parser hoặc evidence vốn đi từ `known` riêng.
Không thể mô tả hiện trạng là “hoàn toàn không cache”.

Evidence được ghi vật lý xong ở M:8222–8224 nhưng `built['coverage_validity']` đã được
xây từ DataFrame evidence gốc (`build_silver:639–641`). Gán `result[name] = spark.table`
trong `write_tables` không thay lineage của V đã tạo. Vì vậy ba root stage V 82/83/84
đọc lại Bronze với 14 task sau khi E đã ghi, từ 13:33:12 đến 13:34:22
(M:8400–8457,8627,8832,8997). Chúng chạy chồng nhau, không cộng thành thời gian tiết kiệm.
Diff C cắt lineage này ở bảng E vật lý trong runner; không giải quyết H1/H2.

### H4 — Lifecycle fan-out, global window hoặc Python UDF: không phải ứng viên chính ở run này

`line_lifecycle:648–666` aggregate từng fact về line trước left join;
`_dedup`, `_latest`, coverage/uncertainty windows đều có `partitionBy`.
Native explode là một dòng mỗi item và tổng native/lifecycle cùng bằng số line
trong receipt. `coverage_receipts(known)` có `sequence/explode` và anti joins nhưng
runner không materialize bảng này; không gán stage V cho hàm đó chỉ vì tên tương tự.
`reconcile()` không được runner Silver gọi. `Registry`, `required_json_paths`
được đọc trong lúc dựng biểu thức; không phải UDF chạy từng dòng.
Không có bằng chứng log cho join bình phương theo Order Line trong Silver.
Bằng nhau về count chưa chứng minh từng identity/tiền đúng: vẫn phải đối soát tập dòng.

## Diff đề xuất, thứ tự áp dụng và hiệu quả có điều kiện

Các diff dưới chỉ là đề xuất để người vận hành/implementer kế tiếp áp dụng. Không sửa
`spark_jobs/*.py` hoặc runner trong task này. Mọi giây “tiết kiệm” dưới đây là ngân
sách thời gian có thể tác động, không phải kết quả đã đo; không cộng cơ học giữa các diff.

### A — Distinct trong aggregate, so mảng đã chuẩn hóa

Vị trí `spark_jobs/lib/order_lifecycle_lakehouse.py:391,398–406`:

```diff
@@ def coverage_validity(evidence: DataFrame) -> DataFrame:
-        F.array_sort(F.collect_list(F.struct(F.col("partition"), F.col("seq")))).alias("actual_seq"),
+        F.array_sort(F.collect_set(F.struct(F.col("partition"), F.col("seq")))).alias("actual_seq"),
@@
-        F.array_sort(F.collect_list(F.struct("partition", "seq"))).alias("declared_seq"))
+        F.array_sort(F.collect_set(F.struct("partition", "seq"))).alias("declared_seq"))
@@
-             & (F.array_distinct(F.coalesce("declared_seq", empty_seq))
-                == F.array_distinct(F.coalesce("actual_seq", empty_seq)))
+             & (F.coalesce("declared_seq", empty_seq)
+                == F.coalesce("actual_seq", empty_seq))
```

Giữ nguyên received_rows, digest, time filter, null coalesce và mọi điều kiện khác.
Hai bên vẫn là tập struct sắp thứ tự; distinct sau sort và sort sau collect_set
phải cho cùng kết quả. Struct có field null vẫn là struct khác null nên cần test
riêng, không mặc định bỏ được các dòng thiếu partition/seq.

Ngân sách tác động lớn nhất: stage V cuối **1.815,877 s** (M:9173). Nếu H1 chiếm chủ yếu,
tiết kiệm phần lớn stage đó; phần Silver còn lại theo phép trừ là **274,226449 s**
(MR:64 trừ M:9173), **không** phải dự báo runtime mới: A vẫn cần sort/hash/write.
Phép thử nhạy cảm nhất là old/new `coverage_validity` trên cùng evidence với các group
sequence lớn, action ghi `receipt_status`, kèm so kết quả hai chiều.

### C — Runner dùng evidence đã ghi làm nguồn coverage_validity

Xếp thứ hai về ngân sách thời gian. Vị trí `scripts/run/run_order_lifecycle_local.py:94–105`.
Điều kiện áp dụng: giữ thứ tự SILVER hiện tại, E trước V. Kiểm thứ tự này khi review;
không chuyển tất cả dữ liệu sang collect phía driver.

```diff
@@ def write_tables(spark, frames, zone, receipt, item):
     for name, frame in frames.items():
+        if zone == 'build_silver' and name == 'coverage_validity':
+            from spark_jobs.lib.order_lifecycle_lakehouse import coverage_validity
+            frame = coverage_validity(result['observed_evidence'])
         target = f'local.{zone}.{name}'
```

Đây là seam riêng của E2E, chưa tối ưu caller production
`spark_jobs/silver/build_order_lifecycle.py`. Caller nào ghi E rồi V cũng cần quyết định vòng đời cache
hoặc tái đọc bảng tương tự; không âm thầm thêm cache sống vô hạn vào API library.
Các test gọi `build_silver` trực tiếp vẫn dùng DataFrame gốc.

Ngân sách quan sát: đoạn root V từ **13:33:12 đến 13:34:22 ≈ 70 s**
(M:8400–8457,8997), cùng shuffle trung gian trước stage cuối lúc **13:34:27** (M:9094).
C có thể giảm một phần đoạn này, nhưng vẫn phải đọc Iceberg E, parse trường coverage,
join và aggregate. Không hứa giảm tổng 38,178+61,250+69,159 s vì ba root chạy song song.
C không làm mất chi phí E gốc **23,235 s** scan + **8,030 s** write (M:8108,8214).

Rủi ro: dependency thứ tự E/V và thay đổi nguồn schema/type do round-trip Iceberg.
Đối chiếu schema, null, timestamp và mọi dòng kết quả V trước/sau; thử output SILVER
đúng thứ tự, xác nhận không phát sinh phụ thuộc bảng từ run cũ (catalog mỗi run riêng).

### B — Không đếm `ops` hai lần

Vị trí `spark_jobs/lib/order_lifecycle_lakehouse.py:436–439`:

```diff
@@ def _dedup(ops: DataFrame) -> tuple[DataFrame, dict]:
+    input_rows = ops.count()
     stats = {
-        "input_rows": ops.count(),
-        "duplicate_rows": ops.count() - unique.count(),
+        "input_rows": input_rows,
+        "duplicate_rows": input_rows - unique.count(),
```

Bỏ stage 8, ngân sách **24,491 s** M:4752 (small **4,327 s**, S:3719),
trừ thay đổi scheduling/cache giữa các run. Không thay cửa sổ dedup, conflict IDs
hay định nghĩa duplicate. Rủi ro thấp trên snapshot Bronze cố định; kiểm stats
retry/conflict/correction giữ nguyên. Các lần `unique.count()`/collect/materialize
events vẫn chạy lại: không tuyên bố B đã giải quyết toàn bộ dedup.

Nếu muốn cache `known` hoặc `unique` để tối ưu thêm: cần API sở hữu cache và giải phóng
trong `finally` cho mọi caller, đo memory/spill trước khi quyết. `known` chứa raw_json
của mọi feed; cache này cộng với events/digest có thể làm xấu heap local. Chưa đề xuất
thêm cache đại trà hoặc tăng driver RAM như bằng chứng giải quyết H1.

## Cách chứng minh giữ nguyên kết quả và đo hiệu quả

Phân tích này dùng log đã ghi, chưa tái hiện trực tiếp biểu thức chậm, chưa có profiler hoặc benchmark sau diff. Cần chạy protocol dưới đây để kiểm chứng thay đổi.

người vận hành áp từng diff độc lập và giữ evidence trước/sau; A trước, C rồi B, cùng nguồn,
seed/cutoff, cấu hình local[*]/driver memory/shuffle và máy không chạy thêm workload.
Ghi event log/physical plan khi tái chạy để gắn chính xác operator và shuffle metrics
vào stage, đồng thời lấy heap/GC/task CPU nếu stage V vẫn chậm. So sánh nhiều lượt
và báo từng lượt; số stage/job có thể đổi nên so theo vai trò, không ghim ID.

Các ca cần thêm vào Spark regression trước khi áp A (chưa viết trong task tài liệu):

- So old/new V đầy đủ bằng `exceptAll` hai chiều và schema; giữ reference hàm cũ
  trong môi trường thử để chạy hai implementation trên cùng DataFrame.
- Coverage rỗng; thiếu partition; sequence null; range chồng lấn/lặp; sequence trùng
  nhưng record count/digest không đúng; missing/unexpected sequence; unavailable;
  phạm vi epoch/dataset khác nhau; nhiều revision và cutoff bằng đúng observed_at.
- Giữ nguyên canonical digest: raw_json có khoảng trắng, thứ tự sid, sự kiện trùng sid
  khác nội dung, retry và correction. `observed_evidence` vẫn giữ mọi revision và
  xung đột; không thay nó bằng `events` latest đã loại conflict.
- Đối chiếu không chỉ count: multiset tất cả cột của bốn bảng Silver, dedup stats,
  quarantine và receipts; test điểm trước/sau refund correction và as_of native.
- Phép đo riêng A: materialize evidence dùng chung, group sequence tăng dần,
  ghi V với receipt_status, ghi thời gian/CPU và equality old/new. Chưa có script
  này trong repo; người vận hành cần bổ sung, pytest hiện có không đo hiệu năng Spark.

Lệnh người vận hành phải chạy sau khi có bản sửa (image/cache offline có sẵn):

```bash
# Regression Silver fixture và scale, không mạng, không catalog dự án.
docker run --rm --pull never --network none --read-only --tmpfs /tmp:rw,size=1g \
  --mount type=bind,src="$PWD",dst=/workspace,readonly --entrypoint python \
  platform-airflow:2.10.5-spark3.5.3 /workspace/tests/spark/check_order_lifecycle_silver.py

docker run --rm --pull never --network none --read-only --tmpfs /tmp:rw,size=2g \
  --mount type=bind,src="$PWD",dst=/workspace,readonly --entrypoint python \
  platform-airflow:2.10.5-spark3.5.3 /workspace/tests/spark/check_order_lifecycle_silver_scale.py

# E2E tạo PostgreSQL/catalog riêng, giữ log/receipt, không dùng DB dự án.
bash tests/harness/run_order_lifecycle_scale.sh --profile small --driver-memory 6g
bash tests/harness/run_order_lifecycle_scale.sh --profile medium --driver-memory 6g
```

Harness có lệnh kiểm cache/image và Docker network internal theo thiết kế sẵn;
người vận hành chạy các lệnh này, Codex không chạy. So small/medium Silver seconds,
stage cuối V, phân bố task, các bảng/kiểm nghiệp vụ; medium có lỗi Gold hiện tại
thì báo riêng, không coi `status=running`/Silver passed là E2E passed.
Để đo đối chứng công bằng, chạy lại bản cũ cùng `--driver-memory 6g` và nguồn với bản mới;
không dùng small lịch sử như bằng chứng tài nguyên hoàn toàn giống medium.

## Kiểm tra đã thực hiện trong task

- Trích stage/job/task từ cả hai spark.log, khoanh Silver bằng scan/write/count
  trong runner; không chạy Spark, không sửa mã thực thi.
- Đã đọc toàn bộ `build_silver`, các helper được gọi và runner, thuật ngữ và
  `docs/shared/architecture_decisions.md` liên quan Bronze/Silver.
- `.venv/bin/pytest -q`: **679 passed, 1 skipped in 44.95s** (exit 0). Đây là test Python; không xác nhận hiệu năng hoặc tương đương Spark của diff chưa áp dụng.

Không có khác biệt phạm vi so với brief/ANSWER: tài liệu ở đúng đường dẫn;
chưa áp các diff đề xuất; chưa có bằng chứng thời gian tiết kiệm sau sửa.

## Tái tạo bảng xếp hạng từ log, không cần Spark

Chạy từ root repo; script chỉ đọc log. Dòng submit được trích cùng ID stage, tránh
đếm số dòng `Finished task` và nhầm task retry thành số partition.

```bash
python - <<'PY'
from pathlib import Path
import re
for folder, lo, hi in [('e2e-v2-medium.G5BI4k', 4479, 9244),
                       ('e2e-v2-small.bpqNq2', 3511, 7278)]:
    tasks, ranked = {}, []
    for n, line in enumerate(Path('out', folder, 'spark.log').read_text().splitlines(), 1):
        m = re.search(r'Submitting (\d+) missing tasks from (?:ShuffleMapStage|ResultStage) (\d+)', line)
        if m:
            tasks[int(m[2])] = (int(m[1]), n)
        m = re.search(r'(ShuffleMapStage|ResultStage) (\d+) \((.*)\) finished in ([\d.]+) s', line)
        if m and lo <= n <= hi:
            sid = int(m[2])
            ranked.append((float(m[4]), sid, tasks[sid], n, m[3]))
    print(folder, '(seconds, stage, (tasks, submit_line), finish_line, callsite)')
    for row in sorted(ranked, reverse=True)[:20]:
        print(row)
PY
```
