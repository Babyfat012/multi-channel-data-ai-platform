# Spark baseline và skew — workload v2

> **Pipeline v2 (order lifecycle) — thiết kế và thử ở local, không phải pipeline đã nộp.** Pipeline chạy trên AWS là v1 (DP1 → DP4 + ML), xem [Data Platform evidence](../evidence.md). Các chỗ nhắc GCP dưới đây là kế hoạch cũ, chưa chạy; đích cloud hiện hành là [AWS](../../shared/aws_deploy.md).

workload lifecycle v2, độc lập với bằng chứng Bronze v1 tại
[spark.md](spark.md). **Chưa đo Spark trong sandbox Codex**. Các số trong bảng
chỉ được điền sau khi người vận hành chạy và lưu JSON/event log, không suy từ thời gian.

Job [measure/measure_order_lifecycle_skew.py](../../../spark_jobs/measure/measure_order_lifecycle_skew.py) gọi trực tiếp
`bronze_from_root` → `build_silver` → `build_gold` của module thật, gồm tuyến
`line_evidence` theo line/order/correction. Không sao chép pipeline Silver/Gold.
Sau đó checkpoint cùng fact/segments một lần rồi chạy `sku_features` gốc và
[`sku_features_salted`](../../../spark_jobs/lib/order_lifecycle_skew.py). Tách job group
`silver_gold_build`, `baseline`, `salted`, `correctness`; so feature chỉ tính phần
sweep và các join liên quan, không tính lại Silver trong mỗi phương án. Không ghi
catalog/Iceberg, không phải benchmark tốc độ ghi lakehouse hay JDBC Walmart.

Baseline mặc định: Spark local[2], UTC, shuffle partitions=16, AQE=false,
`skewJoin.enabled=true` (chỉ có hiệu lực khi bật AQE), auto-broadcast=-1 để phân bố
shuffle dễ quan sát. Salted giữ đúng cấu hình ấy. Đây là cấu hình thí nghiệm,
không phải đề nghị thay cấu hình production. `--aqe` cho phép đo đối chứng riêng;
không gộp số của AQE on vào bảng AQE off. Khi AQE đổi plan hoặc broadcast exchange,
cần kiểm lại nhận diện stage trước khi so; giữ toàn bộ SQL plan/event log.

Nguồn sinh mặc định: seed=7, days=90, orders_per_day=20, sku_count=100,
Zipf s=1.2, followup=270, observation=285. Khoảng tạo đơn đủ dài để feature có
history mature, khác mẫu 10 ngày thường chỉ cho feature 0. Test Spark nhỏ dùng
90 ngày × 4 đơn/ngày, 32 SKU, cùng seed. Không khẳng định hai bộ này có cùng
`dataset_version`. Nguồn cụ thể được pin trong JSON bằng `dataset_versions`,
manifest đầy đủ (seed/cấu hình/digest), SHA-256 manifest và cutoff; muốn replay dùng
lại `--source` trỏ đúng root bất biến. Dataset ID phụ thuộc code/Python, không đoán
ID trước khi sinh. Runtime JSON có Spark/Python/app ID; người vận hành ghi thêm image ID
thực tế cùng bằng chứng. Có thể dùng nguồn 30 ngày đã ghim trong
[source-order-lifecycle-scale.md](source_order_lifecycle_scale.md), nhưng phải kiểm `nonzero_features` trước
khi dùng làm proof correctness có history.

| Chỉ số, cùng input + AQE off | Baseline SKU | Salted SKU + bucket |
|---|---|---|
| dataset_version + seed + manifest hash | chưa đo | cùng nguồn |
| Spark/Python/image ID/app ID | chưa đo | cùng runtime |
| sweep stage ID + số task | chưa đo | chưa đo |
| records/task max / median / p95 | chưa đo | chưa đo |
| records/task max ÷ median | chưa đo | chưa đo |
| shuffle records/task max / median / p95 | chưa đo | chưa đo |
| shuffle bytes/task max / median / p95 | chưa đo | chưa đo |
| top-5 key theo số sự kiện sweep | chưa đo | chưa đo |
| exceptAll baseline-only / salted-only | chưa đo | yêu cầu 0 / 0 |

**Chưa đo:** chạy `python tests/spark/check_order_lifecycle_skew.py`
trong runtime Spark đã chuẩn bị; lệnh Docker và CLI lưu JSON/event log đầy đủ ở
cuối tài liệu. Chưa có stage/task metrics,
manifest hash, bag equality hay kết luận giảm skew v2; bảng trên giữ trạng thái
chưa đo, không dùng bằng chứng v1 thay thế.

JSON có mọi stage, từng task thành công (partition index, task attempt, input
records, shuffle records, shuffle bytes, duration), max/median/p95 và max/median.
`records_read` là input records + shuffle records; đọc cache có thể trả 0, nên
**không gọi nó là số output rows**. Shuffle bytes gồm remote + local bytes.
Median gồm cả task rỗng; median=0 thì ratio=null, không thay mẫu số bằng 1. p95 là
nearest-rank. Retry stage/task giữ riêng stage attempt, chỉ giữ attempt thành công
cuối của mỗi partition; stage lỗi không dùng để kết luận.

Nhận diện sweep dựa vào metric accumulator của SQL Exchange hashpartitioning
(dataset_version, seller_sku[, _bucket]) cấp dữ liệu cho Window qua Sort/wrapper,
và stage consumer trực tiếp của exchange đó. Exchange của aggregate/join trùng
khóa không được tự gắn vai trò sweep. Đọc cả SQLExecutionStart lẫn cập nhật plan
AQE. `sweep_exchange_inputs` đánh dấu ứng viên; đối chiếu physical plan, parent stage
và task metrics trong JSON trước khi chọn stage. Salted có cả prefix theo SKU trên
**một dòng tổng mỗi bucket**, cần phân biệt với sweep cục bộ `bucket_sweep`.
Một consumer có thể nhận thêm shuffle từ join; JSON giữ nguyên số tổng thật của
task, không quy hết bytes đó cho window. `top5_partition_keys` đếm endpoint mở/đóng
và query từ feature windows thật; đây là **tải logical key**, không phải phép gán
chính xác key vào task. Các join order/line ở group build vẫn có metrics riêng.

Test Spark bắt buộc có stage đo được, feature khác 0 và bag equality bằng
`exceptAll` **hai chiều**. Test in max/median và bytes trước/sau. Chỉ assert ratio
giảm khi baseline ratio≥2, ít nhất 8 task, cả hai median>0 và logical key max sau
chia bucket ≤60% trước. Nếu không đủ, in `SKEW_INCONCLUSIVE`, không coi test pass
là proof đã giảm skew; người vận hành phải tăng scale/chọn số bucket rồi chạy lại.
Không dùng thời gian một lần chạy để tuyên bố speedup.
Script còn có fixture dựng tay đối chiếu hai chiều tại biên microsecond/bucket,
cửa sổ nửa mở, query-only bucket, tự trừ history, nhiều dataset, SKU null và
line không có created_at; kiểm số đếm cụ thể ngoài phép so với baseline.

AQE `skewJoin` xử lý partition lệch của **join**. Window cumulative theo
(dataset_version, seller_sku) vẫn đòi toàn bộ timeline một SKU có thứ tự và cùng
partition; bật AQE không tự chia window đó. SKU đầu Zipf có thể chiếm >10% line.
Bài học v1 “AQE không chữa write skew” cũng có giới hạn operator tương tự, nhưng
v2 ở đây là **window skew**, không phải write skew hay skewJoin.

Salting dùng khoảng UTC cố định tính từ epoch, độ rộng `ceil(180/buckets)` ngày:
18 buckets → khoảng 10 ngày, không phải ngẫu nhiên hash line. Tách delta mở/đóng
và query theo bucket; tính tổng dc/dp mỗi bucket, prefix tổng của mọi bucket
**trước đó**, rồi cộng vào cumulative sum cục bộ. Delta tại timestamp trùng query
đứng trước query, giữ nguyên [a,b), microsecond và bước trừ lịch sử chính line.
Bucket query-only cũng tham gia prefix; tổng âm đóng cửa sổ truyền đúng qua các
khoảng thưa. Không cắt đoạn hoặc replicate history vào mọi bucket. Mọi SKU được
chia, kể cả SKU lạnh: bớt một bước thu danh sách hot SKU nhưng thêm overhead.

Đánh đổi: thêm aggregate, prefix, join và shuffle; nhiều nhóm/partition nhỏ, có thể
tăng tổng shuffle bytes và chi phí scheduling. Prefix vẫn theo SKU nhưng input
đã giảm còn số bucket có dữ liệu; nếu timeline rất dài, prefix đó cũng cần đo.
Nếu SKU nóng tập trung trong một bucket thì phương án không chữa được skew.
Không suy rằng nhanh hơn ở mọi quy mô và không áp dụng vào module Gold gốc khi
chưa có bằng chứng Spark. Python oracle ở
[unit/spark_jobs/lib/test_order_lifecycle_skew.py](../../../tests/unit/spark_jobs/lib/test_order_lifecycle_skew.py) kiểm cơ chế bucket/prefix;
correctness DataFrame phải qua Spark test.

Lệnh người vận hành bắt buộc (mount repo chỉ đọc, dữ liệu tạm trong container):

```bash
docker run --rm --network none --read-only --tmpfs /tmp:rw,size=2g -e PYSPARK_SUBMIT_ARGS='--driver-memory 4g pyspark-shell' \
  --mount type=bind,src=$PWD,dst=/workspace,readonly --entrypoint python \
  platform-airflow:2.10.5-spark3.5.3 /workspace/tests/spark/check_order_lifecycle_skew.py
```

Để lưu stdout JSON trên host, thêm `> /tmp/codex-15-spark-report.log` vào lệnh.
Run này không xóa `/tmp` riêng trong test để người vận hành có thể điều tra khi chạy ngoài
container; `--rm` vẫn dọn container sau khi kết thúc. Muốn giữ cả event log, chạy
CLI với thư mục bằng chứng host được mount **riêng** cho phép ghi:

```bash
mkdir -p /tmp/codex-15-evidence
docker run --rm --network none --read-only --tmpfs /tmp:rw,size=2g \
  -e PYSPARK_SUBMIT_ARGS='--driver-memory 4g pyspark-shell' \
  --mount type=bind,src=$PWD,dst=/workspace,readonly \
  --mount type=bind,src=/tmp/codex-15-evidence,dst=/evidence \
  --entrypoint python platform-airflow:2.10.5-spark3.5.3 \
  /workspace/spark_jobs/measure/measure_order_lifecycle_skew.py \
  --workdir /evidence/run-01 --output /evidence/report.json \
  --days 90 --orders-per-day 20 --seed 7 --sku-count 100 --buckets 18 --partitions 16
```

Thư mục workdir phải mới cho mỗi run; dữ liệu scale, checkpoint tạm và output
Parquet tốn đĩa. Với nguồn lớn dùng mount nguồn read-only `--source`, tăng dung
lượng đĩa tạm do người vận hành quyết định. Không gọi Docker/Cloud trong sandbox Codex.
