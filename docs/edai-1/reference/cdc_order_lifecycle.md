<!-- Mô tả thiết kế, vận hành và giới hạn CDC trên nguồn Walmart v2; chưa có số đo service. -->
# CDC Walmart v2 → Bronze có oracle

> **Pipeline v2 (order lifecycle) — thiết kế và thử ở local, không phải pipeline đã nộp.** Pipeline chạy trên AWS là v1 (DP1 → DP4 + ML), xem [Data Platform evidence](../evidence.md). Các chỗ nhắc GCP dưới đây là kế hoạch cũ, chưa chạy; đích cloud hiện hành là [AWS](../../shared/aws_deploy.md).

Nhánh này dành cho `walmart_lifecycle.orders`, tách hoàn toàn CDC v1 `walmart.orders`.
Tên “v2” trong `docs/edai-1/reference/cdc.md` trước đây chỉ phiên bản boundary/epoch của nhánh v1;
không phải CDC nguồn `sim-native-v2` trong tài liệu này.

| Thành phần | Giá trị |
|---|---|
| Connector | `platform-walmart-order-lifecycle` |
| Publication và replication slot | `platform_walmart_order_lifecycle_orders` |
| Topic (một partition) | `platform_cdc_order_lifecycle.walmart_lifecycle.orders` |
| Bronze (Iceberg) | `lakehouse.bronze.lifecycle_raw_walmart_cdc_events` |
| Contract | `sim-native-v2/walmart/1` |
| DAG | `dp_order_lifecycle_cdc`, paused, trigger tay |

DDL thật trong `scripts/setup/load_order_lifecycle_walmart.py` có **PRIMARY KEY
(dataset_version, purchase_order_id)**. Setup kiểm đúng khóa này rồi chọn
`REPLICA IDENTITY DEFAULT`: UPDATE/DELETE giữ được khóa cũ, không cần FULL như
v1 không có PK. `raw_payload` là TEXT. Với DEFAULT, before của update/delete
có thể chỉ có khóa, nên `payload_before` có thể null; không hứa ảnh cũ đầy đủ.
Sau của insert/update giữ nguyên TEXT, không canonical lại ở Spark.
`snapshot.mode=no_data` vì lịch sử đã bootstrap bằng JDBC qua job `spark_jobs/bronze/ingest_order_lifecycle.py`; nhánh CDC
chỉ lấy commit sau khi connector/slot sẵn sàng. Không tự backfill khoảng trống
trước đăng ký connector. Không reload fixture qua loader khi đang đo: loader
xóa/chèn dataset sẽ tạo thêm CDC thật và validator sẽ báo extra.

## Oracle và đối soát

`scripts/run/mutate_order_lifecycle_cdc.py` kiểm connector và task RUNNING, slot active, chọn mẫu
có thứ tự xác định rồi sao chép thành khóa probe theo seed. Mỗi thao tác là một
transaction commit riêng: insert (Created), update raw_payload (Shipped), delete.
Không sửa đơn bootstrap. C/u được kiểm bằng `Registry` và `validate_record` của
`spark_jobs.lib.data_contracts`; payload có đủ cấu trúc line, charge, SKU, quantity.
Kết thúc bình thường nguồn trở về số dòng ban đầu.

Oracle mặc định `order_lifecycle_cdc_expected.json` ghi op `c/u/d`, khóa kép,
`after_sha256`, `commit_order`, raw_payload và thời điểm COMMIT trả về. Hash là
SHA256 đúng byte UTF-8 của TEXT sau, **delete có after null và hash null**.
Oracle độc lập với Kafka/Bronze. Nội dung plan tái lập theo seed và mẫu bootstrap;
thời điểm commit là số đo thực nên không tái lập. Oracle ghi atomic sau mỗi
commit; chỉ `complete=true` sau đủ ba commit. Nếu tiến trình hỏng giữa commit và
ghi oracle, file chưa complete sẽ bị validator từ chối. Không tự rollback các
commit đã thành công, không tự ghi đè oracle cũ; người vận hành cần kiểm nguồn và chọn
một lượt chạy cô lập mới khi có lỗi.

Job `spark_jobs/cdc/ingest_order_lifecycle_changes.py` dùng helper `spark_jobs/lib/cdc_offsets.py` có sẵn: chốt cluster ID, topic UUID, bộ
partition, start inclusive/end exclusive trước khi đọc; lưu boundary checksum
atomic vào control directory POSIX và chép boundary vào receipt. Kafka đọc
`read_committed`, `failOnDataLoss=true`; retry giữ offset cuối cũ. Kiểm incarnation
và retention trước/sau đọc và sau ghi. Không recreate/reset topic đồng thời job;
đây không phải khóa broker. Bounded mặc định từ earliest còn giữ đến latest đã
commit tại lúc bắt đầu. Một lượt proof cần topic mới/cô lập chỉ chứa oracle này;
trên topic đã có lịch sử, oracle phải mô tả toàn bộ range đó, nếu không validator
sẽ báo extra. Không tự bỏ qua sự kiện ngoài probe.

Cột Bronze: `op`, `dataset_version`, `purchase_order_id`, `payload_before`,
`payload_after`, `source_lsn`, `source_ts` (epoch milliseconds của commit nguồn),
`topic`, `kafka_partition`, `kafka_offset`, `source_epoch` (cluster/topic UUID),
`run_id`, `record_sha256`, `event_json`. Không lọc thông điệp lỗi/null âm thầm.
Receipt ingest lưu boundary trước append, trạng thái hoàn tất và số dòng Kafka.

Bronze **append**, không MERGE vào `raw_source_records`: giữ lịch sử các thay
đổi và bằng chứng offset, đổi lại Silver chưa nhận trạng thái CDC mới. Xử lý
snapshot + CDC, delete/tombstone và cutoff nhất quán là quyết định tiếp theo.
Cùng `run_id` đã có dòng thì job `spark_jobs/cdc/ingest_order_lifecycle_changes.py` bỏ append khi retry, rồi job `spark_jobs/cdc/validate_order_lifecycle_changes.py` kiểm toàn bộ
run; Iceberg append atomic. DAG giới hạn một run/task đồng thời. Chưa hỗ trợ hai
driver ghi cạnh tranh cùng run; run mới append lại range cũ sẽ tạo một proof
riêng chứ không phải ingest tăng dần toàn cục.

Job `spark_jobs/cdc/validate_order_lifecycle_changes.py` đọc toàn bộ dòng cùng run, không lọc epoch sai trước khi so. Đối soát
multiset theo khóa kép/op rồi hash payload sau (tính lại từ TEXT, kiểm cả cột
hash); kiểm thứ tự op bằng Kafka offset trong cùng khóa, missing, extra,
payload_mismatch, order_mismatch, epoch_mismatch. Cũng bắt duplicate offset,
row invalid và ngoài boundary. Receipt JSON có checks, số dòng, boundary,
epoch và thời điểm hoàn tất, exit khác 0 khi vi phạm. Validator dùng frozen
boundary trong receipt/control store, không cần Kafka đang online. Vì thu
oracle và run về driver, nhánh proof này dành cho lượng mutation có kiểm soát;
chưa là đối soát CDC quy mô lớn phân tán.

## Chạy thủ công và DAG

Không dùng những lệnh này trên dịch vụ đang có nếu chưa được người vận hành chọn.
Cấu hình `.env` theo loader hiện hữu; setup/mutate còn nhận
`POSTGRES_HOST`, `POSTGRES_CONNECT_HOST` (DNS từ Connect tới DB),
`KAFKA_BOOTSTRAP_SERVERS`, `DEBEZIUM_URL`. Mặc định setup dùng host
`127.0.0.1` để gọi DB/Kafka/REST, connector gọi PostgreSQL qua DNS `postgres`.

```bash
.venv/bin/python scripts/setup/load_order_lifecycle_walmart.py --root <fixture> --env-file <env>
.venv/bin/python scripts/setup/setup_order_lifecycle_debezium.py --env-file <env>
# Chờ connector/task RUNNING và slot active (harness đã tự poll).
.venv/bin/python scripts/run/mutate_order_lifecycle_cdc.py --env-file <env> --seed 11 \
  --output <control-dir>/order_lifecycle_cdc_expected.json
# Chờ Kafka đủ ba sự kiện và chốt range sau đó, hoặc dùng harness bên dưới.
```

DAG chỉ chạy `ingest_cdc` → `validate_cdc`, không tự mutation nguồn.
`scripts/setup/setup_airflow.sh` được thêm Variables `platform_order_lifecycle_cdc_control_dir`,
`platform_order_lifecycle_cdc_oracle`, `platform_order_lifecycle_cdc_spark_packages`. RAM/cores dùng lại
`platform_order_lifecycle_driver_memory`, `platform_order_lifecycle_executor_memory`, `platform_order_lifecycle_executor_cores`.
Subclass bổ sung cả ba vào `template_fields`. Cần mount script/helper mới và
oracle/control directory cùng đường dẫn giữa các driver, cấp quyền ghi receipt,
và cấu hình Connection `platform_kafka`. Topic v2 được job cố định, không dùng
Variable topic v1. Sau mutate phải poll đủ sự kiện rồi mới trigger DAG; trigger
sớm có thể chốt range thiếu, cần run mới sau khi nguồn đã theo kịp.

## Harness để người vận hành chạy

```bash
bash tests/harness/run_order_lifecycle_cdc.sh
```

Không chạy compose. Harness tạo network Docker riêng và container tạm
`postgres:15` (`wal_level=logical`), `apache/kafka:3.9.1`,
`quay.io/debezium/connect:3.4`, `platform-airflow:2.10.5-spark3.5.3`.
Tất cả image phải có sẵn (`--pull never`); không thêm dependency. Python host
`.venv/bin/python` cần psycopg và confluent_kafka hiện có. Cache volume
`platform_airflow-data` phải tồn tại. Harness `ls /ivy/.ivy2/jars`, dừng và nêu rõ
jar thiếu nếu cache thiếu các thành phần sau:

- `org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.11.0.jar`
- `org.apache.spark_spark-sql-kafka-0-10_2.12-3.5.3.jar`
- `org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.5.3.jar`
- `org.apache.kafka_kafka-clients-*.jar`
- `org.apache.commons_commons-pool2-*.jar`

Spark dùng các jar trong cache, không dùng `--packages` tải Maven. Warehouse
Hadoop local đặt trong `/tmp/platform-cdc-v2.*/warehouse` trên host (mount `/proof`
trong container), không cần Lakekeeper/S3. Harness sinh fixture mới bằng
producer rồi nạp qua loader. Poll readiness có timeout, đợi connector và slot,
mutate, poll message/offset và so oracle đến khi đủ, lưu `poll.json`, chạy job
26 + 27. Sau lượt sạch, append đúng một bản ghi giả vào Bronze: lượt validator
sau phải exit khác 0 và receipt có `extra=1`. `trap` dọn đúng tên container/mạng
riêng, giữ thư mục `/tmp` và receipts cho người vận hành. Chưa chạy service harness
trong lượt triển khai vì luật brief cấm bật/tắt Docker service.

## Đề xuất GCP — người vận hành quyết

`docker-compose.gcp.yml` hiện chưa có Kafka + Debezium. Đề xuất **overlay mới**
trong bước sau: Kafka 3.9.1 single broker KRaft trên mạng nội bộ và volume riêng;
Connect 3.4 trên cùng mạng, config/offset/status topics replication factor 1;
PostgreSQL logical WAL, publication/slot và quyền replication; lưu oracle và
control directory bền vững cùng driver Airflow. Không expose PLAINTEXT/REST ra
Internet. Cấu hình listener DNS đúng từ executor và Connect, giới hạn WAL của
slot và theo dõi backlog khi Connect dừng.

Ước lượng ngân sách để người vận hành thử: Kafka heap 768 MiB, container 1.5–2 GiB;
Connect heap 768 MiB, container 1.25–1.5 GiB; cộng khoảng 3–4 GiB trên VM bên
cạnh PostgreSQL và Spark. Spark theo Variables đang có: driver 3g, executor 4g,
2 cores; cần tính tổng theo số executor, không suy diễn đã vừa VM. Đây là đề
xuất chưa benchmark, chưa cấu hình bảo mật production hay thay compose.

## Số đo nghiệm thu

người vận hành chạy `bash tests/harness/run_order_lifecycle_cdc.sh` ngày 2026-10-19 (container tạm postgres:15, Kafka 3.9.1, Debezium 3.4,
Spark trong `platform-airflow:2.10.5-spark3.5.3`): **`CDC v2 harness PASS`**, 58 s. Receipt:
[`evidence/cdc-v2-local-20261019.json`](../../evidence/cdc-v2-local-20261019.json).

| Run | Oracle | Bronze | Missing | Extra | Payload | Order | Epoch | Kết quả |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| Sạch (insert/update/delete) | 3 | 3 | 0 | 0 | 0 | 0 | 0 | passed |
| Tiêm 1 bản ghi giả vào Bronze | 3 | 4 | 0 | **1** | 0 | 0 | 0 | failed (đúng kỳ vọng; `outside_boundary` = 1) |

người vận hành sửa harness khi chạy: Spark chạy bằng user mặc định của image (uid host không có trong passwd ⇒ Hadoop
login NPE), `HOME`/ivy ghi được, driver 3g, mở quyền WORK cho container và `chown` file về uid host sau mỗi bước.
