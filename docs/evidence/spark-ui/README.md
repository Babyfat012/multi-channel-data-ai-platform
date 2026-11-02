# Bằng chứng Spark UI — Bronze và Silver

## Bổ sung Silver ngày 2026-10-15

App `app-20261014035356-0002` thuộc task Amazon Silver trong DP2 run ngày
2026-10-14, đã hoàn tất không có job thất bại. Execution **3** là phần xử lý
thật; execution **2** chỉ là lệnh ghi bảng bao ngoài.

| Artifact | Nội dung và cách đọc |
|---|---|
| `09-silver-amazon-jobs.png` | Job của application Silver đã chạy |
| `10-silver-amazon-sql.png` | SQL execution 2 có sub-execution 3 |
| `11-silver-amazon-query-plan.png` | Ảnh cũ của execution 2; chỉ thấy `AtomicReplaceTableAsSelect`, không dùng làm proof parse/dedup |
| `12-silver-amazon-dedup-plan.png` | Graph execution 3: hai nhánh schema → Union → window dedup; 8.449.302 → 8.314.312 dòng |
| `13-silver-amazon-schema-details.png` | Details của plan với parse schema nguồn |
| `silver-amazon-execution-3.json` | REST execution hoàn tất, physical plan và metrics của đúng run |

Ảnh được nhúng và diễn giải trong [Spark mục 10](../../edai-1/reference/spark.md). Script
`scripts/capture_spark_silver_evidence.py` chụp History UI thật và lưu REST
response, không chạy lại job.

## Bằng chứng Bronze lịch sử

Ảnh chụp 2026-10-11 từ **Spark History Server**, không phải UI cổng 4040 (UI đó chết cùng
job nên không chụp được job chạy 19 phút). Nguồn: ba event log trong `out/spark-events/`.

Bật lại History Server để tự xem:
```bash
cd /home/phattl/eonsr/FSDS
docker run -d --rm --name fsds-spark-history -p 18080:18080 --user root \
  -v "$PWD/out/spark-events:/spark-events:ro" \
  -e SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=file:///spark-events" \
  apache/spark:3.5.3-java17 /opt/spark/bin/spark-class \
  org.apache.spark.deploy.history.HistoryServer
# rồi mở http://localhost:18080 ; xong thì: docker stop fsds-spark-history
```

| Lần | App ID | Cấu hình | Thời gian |
|---|---|---|---|
| 1 | `app-20261010103931-0000` | 20 ngày, AQE **off** | 75,8s |
| 2 | `app-20261010104107-0001` | 20 ngày, AQE **on** | 86,6s |
| 3 | `app-20261011080203-0001` | 180 ngày, AQE **on** | 1.153,8s (19 ph 11 gy) |

Hai app còn lại trong History Server **không phải bằng chứng**, đừng dùng:
`app-20261010104633-0002` (lần 3 chết giữa đường 10-10) và `app-20261011080057-0000`
(gãy sau 14s vì quên bật MinIO).

## Từng ảnh nói gì

| Ảnh | Nội dung | Số cần chỉ vào |
|---|---|---|
| `00-history-danh-sach-3-lan-chay.png` | Trang chủ History Server, thấy cả 5 app | định danh 3 app bằng chứng |
| `01-lan1-aqe-off-stages.png` | Danh sách stage, lần 1 | stage 1 là stage ghi |
| `02-lan1-aqe-off-stage-ghi.png` | **Ảnh skew rõ nhất.** Stage 1, 200 task | Duration: Min **1,0ms** / Median **3,0ms** / Max **19s**. 180/200 task rỗng, vài task ôm hết việc |
| `03-lan2-aqe-on-stages.png` | Danh sách stage, lần 2 | stage 2 là stage ghi |
| `04-lan2-aqe-on-stage-ghi.png` | **Ảnh AQE gộp task.** Stage 2, còn **8 task** | Duration: Min **6s** / Median **21s** / Max **23s**. Task đều hơn nhưng task chậm nhất **tệ hơn** lần 1 (19s → 23s) |
| `05-lan3-180ngay-jobs.png` | Danh sách job lần 3 | tổng thời gian job |
| `06-lan3-180ngay-stage-ghi.png` | Stage 3 = stage **ghi** của lần 3, **69 task** (AQE gộp từ 1.002) | Output **7,9 GiB / 108.248.563 dòng**. Duration: Min **11s** / Median **33s** / Max **1,1 phút** |
| `07-lan3-180ngay-stage-doc.png` | Stage 1 = stage **đọc** của lần 3, 1.002 task | Input **80,8 GiB / 108.248.563 dòng**. Duration Median **2s**, Max **11s** → đọc KHÔNG phải chỗ nghẽn |
| `08-lan3-180ngay-stages.png` | Toàn bộ stage lần 3 | thấy stage 2 bị SKIPPED do AQE thay bằng stage 3 |

## Chuỗi lập luận ba ảnh (dùng cho phần "tìm và sửa vấn đề hiệu năng")

1. Ảnh `02`: tắt AQE → task lệch nhau khủng khiếp (3ms so với 19s). Skew có thật.
2. Ảnh `04`: bật AQE → task đều hơn, **nhưng chậm hơn** (75,8s → 86,6s) và task chậm nhất
   **tệ hơn** (19s → 23s). **Giả định ban đầu bị số đo bác bỏ.**
3. Nguyên nhân thật: job này **không có JOIN**, mà AQE chỉ cắt skew trong JOIN. Chỗ nghẽn
   là `write.distribution-mode=hash` dồn mỗi `export_date` về một task — AQE không chia
   việc ghi một partition cho nhiều task được.
4. Ảnh `06`: ở quy mô 180 ngày, skew vẫn còn (median 33s, max 1,1 phút = gấp 2) nhưng
   không chặn được việc. Quyết định 2026-10-11: **giữ `hash`** (lý do và phép đo tiếp theo ở [Spark mục 5](../../edai-1/reference/spark.md)).
