# Gold Order Line và feature offline

## Bảng `lakehouse.gold.fact_order_lines_dp2`

Độ hạt: **một sản phẩm trong một Order của một Channel**. Job
`spark_jobs/gold/build_order_snapshot_lines.py` gộp năm bảng Silver sau khi từng bảng đã
được validate. Khóa `order_line_key` là SHA-256 của bộ `channel`, mã Order gốc,
mã Order thứ hai nếu có và mã Order Line gốc. Walmart luôn có
`channel_secondary_order_id = customer_order_id`, nên hai Order cùng
`purchase_order_id` vẫn tách biệt.

| Nhóm cột | Ý nghĩa |
|---|---|
| `channel`, `channel_order_id`, `channel_secondary_order_id`, `channel_line_id`, `order_line_key` | Định danh và khóa xuyên năm Channel |
| `amazon_order_version_key` | Khóa phiên bản SCD2 Amazon tương ứng snapshot trong fact; bốn Channel khác để null |
| `created_at`, `last_updated_time`, `order_status`, `channel_status` | Thời gian và trạng thái snapshot quan sát được |
| `seller_sku`, `product_name`, `quantity` | Sản phẩm của Seller; tên Walmart và ba Channel mới có sẵn, Amazon có thể null |
| `line_gross_amount`, `line_discount_amount`, `line_net_amount`, `currency` | Tiền trên từng Order Line; Walmart chỉ có tiền net, hai cột gross/discount null |
| `city`, `postal_code`, `country` | Địa chỉ nhận từ Channel |
| `estimated_delivery_start`, `estimated_delivery_end` | Mốc **dự kiến**; không phải ngày giao thật |
| `source_export_date`, `source_record_ref`, `source_silver_table`, `source_run_id`, `gold_created_at`, `run_id` | Truy vết nguồn và lần xử lý |

Các cột tiền giữ đơn vị của nguồn. Truy vấn tổng doanh thu phải nhóm theo
`currency`; không cộng USD và VND khi chưa có tỷ giá. Bảng không có nhãn trả
hàng hoặc thời điểm giao thật. Quyết định và nguồn dữ kiện xem
`docs/edai-1/reference/gold_design.md`.

## Bảng `lakehouse.gold.dim_amazon_order_dp2`

Một dòng cho mỗi cặp `(order_id, valid_from_ts)` quan sát được trong các export
Amazon Bronze. `valid_from_ts` là `LastUpdateDate`/`lastUpdatedTime` của trạng
thái; `valid_to_ts` là mốc bắt đầu phiên bản kế tiếp và không bao gồm mốc đó.
Phiên bản cuối có `valid_to_ts = NULL`, `is_current = true`. Cột
`first_observed_export_date` giữ ngày đầu hệ thống nhìn thấy phiên bản. Khóa
`spark_jobs.lib.order_version_key` là SHA-256 từ Order ID và thời điểm đổi trạng thái, cùng
cách tính với `amazon_order_version_key` trong fact. Nếu hai export cùng mốc
có trạng thái chuẩn khác nhau, job dừng; hai cách viết nguồn như `Shipped` và
`SHIPPED` được coi là cùng trạng thái và giữ chữ nguồn từ export mới nhất.

Run DP2 `manual__2026-10-14T03:53:52+00:00` sau khi bổ sung SCD2 hoàn tất
**14/14 task**. Validate ghi **5.779.561** phiên bản, gồm **5.734.309** phiên
bản hiện hành và **45.252** phiên bản hết hiệu lực; `invalid=0`,
`duplicate=0`, `multiple_current=0`, `bad_fact=0`, `missing_version=0`.
Gold fact cũng đạt `invalid=0`, `duplicate_order_line_key=0` sau lần ghi lại.

![Airflow Graph DP2 có SCD2 và Gold, 14 task success](../../images/airflow/dp2_silver-graph-scd2-five-channels.png)

![Airflow Grid DP2 có SCD2 và Gold](../../images/airflow/dp2_silver-grid-scd2-five-channels.png)

## Bảng `lakehouse.gold.feat_order_line_return_dp3`

Độ hạt giống fact. Job `spark_jobs/gold/build_order_snapshot_return_features.py` lấy duy nhất cột có
thể biết **ngay lúc đặt Order**: Channel, SKU, tiền, số lượng, quốc gia và các
thành phần lịch từ `created_at`. `event_timestamp` bằng `created_at`; `created`
là lúc ghi bảng feature. Hai cột này tách thời điểm sự kiện khỏi thời điểm
feature được tính. Walmart không có giá gốc và giảm giá, nên
`discount_fraction` là null cho Channel đó.

Job không đọc `order_status`, `channel_status`, `last_updated_time`,
`delivered_at`, nhãn hay `label_available_at` làm feature. Nhãn Walmart được
nối trong bảng riêng qua `spark_jobs/gold/build_walmart_order_snapshot_labels.py` theo quyết
định Q3, không nằm trong bảng feature.

## Xác nhận runtime

Trước khi thêm SCD2, cùng DP2 run có 12/12 task và bảng Gold được ghi từ năm
bảng Silver. Phân bố Order Line không đổi sau khi thêm khóa phiên bản:

| Channel | Silver | Gold |
|---|---:|---:|
| Amazon | 8.314.312 | 8.314.312 |
| Walmart | 1.875.607 | 1.875.607 |
| TikTok Shop | 2.609 | 2.609 |
| Shopify | 1.316 | 1.316 |
| Shopee | 514 | 514 |
| **Tổng** | **10.194.358** | **10.194.358** |

Log Validate ghi `Gold invalid=0, duplicate_order_line_key=0`; bảng không có
`label`, `delivered_at` hay `label_available_at`. Nguồn Amazon ở đây là cửa sổ
10 ngày và ba Channel mới dùng nguồn nhỏ, theo phạm vi DP1 đã ghi trong
[`airflow.md`](../pipeline_orchestration.md).

![Airflow Graph DP2 gồm Silver và Gold, 12 task success](../../images/airflow/dp2_silver-graph-gold-five-channels.png)

![Airflow Grid DP2 gồm Silver và Gold](../../images/airflow/dp2_silver-grid-gold-five-channels.png)

DP3 run `manual__2026-10-14T04:11:15+00:00` hoàn tất 2/2 task. Bảng feature
có **10.194.358** dòng, bằng Gold, phân bố theo Channel đúng bảng trên.
`validate_return_features` ghi `feature invalid=0`,
`duplicate_order_line_key=0` và `PASS`. Validate cũng chặn các cột trạng thái
và thời gian chỉ có sau lúc đặt Order để tránh rò rỉ thông tin tương lai.

![Airflow Graph DP3 feature, Build và Validate thành công](../../images/airflow/dp3_features-graph-five-channels.png)

![Airflow Grid DP3 feature](../../images/airflow/dp3_features-grid-five-channels.png)

Sau khi Gold thêm `amazon_order_version_key`, cùng run DP3 được chạy lại và
hoàn tất **2/2 task** lúc 05:02:40 UTC. Validate lần hai tiếp tục ghi
`feature invalid=0`, `duplicate_order_line_key=0`. Job feature chỉ đọc các cột
biết tại lúc đặt Order, nên khóa phiên bản trạng thái mới không đi vào feature.

![Airflow Graph DP3 sau SCD2](../../images/airflow/dp3_features-graph-scd2-five-channels.png)

![Airflow Grid DP3 sau SCD2](../../images/airflow/dp3_features-grid-scd2-five-channels.png)

## Nhãn Walmart tách riêng

`scripts/run/prepare_walmart_label_keys.py` đọc mapping ground truth theo batch,
dùng seed 42 từ manifest và hàm `native_digits` của generator để bổ sung
`customer_order_id`. File Parquet chuẩn bị có **15.562.675** dòng; bước này cần
chạy lại nếu nguồn hoặc seed thay đổi:

```bash
.venv/bin/python scripts/run/prepare_walmart_label_keys.py
```

DAG `dp3_walmart_labels`, run `manual__2026-10-14T04:15:18+00:00`, ghi bảng
`lakehouse.gold.fact_walmart_return_labels_dp3` với **1.319.345** Order Line
Walmart đã có nhãn. Log task ghi `duplicate=0`, `invalid=0`. Đây là bảng nhãn
riêng để huấn luyện và đánh giá, không được nối vào feature tại thời điểm dự
đoán. Mapping có 15,56 triệu dòng cho toàn bộ ground truth, còn Gold Walmart
trong run này chỉ có 1,88 triệu Order Line; vì thế số dòng có nhãn nhỏ hơn
mapping là phù hợp với phạm vi bảng thử, chưa phải phép đo độ phủ toàn kỳ.

![Airflow Graph bridge nhãn Walmart thành công](../../images/airflow/dp3_walmart_labels-graph-five-channels.png)

![Airflow Grid bridge nhãn Walmart](../../images/airflow/dp3_walmart_labels-grid-five-channels.png)

## Trên AWS (2026-10-28)

DP2/DP3 chạy Spark Compose EC2, bảng Iceberg ở S3; run_id nằm trong
[manifest AWS](../../evidence/aws/datahub-validation-aws-20261029.json).
`fact_order_lines_dp2` hợp năm Silver thành 227.200.303 Order Line;
`dim_amazon_order_dp2` giữ 106.109.637 bản hiện hành và 1.062.966 bản lịch sử.
`feat_order_line_return_dp3` giữ thuộc tính lúc đặt Order, cũng 227.200.303 dòng.

![Session Manager EC2: log DP2 ghi Silver validate PASS, Gold khớp từng Channel và Amazon SCD2 không lỗi](../../evidence/aws/dp2-validate-logs-20261028.png)

![Session Manager EC2: fact_rows và feature_rows đều 227.200.303, missing, extra và wrong_event_time bằng 0](../../evidence/aws/dp3-validate-log-20261028.png)

[Job fact](../../../spark_jobs/gold/build_order_snapshot_lines.py),
[job SCD2](../../../spark_jobs/gold/build_amazon_order_status_scd2.py) và
[job feature](../../../spark_jobs/gold/build_order_snapshot_return_features.py) được submit qua
[DAG DP2](../../../dags/dp2_silver.py) / [DP3](../../../dags/dp3_features.py).
Log kiểm SCD2/fact không lỗi; feature khớp tập khóa Gold, event time không sai.
Chưa có capture AWS Navigator/ERD/DESCRIBE các cột; log PASS không chứng minh toàn bộ yêu cầu DE34–38.
