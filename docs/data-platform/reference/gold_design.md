# Thiết kế tầng Gold — các quyết định đã triển khai

Tài liệu này ghi các dữ kiện và lựa chọn đã chốt trước khi xây Gold.
Gold fact, Amazon SCD2, feature và bridge nhãn đã được kiểm chứng trong
[các run](gold.md). Từ vựng nghiệp vụ giữ ở
[thuật ngữ và tầng dữ liệu](../../shared/terminology.md).

## Dữ kiện đã xác minh

- Năm bảng Silver đều lưu **một dòng cho mỗi sản phẩm trong đơn hàng**. Mỗi kênh
  bán hàng có một bảng riêng.
- Dữ liệu đơn Amazon chỉ có khoảng thời gian **giao dự kiến**; không có thời điểm giao thật
  (`generator/channels/amazon.py`, `spark_jobs/silver/build_amazon_order_snapshots.py`).
- Bộ sinh dữ liệu ghi `"Delivered"` cho cả trạng thái **đã giao** và **đã hoàn tất**
  của Walmart (`generator/channels/walmart.py`). Vì vậy `status_changed_at` có
  thể là lúc hoàn tất, không thể coi chắc chắn là `delivered_at` (thời điểm giao).
- Dữ liệu đáp án về việc trả hàng (`label`), thời điểm giao (`delivered_at`) và
  thời điểm có đáp án (`label_available_at`) được giữ riêng trong
  `ground-truth/labels`. Bảng `ground-truth/order_line_keys` nối mã nội bộ với mã
  từng kênh (`generator/generate_order_snapshots.py`). Đáp án chỉ có sau thời hạn 30 ngày.
- Mã `purchase_order_id` của Walmart từng trùng giữa hai đơn khác nhau. Muốn
  xác định duy nhất một dòng sản phẩm phải dùng đồng thời
  `purchase_order_id`, `customer_order_id`, `line_number` (`docs/data-platform/reference/silver.md` mục 3.1).

## ✅ Q1 — Bảng Gold đầu tiên chi tiết đến mức nào? (đã chốt)

| Lựa chọn | Được gì | Đánh đổi |
|---|---|---|
| **A. Gộp năm kênh vào `fact_order_lines`, mỗi dòng là một sản phẩm trong đơn** | Dùng trực tiếp để tính doanh thu, làm bảng đặc trưng cho dự đoán, và nối với các bảng mô tả | Phải chuẩn hóa mã định danh và các cột khác nhau giữa các kênh; bảng lớn hơn |
| **B. Làm bảng tổng hợp doanh thu theo ngày trước** | Bảng nhỏ, dễ trình bày báo cáo và đo tốc độ truy vấn | Vẫn phải làm bảng chi tiết sau đó mới hoàn thành phần dự đoán và sơ đồ quan hệ bảng |

**Người dùng chọn A:** tạo `fact_order_lines`, một dòng cho mỗi sản phẩm trong
đơn của cả năm Channel. Bảng này chỉ chứa những thông tin quan sát được từ đơn
hàng. Không đưa đáp án trả hàng vào bảng và không lấy ngày giao dự kiến làm
ngày giao thực tế. Sau khi bảng chi tiết ổn định, có thể tính bảng doanh thu
theo ngày từ đó. Mỗi số tiền giữ nguyên đơn vị tiền tệ của nguồn; không cộng
chéo USD/VND nếu chưa có bảng tỷ giá.

## Hệ quả đã xác minh từ Q1

- Không Channel nào trong năm bảng Silver hiện có cột thời điểm giao thật đáng
  tin cậy cho **mọi** Order Line. TikTok có trạng thái `DELIVERED`, nhưng
  `update_time` của snapshot có thể là lần đổi trạng thái sau đó; Walmart gộp
  trạng thái giao và hoàn tất thành `Delivered`. `delivered_at` chỉ nằm trong
  ground truth. Vì vậy `fact_order_lines` không có cột tên `delivered_at`.
  Thời gian giao dự kiến của Amazon/Walmart có thể giữ dưới tên
  `estimated_delivery_start` và `estimated_delivery_end`, không đổi tên thành giao thật.
- `fact_order_lines` dùng khóa ổn định từ Channel cùng bộ ID gốc; riêng Walmart
  phải chứa cả `purchase_order_id`, `customer_order_id`, `line_number` trước
  khi tạo khóa Gold. Điều này giữ hai đơn trùng `purchase_order_id` tách biệt.
- Ground truth chỉ dùng ở DP3 khi tạo tập dữ liệu có nhãn; không đi vào fact.
  `label_available_at` phải nhỏ hơn hoặc bằng mốc chốt dữ liệu của lần chạy.

## ✅ Q2 — DP3 dự đoán vào thời điểm nào? (đã chốt)

| Lựa chọn | Trường được dùng làm đặc trưng | Đánh đổi |
|---|---|---|
| **A. Ngay khi Order được đặt** | Giá, số lượng, SKU, Channel, nơi nhận, dấu thời gian đặt hàng và lịch sử đã biết trước đó | Không dùng trạng thái giao hoặc thời điểm cập nhật muộn; sát việc ra quyết định sớm |
| **B. Khi Order vừa giao** | Thêm các tín hiệu vận chuyển/giao hàng nếu Channel có | Năm Channel không cung cấp `delivered_at` đủ tin cậy; phải dùng mốc ground truth hoặc chấp nhận độ phủ không đều |

**Người dùng chọn A:** dự đoán ngay lúc Order được đặt. Bảng Gold có thể lưu
snapshot mới nhất để báo cáo, nhưng job feature DP3 chỉ chọn các cột thật sự
biết được khi đặt hàng. `order_status` cuối cùng và `last_updated_time`
không được dùng làm đặc trưng.

## ✅ Q3 — Nối nhãn Walmart khi mã đơn bị trùng thế nào? (đã chốt)

Mapping `ground-truth/order_line_keys/walmart` chỉ có
`channel_order_id` và `channel_line_id`; payload Walmart còn có
`customer_order_id`. Đã xác nhận mã `1678172477013` nối với hai Order nội bộ
khác nhau (`ORD-011-539381-1` và `ORD-042-53671-1`) tại cùng line 1. Join
theo cặp ID trong mapping sẽ gán nhãn sai hoặc nhân đôi dòng.

| Lựa chọn | Cách làm | Đánh đổi |
|---|---|---|
| **A. Bổ sung `customer_order_id` vào bảng khóa nhãn bằng hàm sinh mã xác định và seed của lần sinh nguồn** | Nối chính xác cả ba khóa, giữ các dòng có va chạm | Phụ thuộc quy tắc mã của generator; phải ghi rõ đây là bridge của dữ liệu mô phỏng |
| **B. Loại các khóa Walmart mơ hồ khỏi tập có nhãn** | Đơn giản, không phụ thuộc thuật toán sinh mã | Mất một số Order Line trong tập train/evaluation và phải thống kê số dòng bị loại |

**Người dùng chọn A.** `order_line_id` trong mapping có dạng mã Order nội bộ
kèm `-line_number`, nên có thể khôi phục mã Order, tính lại `customer_order_id`
theo `native_digits(seed, 'walmart_co', order_id, 13)` đã dùng trong renderer,
rồi đối chiếu với payload nguồn. Job phải dừng nếu một khóa vẫn nối nhiều dòng.

`scripts/run/prepare_walmart_label_keys.py` tạo Parquet bốn cột khóa từ mapping
ground truth, dùng seed trong `/data/raw/run_manifest.json` và đúng hàm
`native_digits` của generator. Bước này chạy trên host vì Airflow driver dùng
Python 3.11 còn Spark worker dùng Python 3.10; UDF Python trong Spark bị lỗi
`PYTHON_VERSION_MISMATCH`. Mã `spark_jobs/gold/build_walmart_order_snapshot_labels.py` đọc
Parquet đã chuẩn bị, nối đủ ba khóa gốc với Gold fact, rồi mới lấy nhãn từ
ground truth. Bảng nhãn tách khỏi Gold fact và bảng feature để không rò rỉ đáp
án vào thời điểm đặt Order. Bridge đã chạy thành công với 1.319.345 dòng,
không có khóa trùng hoặc dòng lỗi; xem [bằng chứng runtime](gold.md).

## ✅ Q4 — Nguồn lịch sử cho bảng chiều SCD2

Bảng dimension SCD2 sử dụng `valid_from_ts`, `valid_to_ts`, `is_current`
và quan hệ với fact. Nguồn `seller.skus` hiện là catalog tĩnh; giá trên Order
Line là giá giao dịch, không thể coi là lịch sử giá Listing. Bronze Amazon có
các bản export chồng lấn của cùng Order, Silver giữ bản mới nhất, nên Bronze
có thể cung cấp thay đổi Order Status thực tế của dữ liệu mô phỏng.

| Lựa chọn | Cách làm | Đánh đổi |
|---|---|---|
| **A. `dim_amazon_order` với lịch sử Order Status từ Bronze Amazon** | Tạo một phiên bản khi trạng thái quan sát được đổi qua các Export Date; nối với fact Amazon bằng mã Order gốc | Chỉ chứng minh SCD2 cho Amazon; Mốc hiệu lực đã chốt theo Q5; giữ riêng ngày export đầu tiên quan sát phiên bản |
| **B. `dim_sku` từ catalog Seller tĩnh** | Quan hệ SKU xuyên cả năm Channel rõ ràng | Không có thay đổi thuộc tính thật trong nguồn hiện tại; phải bổ sung lịch sử nguồn mới trước khi chứng minh SCD2 |

**Đề xuất A:** có lịch sử nguồn đã được giao cho hệ thống và tránh bịa phiên
bản SKU. **Người dùng chọn A.**

## ✅ Q5 — Mốc bắt đầu phiên bản SCD2

Sau Q4, kiểm tra `generator/channels/amazon.py` thấy mỗi snapshot Amazon ghi
`LastUpdateDate` hoặc `lastUpdatedTime` từ `change.at` của trạng thái đang thấy.
Vì vậy có hai mốc nguồn thật để đặt `valid_from_ts`:

| Lựa chọn | Nghĩa của `valid_from_ts` | Đánh đổi |
|---|---|---|
| **A. Thời điểm trạng thái đổi (`LastUpdateDate` / `lastUpdatedTime`)** | Thời điểm nghiệp vụ theo snapshot Amazon; phù hợp truy vấn trạng thái tại thời điểm Order Line | Chỉ những trạng thái có mặt trong các export đã nạp mới thành phiên bản; các bước trung gian không từng thấy vẫn thiếu |
| **B. Ngày export quan sát trạng thái (`export_date`)** | Lúc hệ thống biết phiên bản đó | Mất độ chính xác theo giờ; ngày export có thể sau nhiều ngày so với lúc đổi trạng thái |

**Đề xuất A:** nguồn mô phỏng ghi thời điểm đổi trạng thái trực tiếp. Giữ thêm
`first_observed_export_date` để phân biệt thời điểm nghiệp vụ và thời điểm quan
sát. **Người dùng chọn A.** Các trạng thái không từng xuất hiện
trong Bronze vẫn không thể phục hồi; không tự lấp bằng lịch sử của generator.

## ✅ Q6 — Gắn phiên bản chiều với Gold fact

Fact Gold Amazon hiện giữ `channel_order_id` và `last_updated_time` của snapshot
Silver mới nhất. Bảng SCD2 sẽ có nhiều phiên bản cho một Order, nên chỉ nối
theo mã Order sẽ nhân dòng. Hai lựa chọn đều dùng đúng thời điểm phiên bản đã
chốt ở Q5:

| Lựa chọn | Cách nối | Đánh đổi |
|---|---|---|
| **A. Thêm `amazon_order_version_key` vào fact và dim** | Khóa ổn định từ mã Order và thời điểm đổi trạng thái; fact Amazon trỏ đúng một phiên bản, Channel khác để null | Phải ghi lại Gold fact và chạy lại Validate/DP3 để có schema mới |
| **B. Giữ fact hiện có, nối bằng mã Order + thời gian trong câu SQL** | Không cần ghi lại fact | Mỗi người viết query phải nhớ điều kiện thời gian; không có khóa phiên bản hiện rõ trong schema |

**Đề xuất A:** quan hệ dim/fact có thể kiểm khóa và thể hiện rõ trong tài liệu
schema. **Người dùng chọn A.**

## SCD2 đã triển khai và kiểm chứng

- `lakehouse.gold.dim_amazon_order_dp2` có một dòng cho mỗi **phiên bản trạng thái
  của một Order Amazon đã xuất hiện trong Bronze**. Nguồn là
  `raw_amazon_orders_dp1`; parse cả schema v0 và 2026 như Silver, ở mức Order
  để không nhân theo Order Line.
- Khóa phiên bản ổn định từ `(order_id, last_updated_time)`; giữ
  `order_status`, `channel_status`, `first_observed_export_date`,
  `valid_from_ts`, `valid_to_ts`, `is_current`. `valid_from_ts` là thời điểm
  trạng thái đổi trong payload. `valid_to_ts` là `valid_from_ts` của phiên bản
  tiếp theo (biên **không bao gồm**); phiên bản cuối có `valid_to_ts = NULL`
  và `is_current = true`.
- Cùng Order và cùng thời điểm cập nhật có nhiều export thì gộp thành một
  phiên bản. Hai schema Amazon có thể viết cùng trạng thái chuẩn khác nhau
  (`Shipped` và `SHIPPED`); `channel_status` giữ chữ nguồn ở export mới nhất.
  Nếu trạng thái **chuẩn** mâu thuẫn, job dừng thay vì chọn bừa.
  Trạng thái đổi giữa hai lần export nhưng không lần nào được quan sát sẽ
  không xuất hiện; không lấy lịch sử ẩn của generator để lấp.
- Fact Gold Amazon thêm `amazon_order_version_key` từ bộ khóa trên; bốn Channel
  còn lại để null. Validate yêu cầu mọi Order Line Amazon nối **đúng một**
  phiên bản trong dim. Bảng feature DP3 không đưa trạng thái hay khóa chiều
  này vào đặc trưng, vì dự đoán ở thời điểm Order được đặt.
- Sau khi chạy lại Gold/Validate và DP3/Validate, cập nhật ảnh và số liệu runtime
  thay cho bằng chứng cũ nếu đầu ra thay đổi. Bảng nhãn Walmart ở riêng và
  không bị đổi schema.

## Trên AWS

DP2/DP3 Spark Compose EC2 đã ghi Gold trên S3; [Gold AWS](gold.md#trên-aws)
nhúng log validator và dẫn code fact, SCD2, feature.
[Manifest AWS](../../evidence/aws/datahub-validation-aws-20261029.json) ghi SCD2 không thiếu phiên bản,
fact không trùng khóa, feature không sai event time trong các run được ghi.

Quan hệ logic là `fact.amazon_order_version_key → dim.order_version_key`, feature nối fact bằng `order_line_key`.
SCD2 dùng `valid_from_ts`, `valid_to_ts`, `is_current`; feature tách `event_timestamp` và `created` theo thiết kế trên.
Chưa có capture metadata/ERD AWS để xác nhận trực quan các cột và quan hệ; ảnh schema local vẫn chỉ là đối chứng.
