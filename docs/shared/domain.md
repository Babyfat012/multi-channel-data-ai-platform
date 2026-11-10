# Tổng quan business domain

Tài liệu giải thích chi tiết business domain của dự án: các thuật ngữ, bài toán dự đoán và những vấn đề dữ liệu mà generator phải mô phỏng. README chỉ tóm tắt phần này.

## Mục lục

1. [Bức tranh tổng thể](#1-bức-tranh-tổng-thể)
2. [Các danh từ chính](#2-các-danh-từ-chính)
3. [Sản phẩm](#3-sản-phẩm)
4. [Đơn hàng](#4-đơn-hàng)
5. [Bài toán dự đoán](#5-bài-toán-dự-đoán)
6. [Dữ liệu chảy vào thế nào](#6-dữ-liệu-chảy-vào-thế-nào)
7. [Quy tắc về tiền và thời gian](#7-quy-tắc-về-tiền-và-thời-gian)
8. [Các bài toán dữ liệu generator phải sinh ra](#8-các-bài-toán-dữ-liệu-generator-phải-sinh-ra)
9. [Phần mở rộng riêng của Amazon](#9-phần-mở-rộng-riêng-của-amazon)
10. [Giả định và giới hạn](#10-giả-định-và-giới-hạn)

## 1. Bức tranh tổng thể

Dự án không xây một sàn như Amazon. Dự án xây **hệ thống dữ liệu của một người bán hàng** bán trên nhiều sàn cùng lúc. Người bán này cần gom dữ liệu từ 5 sàn về một chỗ để phân tích và dự đoán.

Ví dụ xuyên suốt: công ty **ACME** bán áo hoodie trên Amazon, TikTok Shop, Shopify, Walmart và Shopee. Mỗi sàn trả dữ liệu theo một kiểu khác nhau, và hệ thống gom hết lại.

Toàn bộ dữ liệu là **giả lập (synthetic)**. Amazon là mô hình tham chiếu sâu nhất, bốn Channel còn lại mô hình hóa mỏng hơn.

## 2. Các danh từ chính

| Thuật ngữ | Nghĩa | Ví dụ với ACME |
|---|---|---|
| **Seller** | Người bán duy nhất mà hệ thống phục vụ | ACME |
| **Channel** | Nền tảng bên ngoài | Amazon, TikTok Shop, Shopify, Walmart, Shopee |
| **Marketplace** | Gian hàng theo quốc gia trong một Channel (chỉ Channel nào bán theo quốc gia mới có) | Amazon DE |
| **Storefront** | "Chỗ cụ thể ACME bán hàng" = Channel + Marketplace nếu có | Amazon DE, TikTok Shop, store Shopify của ACME |

Có 7 Storefront: Amazon US/UK/DE, TikTok Shop US, Shopify (một store), Walmart US, Shopee VN.

Marketplace chỉ xuất hiện ở Channel bán theo quốc gia. TikTok Shop không có, nên cần từ **Storefront** để gọi chung mọi trường hợp.

## 3. Sản phẩm

- **SKU** là mã do ACME tự đặt, ví dụ `HOODIE-BLK-M`. Đây là mã chung xuyên mọi sàn.
- **Listing** là bản đăng bán của SKU đó trên một Storefront. Amazon US có ASIN riêng, Shopify có variant ID riêng, giá cũng khác nhau.

Một SKU có nhiều Listing. Nhờ SKU, ta nhận ra "cùng một cái áo đang bán ở 5 nơi". Giá và trạng thái của Listing thay đổi theo thời gian, đây là chỗ đặt dim SCD2.

## 4. Đơn hàng

- **Order** là một đơn mà sàn giao cho ACME xử lý.
- **Order Line** là một dòng sản phẩm trong đơn, ví dụ "2 cái hoodie đen size M".
- **Order Status** là bộ trạng thái chuẩn: `pending_payment` → `awaiting_shipment` → `shipped` → `delivered` → `completed`, cùng nhánh phụ `cancelled` và `returned`.

Mỗi sàn dùng tên trạng thái khác nhau (TikTok Shop có `AWAITING_COLLECTION`, Shopify tách trạng thái thanh toán và giao hàng). Ta giữ lại trạng thái gốc ở Silver và có một bảng ánh xạ sang trạng thái chuẩn.

Hai thuật ngữ liên quan: **Return** là buyer gửi trả một Order Line sau khi đã giao, **Refund** là tiền được hoàn cho buyer (có hoặc không kèm Return).

## 5. Bài toán dự đoán

Câu hỏi ML: *"Order Line này có bị Return hoặc Refund trong 30 ngày sau khi giao không?"*

| Tình huống | Cách xử lý |
|---|---|
| Khi nào dự đoán? | Ngay lúc đơn được tạo |
| Bị hủy trước khi giao | Không tính, vì chưa từng giao |
| Mua 2 cái, trả 1 cái | Tính là có (nhãn dương) |
| Vừa giao 10 ngày trước | Chưa biết nhãn, loại khỏi tập train |

**Vì sao không dự đoán "khách có mua lại không"?** Amazon và TikTok Shop giấu danh tính người mua, Shopify có thể null với khách checkout không đăng nhập, chỉ Walmart trả đủ. Nên không có `customer_id` dùng chung được giữa 5 Channel.

Vì vậy Online Feature Store tra cứu theo **`sku_id` + `storefront_id`**, ví dụ "tỷ lệ trả hàng 90 ngày của hoodie đen ở Amazon DE".

Nhãn đến sau 30 ngày. Đây chính là tình huống "chưa có đáp án thật mà vẫn phải giám sát drift".

## 6. Dữ liệu chảy vào thế nào

- **Batch**: lịch sử 180 ngày của cả 5 Channel, mỗi Channel một định dạng riêng, đặt trong MinIO/PostgreSQL để kéo về Bronze.
- **Stream**: event đổi trạng thái đơn và trả hàng, đi qua Kafka và Flink. Mỗi sàn "hỏng" theo kiểu riêng:
  - TikTok Shop và Shopify (webhook): hay gửi trùng.
  - Amazon (notification): hay đến trễ.
  - Walmart và Shopee (giả định polling): đến theo lô.

Bên seller **không có clickstream** (lượt xem, giỏ hàng), vì đó là dữ liệu nội bộ của sàn.

## 7. Quy tắc về tiền và thời gian

- Số tiền gốc giữ nguyên kèm mã tiền tệ gốc, thêm một cột quy ra **USD** theo tỷ giá từng ngày.
- Shopify có hai loại tiền tệ trên một đơn (tiền của store và tiền khách trả), giữ cả hai.
- Thời gian lưu **UTC**, tách `event_time` (lúc sự kiện xảy ra) và `ingested_at` (lúc hệ thống nhận). Khoảng chênh giữa hai mốc này là late arrival.

## 8. Các bài toán dữ liệu generator phải sinh ra

| Vấn đề | Cụ thể |
|---|---|
| **Skew** | Order theo Storefront: Amazon US 45%, TikTok Shop US 15%, Amazon UK 12%, Amazon DE 10%, Shopify 8%, Walmart US 7%, Shopee VN 3%. Vài SKU chiếm phần lớn doanh số (Zipf). 8 danh mục với tỷ lệ trả hàng khác nhau, quần áo khoảng 22%, đồ gia dụng khoảng 4% |
| **High cardinality** | Giữ 2.000 SKU. Cardinality cao nằm ở mã Order (khoảng 720 nghìn), mã Order Line (hơn 1 triệu), mã vận đơn và mã buyer theo Channel |
| **Schema evolution** | Từ một ngày cố định, dữ liệu đơn Amazon chuyển từ model Orders v0 sang Orders 2026-01-01: đổi tên field, đổi cấu trúc, thêm field mới nên partition cũ bị null |
| **Duplicate offline** | Khoảng 2% dòng bị trùng do export chồng lấn ngày, gồm bản trùng y hệt và bản sửa lại mới hơn |
| **Data drift** | (1) Covariate: đợt sale kiểu Prime Day làm mức giảm giá và phân phối SKU đổi đột ngột trên các Storefront Amazon. (2) Concept: livestream TikTok làm tỷ lệ trả quần áo tăng gần gấp đôi nhưng feature trông vẫn bình thường |
| **Duplicate, burst, late (stream)** | Tùy đặc tính từng Channel như mục 6 |

Mọi con số đều là config của generator, có thể tăng khi chạy trên cloud. Thiết kế chi tiết xem [generator-design.md](../data-platform/reference/generator_design.md).

## 9. Phần mở rộng riêng của Amazon

Amazon được làm sâu hơn các Channel khác:

- **Fulfillment Method** (ai giao hàng: Seller hay Channel, như FBA/FBM), áp dụng cho cả 5 Channel vì nó ảnh hưởng mạnh tới tỷ lệ trả hàng.
- **Mã lý do Return** riêng của Amazon.

Phí, settlement, quảng cáo, tồn kho FBA chưa làm ở giai đoạn này, có thể mở rộng sau.

## 10. Giả định và giới hạn

- **Mức xác minh của dạng dữ liệu từng Channel**: đơn hàng và trả hàng của cả 5 Channel đã đối chiếu với tài liệu chính thức. Một số chi tiết vẫn là *giả định* (ví dụ ngày giao thực tế của Amazon và Shopee, notification của Amazon). Danh sách đầy đủ, kèm nguồn và mức tin cậy, xem [channel-sources.md](../data-platform/reference/channel_sources.md).
- **Dữ liệu hoàn toàn giả lập**: phân phối (tỷ trọng Storefront, tỷ lệ trả hàng theo danh mục) là tham số của generator, không phải số liệu thật.
- **Không có clickstream**: dữ liệu lượt xem, giỏ hàng là dữ liệu nội bộ của sàn, nằm ngoài phạm vi góc nhìn Seller.
