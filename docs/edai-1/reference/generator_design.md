# Thiết kế Data Generator

Tài liệu thiết kế và triển khai generator dữ liệu giả lập cho dự án. Trạng thái: **đã triển khai trong phạm vi mô tả ở mục 15**. Thuật ngữ (Channel, Storefront, Order Line, ...) xem [Domain.md](../../shared/domain.md). Nguồn và mức tin cậy của dạng dữ liệu từng Channel xem [channel-sources.md](channel_sources.md).

Cập nhật resume 2026-10-18: trước mọi ghi/xóa, Batch so
`_dataset_identity.json` với config (không gồm output root), thứ tự Storefront,
SHA-256 mã generator và Python/NumPy/PyArrow/fsspec versions. Checkpoints và
manifest cùng mang `dataset_version`. Đổi config/code/runtime hoặc root cũ
chưa có identity sẽ fail; không tự gắn identity mới lên dữ liệu cũ.
Dùng root mới khi cần world khác. `--restart` giữ nghĩa tái sinh dữ liệu
generator sở hữu trong root đã chọn, vì thế chỉ dùng khi chủ động muốn làm lại.
Không chạy restart trên bộ evidence cần giữ. Guard giả định một writer;
chưa là khóa phân tán hoặc phép kiểm hash toàn bộ file đã sinh.
Bản xử lý ghi test và giới hạn.
[Source contract v2](source_contract_order_lifecycle.md) là đích tiếp theo, chưa được
thực thi chỉ nhờ có `dataset_version`; logic label hiện tại vẫn là v1.

## Mục lục

1. [Mục tiêu và phạm vi](#1-mục-tiêu-và-phạm-vi)
2. [Kiến trúc ba lớp](#2-kiến-trúc-ba-lớp)
3. [Đường thời gian: Batch và Stream](#3-đường-thời-gian-batch-và-stream)
4. [Sinh nhãn Return/Refund](#4-sinh-nhãn-returnrefund)
5. [Vấn đề dữ liệu được chèn vào](#5-vấn-đề-dữ-liệu-được-chèn-vào)
6. [Nguồn dữ liệu từng Channel](#6-nguồn-dữ-liệu-từng-channel)
7. [Dữ liệu tham chiếu](#7-dữ-liệu-tham-chiếu)
8. [Bố cục lưu trữ](#8-bố-cục-lưu-trữ)
9. [Buyer, tiền tệ và thời gian](#9-buyer-tiền-tệ-và-thời-gian)
10. [Cấu hình](#10-cấu-hình)
11. [Bản đáp án và bằng chứng chất lượng](#11-bản-đáp-án-và-bằng-chứng-chất-lượng)
12. [Kỹ thuật và đóng gói](#12-kỹ-thuật-và-đóng-gói)
13. [Giả định và việc còn mở](#13-giả-định-và-việc-còn-mở)
14. [Thiết kế và evidence](#14-thiết-kế-và-evidence)
15. [Triển khai](#15-triển-khai)

## 1. Mục tiêu và phạm vi

Generator sinh dữ liệu bán hàng của một Seller (ACME) trên 7 Storefront thuộc 5 Channel, để các thành phần phía sau (Bronze, Silver, Gold, Feature Store, giám sát drift) có dữ liệu thật sự có vấn đề để xử lý.

Mặc định: 2.000 SKU, 180 ngày lịch sử, khoảng 4.000 Order mỗi ngày (khoảng 720 nghìn Order, hơn 1 triệu Order Line). Mọi con số là cấu hình, xem [mục 10](#10-cấu-hình).

Ngoài phạm vi giai đoạn này: phí, settlement, quảng cáo, tồn kho FBA, clickstream.

## 2. Kiến trúc ba lớp

```
Lõi mô phỏng ──► Renderer theo Channel ──► Lớp giao hàng lỗi ──► MinIO / PostgreSQL / Kafka
 (sự thật)         (dạng gốc từng sàn)       (trùng, trễ, lô)
```

1. **Lõi mô phỏng** sinh sự thật chung, không mang hình dạng của Channel nào: SKU, Listing, Order, Order Line, Return, Refund, buyer, `Delivered At`, theo Order Status chuẩn.
2. **Renderer theo Channel** biến sự thật thành dạng gốc của Channel: tên field, tên trạng thái, định dạng file, chỉ thể hiện những gì Channel thật sự cho Seller thấy.
3. **Lớp giao hàng lỗi** chèn lỗi sau cùng: duplicate, late, theo lô, burst.

Vì sao chia lớp:
- Có đáp án chung để đối chiếu kết quả Silver (số Order chuẩn, nhãn thật).
- Quy luật sinh nhãn nằm ở một chỗ, không lặp cho 5 Channel.
- Lỗi chèn sau cùng nên biết chính xác đã chèn bao nhiêu, ghi vào `run_manifest.json`.

Đổi lại phải viết thêm một lớp renderer so với cách sinh thẳng từng Channel.

## 3. Đường thời gian: Batch và Stream

`cutoff` là mốc "hôm nay" trong thế giới giả lập. Lõi mô phỏng chạy **liền một mạch** qua `cutoff`:

```
ngày 1 ───────────── ngày 180 (cutoff) ───────────── ngày 181 → 210
       BATCH                                STREAM
```

- **Batch**: mọi thứ xảy ra trước `cutoff`, mỗi Channel ở dạng gốc, kèm ảnh chụp trạng thái của các Order đang mở tại `cutoff`.
- **Stream**: phần mô phỏng tiếp tục sau `cutoff`, gồm Order mới, đổi Order Status của Order đã có trong Batch, và Return đến muộn.
- Một Order thấy ở Batch được cập nhật tiếp ở Stream vì cùng một mô phỏng.
- Nhãn của Order giao trong 30 ngày cuối trước `cutoff` chưa biết tại `cutoff`, nên bị loại khỏi tập train. Nhãn đến dần trong Stream.
- Ngày mô phỏng ánh xạ sang lịch thật qua `start_date` trong cấu hình.
- Tốc độ phát Stream cấu hình được theo từng đoạn ngày (xem [mục 5](#streaming)), có chế độ phát nhanh để test.

Vì sao không phát lại chính dữ liệu Batch: không có Order mới, không có nhãn đến muộn, không có drift sau khi mô hình đã train.

## 4. Sinh nhãn Return/Refund

Nhãn: Order Line có bị Return hoặc Refund trong 30 ngày kể từ ngày giao không.

Dữ liệu giả không có hành vi thật, nên lõi dùng một quy luật xác suất có cấu trúc để mô hình học được thay vì đoán mò:

- Xác suất trả là hàm logistic của: danh mục, Storefront, Fulfillment Method, mức giảm giá, giá, số lượng, và **SKU Defect Propensity**.
- **SKU Defect Propensity** là "độ lỗi" ẩn riêng từng SKU (ví dụ `HOODIE-BLK-M` bị chật nên trả nhiều ở mọi Storefront). Nó không xuất hiện thành cột nào, chỉ suy ra được từ lịch sử. Đây chính là thứ Feature Store tra theo `sku_id` + `storefront_id`.
- Độ trễ từ lúc giao tới lúc trả rút từ phân phối riêng theo Channel, tối đa khoảng 30 ngày, có đuôi hiếm vượt 30 ngày.
- **Không leakage**: không cột nào ở thời điểm tạo Order lộ nhãn.
- Tỷ lệ mục tiêu: mức cơ sở khoảng 10 đến 12%, quần áo khoảng 22%, đồ gia dụng khoảng 4%.
- Lý do trả: mỗi Channel dùng lý do gốc của nó (TikTok Shop `return_reason`, Shopee `reason`, Walmart lý do trong `returnOrderLines`). Riêng Amazon dùng khoảng 10 mã (`WRONG_SIZE`, `DEFECTIVE`, `NOT_AS_DESCRIBED`, `CHANGED_MIND`, `ARRIVED_LATE`, `DAMAGED_IN_TRANSIT`, `WRONG_ITEM_SENT`, `BETTER_PRICE_FOUND`, `QUALITY_NOT_EXPECTED`, `ACCIDENTAL_ORDER`). Danh sách Amazon là **giả định** vì chưa có danh sách chính thức.

Quy luật này là giả định của generator. Mô hình tốt trên dữ liệu giả không chứng minh tốt ngoài đời.

### Ngày giao (`Delivered At`)

Nhãn tính theo `Delivered At` **thật** của lõi. Mỗi Channel chỉ để lộ phần nó thật sự cho Seller thấy:

| Channel | Seller thấy gì |
|---|---|
| TikTok Shop, Walmart | Trạng thái `DELIVERED` kèm thời điểm cập nhật |
| Shopify | Sự kiện fulfillment |
| Amazon MFN | Chỉ ngày giao dự kiến: `EarliestDeliveryDate`/`LatestDeliveryDate` trong Orders |
| Amazon FBA | Chỉ ngày giao dự kiến: `estimated-arrival-date` trong report FBA Shipments |
| Shopee | Chỉ `update_time` khi sang `COMPLETED` |

Pipeline và feature chỉ thấy ngày Channel cho, còn nhãn dùng ngày thật để có đáp án đúng. Amazon và Shopee là giả định, xem [channel-sources.md](channel_sources.md).

## 5. Vấn đề dữ liệu được chèn vào

### Offline (Batch)

| Vấn đề | Cách chèn |
|---|---|
| **Skew** | Order theo Storefront: Amazon US 45%, TikTok Shop US 15%, Amazon UK 12%, Amazon DE 10%, Shopify 8%, Walmart US 7%, Shopee VN 3%. SKU theo phân phối Zipf, tham số chọn sao cho SKU bán chạy nhất chiếm khoảng 5% Order Line để job Spark có task chạy chậm rõ rệt. 8 danh mục có tỷ lệ trả khác nhau. Địa lý lệch: vài thành phố lớn chiếm phần lớn Order của mỗi Storefront. Prime Day làm một số ngày có lượng Order gấp nhiều lần |
| **High cardinality** | Giữ 2.000 SKU. Cardinality cao đến từ `order_id` (khoảng 720 nghìn), `order_line_id` (hơn 1 triệu), `tracking_number` (khoảng 700 nghìn), mã buyer theo Channel (xem [mục 9](#9-buyer-tiền-tệ-và-thời-gian)) |
| **Schema evolution** | Từ một ngày cố định, dữ liệu đơn Amazon chuyển từ model Orders v0 sang Orders 2026-01-01. Cả hai là model chính thức. Dạng 2026 không có field mới — mọi khoá truy được về một khoá v0: đổi tên (`AmazonOrderId` thành `orderId`, `PurchaseDate` thành `createdTime`, `OrderTotal` thành `proceeds.grandTotal` lồng thêm một cấp), đổi cấu trúc (`OrderStatus` thành `fulfillment.status`, giá trị `Shipped` thành `SHIPPED`), đổi kiểu (`IsPrime` bool thành mảng `programs`; thông tin không mất vì mảng rỗng khi không Prime), và bỏ khoá (5 khoá chỉ có ở v0). Vì tên khoá đổi, cột chỉ có ở một dạng null ở dạng kia: `proceeds` null ở partition v0, `is_prime` null ở partition 2026, `programs` null ở partition v0 |
| **Duplicate 2%** | Export chồng lấn ngày (ví dụ lấy lại 3 ngày gần nhất để bắt cập nhật, như report `GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL` của Amazon). Có bản trùng y hệt và **bản sửa lại** (cùng khóa, thời điểm cập nhật mới hơn, trạng thái mới hơn). Dedup phải giữ bản mới nhất, không chỉ bỏ bản giống hệt |

Cách đo tỷ lệ trùng: `1 - count(distinct khóa) / count(*)`. Khóa theo Channel: Amazon `AmazonOrderId` (từ khi đổi schema là `orderId`), TikTok Shop `id`, Shopify `id`, Walmart `purchaseOrderId`, Shopee `order_sn`. Cột thời điểm cập nhật dùng để chọn bản mới nhất: Amazon `LastUpdateDate`/`lastUpdatedTime`, TikTok Shop và Shopee `update_time`, Shopify `updated_at`, Walmart `fetched_at`.

### Streaming

| Vấn đề | Channel | Mức mặc định |
|---|---|---|
| **Duplicate** | TikTok Shop, Shopify (webhook) | 1.5%. Bản trùng giữ **cùng** mã thông báo (TikTok Shop `tts_notification_id`) để Flink dedup được |
| **Late** | Amazon (notification) | Trễ trung vị vài phút, 5% trễ hơn 1 giờ. Message trễ được gửi sau cả những message có `event_time` muộn hơn, tức là đến lệch thứ tự |
| **Theo lô** | Walmart, Shopee (polling, là lựa chọn thiết kế của dự án) | Gộp theo chu kỳ 5 đến 15 phút |
| **Burst** | Livestream TikTok (sau `cutoff`) | 5 đến 8 lần tốc độ thường trong vài giờ |

Prime Day cũng gây burst nhưng nằm trong Batch, nên chỉ tính cho skew theo ngày, không tính cho burst của stream.

**Tốc độ phát theo đoạn.** Burst phải kéo dài đủ lâu theo đồng hồ thật để thấy trên Flink UI (backpressure, throughput). Nếu phát đều 60 giây thật cho mỗi ngày mô phỏng thì buổi livestream 6 giờ chỉ kéo dài khoảng 15 giây. Vì vậy Stream được phát theo đoạn:
- Ngày bình thường: 60 giây thật cho mỗi ngày mô phỏng.
- Ngày có livestream: 600 giây thật cho mỗi ngày mô phỏng. Buổi livestream 6 giờ kéo dài khoảng 150 giây thật, với tốc độ message gấp 5 đến 8 lần bình thường.
- Một lần phát đủ 30 ngày Stream mất khoảng 40 phút.

Late arrival không bị ảnh hưởng bởi tốc độ phát, vì Flink đo độ trễ theo `event_time` trong payload.

### Drift

| Loại | Sự kiện | Nằm ở đâu |
|---|---|---|
| **Covariate** | Prime Day: giảm giá sâu và phân phối SKU đổi ở Amazon | Trong Batch, khoảng ngày 120. Tập train đã từng thấy một đợt sale |
| **Concept** | Livestream TikTok: tỷ lệ trả quần áo gần gấp đôi, feature không đổi | Chỉ trong Stream, sau `cutoff`. Mô hình chưa từng gặp, phải phát hiện được trước khi nhãn về sau 30 ngày |

Drift là thay đổi tham số của lõi theo lịch cấu hình, không phải sửa dữ liệu sau khi sinh.

## 6. Nguồn dữ liệu từng Channel

Mỗi nguồn khớp tài liệu chính thức. Field mức "Trung bình" hoặc "Thấp" ghi là giả định, xem [channel-sources.md](channel_sources.md).

| Channel | Nguồn Batch | Nơi lưu | Stream |
|---|---|---|---|
| Amazon | (1) Đơn hàng JSON, theo Orders v0 rồi Orders 2026-01-01. (2) Returns report TSV `GET_FLAT_FILE_RETURNS_DATA_BY_RETURN_DATE` cho đơn MFN. (3) Report FBA Shipments `GET_AMAZON_FULFILLED_SHIPMENTS_DATA_GENERAL` (TSV). (4) Report FBA Returns `GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA` (TSV) | MinIO | Notification, hay đến trễ |
| TikTok Shop | (1) Đơn hàng JSON theo Get Order Detail 202309. (2) Return JSON theo Search Returns 202602 | MinIO | Webhook type 1 (đổi trạng thái đơn) và 12 (đổi trạng thái trả hàng), hay gửi trùng |
| Shopify | Đơn hàng JSONL theo cấu trúc REST Order, gồm `refunds` và `fulfillments` (REST là legacy, giữ vì đã xác minh field) | MinIO | Webhook, hay gửi trùng |
| Walmart | Đơn hàng và return JSON trong cột `raw_payload` JSONB, kèm `fetched_at` | PostgreSQL | Polling, đến theo lô |
| Shopee | Đơn hàng (`get_order_detail`) và return (`get_return_list`) JSON trong cột `raw_payload` JSONB, kèm `fetched_at` | PostgreSQL | Polling, đến theo lô |

Nhận xét:
- Amazon có 4 nguồn định dạng khác nhau, nên Silver phải ghép chúng theo `order-id`.
- Bronze có hai kiểu ingest: đọc file từ MinIO và đọc JDBC từ PostgreSQL.
- Chi tiết giữ nguyên từ tài liệu để buộc lớp ánh xạ trạng thái phải xử lý: TikTok Shop dùng `CANCELLED` ở API nhưng `CANCEL` ở webhook.

## 7. Dữ liệu tham chiếu

Các phần sau cần bảng dim có lịch sử (SCD2) và quan hệ dim–fact, nên generator xuất thêm:

| Dữ liệu | Nội dung | Nơi lưu | Dùng cho |
|---|---|---|---|
| Danh mục SKU | `sku_id`, tên, danh mục, giá vốn. Giả lập hệ thống ERP của Seller | PostgreSQL, `seller.skus` | `dim_sku` |
| Ảnh chụp Listing hàng ngày | Mỗi Listing mỗi ngày: Storefront, mã Listing của Channel (ASIN, variant ID, ...), giá, trạng thái. Giá đổi quanh các đợt sale, nên Prime Day tự sinh nhiều phiên bản mới | MinIO | `dim_listing` SCD2 |
| Tỷ giá ngày | Tỷ giá USD của GBP, EUR, VND theo ngày | MinIO | Quy đổi tiền ra USD |

Ảnh chụp Listing ở dạng chung do Seller tự xuất, không theo định dạng từng Channel. Đây là giả định để giữ phạm vi.

## 8. Bố cục lưu trữ

- **MinIO**, bucket `platform`, tiền tố `source-exports/`: đường dẫn theo nguồn rồi theo **ngày export**, ví dụ `source-exports/channel=amazon/dataset=orders/export_date=2026-03-14/`. Cùng bucket còn có `pg-load/` (dòng chờ nạp PostgreSQL), `ground-truth/` và `run_manifest.json`. Dùng một bucket vì manifest phải nằm trong một bucket nào đó, xem [Docker.md](../docker.md#1-môi-trường-nguồn-bằng-docker-compose). Chia theo ngày export (không theo ngày đặt hàng) vì export chồng lấn và thay đổi schema đều gắn với ngày export, và pipeline DP1 kéo dữ liệu theo từng ngày. Mỗi ngày vài file cỡ vừa. Một số ngày cố ý sinh rất nhiều file nhỏ để tạo bài toán small-file cho phần Lakehouse.
- **PostgreSQL** (giả lập hệ thống của phòng ban khác): mỗi nguồn một schema, gồm `walmart.orders`, `walmart.returns`, `shopee.orders`, `shopee.returns`, `seller.skus`. Bảng đơn giữ body API trong một cột `JSONB` cạnh vài cột khóa, như một hệ thống lưu lại response API. Bảng đơn không có khóa chính, vì bản trùng phải còn nguyên để pipeline phía sau dedup. Tiền tố `raw_` để dành cho tầng Bronze, không dùng ở hệ thống nguồn.
- **Kafka**: mỗi Channel một topic. Key là mã Order để các event của cùng một Order vào cùng partition. Value là payload gốc của Channel (ví dụ body webhook TikTok Shop).

### Tên file trong một thư mục `export_date=`

Mỗi thư mục `export_date=` chứa **nhiều** file, mỗi file của một ngày mô phỏng:

```
source-exports/channel=amazon/dataset=orders/export_date=2026-03-14/
    part-sim00039.jsonl      ← dòng do ngày mô phỏng 39 ghi
    part-sim00040.jsonl
    ...
```

Vì sao không dùng một file dùng chung: một Order được export vào ngày nó **đổi
trạng thái lần cuối**, muộn hơn ngày tạo tới khoảng 20 ngày (đo được tối đa 18 trên
736.129 Order; trần cứng từ hằng số vòng đời là `handling` ≤ 3 + `transit` ≤ 6 +
`completed` ≤ 7 = 16 ngày, cộng `overlap_days` = 3 cho bản trùng). Nên một file dùng
chung phải mở suốt ~20 ngày mô phỏng và nhận dòng từ cả 20 ngày đó — và lúc ấy không
có thời điểm nào nói được "ngày d đã xong và nằm trọn trên đĩa".

Phân vùng không đổi: thư mục vẫn là ngày export, nên Spark, Iceberg, Trino và
`generator/report.py` nhận đúng tập dòng như khi mỗi thư mục chỉ có một file.

### Chạy tiếp sau khi bị cắt

Lần chạy đạt ngưỡng 100 GB mất khoảng 3,2 giờ trên một core, nên nó phải chịu được
việc bị tắt giữa đường.

| Thành phần | Vai trò |
|---|---|
| `_checkpoint/day-NNNNN.json` | Vừa là dấu "ngày NNNNN đã xong", vừa chứa mọi bộ đếm dồn tới hết ngày đó. Gộp một file nên dấu và số không lệch nhịp |
| `part-simNNNNN.*` | File do ngày NNNNN ghi. Không có dấu của ngày NNNNN thì mấy file này là ghi dở |

Thứ tự ghi là điều kiện sống còn:

```
đóng file  →  fsync (ép xuống đĩa thật)  →  MỚI ghi dấu (ghi tạm rồi đổi tên)
```

Làm ngược lại thì mất điện có thể để lại dấu nói "ngày d xong" trong khi file ngày d
thiếu đuôi; lần chạy sau tin dấu, bỏ qua ngày d, và dữ liệu thiếu **mà manifest vẫn
cộng ra số đẹp, không lỗi nào báo**.

Lúc khởi động: tìm `d` lớn nhất mà dấu của mọi ngày `1..d` đều có (không lấy số lớn
nhất tìm được — một dấu đứng lẻ sau khoảng trống không chứng minh được gì), nạp bộ
đếm từ dấu đó, xóa mọi `part-sim` của ngày `> d`, rồi chạy từ `d+1`. Cờ `--restart`
bỏ qua dấu và sinh lại từ ngày 1.

Điều này đúng được vì mỗi ngày mô phỏng rút số ngẫu nhiên riêng theo
`(seed, "batch_exports", day)` và không đọc trạng thái ngày trước
(`tests/unit/generator/core/test_simulate.py::test_a_day_does_not_depend_on_previous_days`), nên ngày `d` cho
ra cùng dữ liệu dù chạy từ ngày 1 hay nhảy vào từ ngày `d`. `tests/unit/generator/test_generate_order_snapshots_resume.py`
khóa lời hứa đó bằng cách so từng byte giữa một lần chạy liền mạch và một lần bị cắt.

## 9. Buyer, tiền tệ và thời gian

- Lõi biết buyer đầy đủ (mã, quốc gia, mã bưu chính). Renderer chỉ lộ những gì Channel lộ thật:

  | Channel | Mã buyer | Ghi chú |
  |---|---|---|
  | Walmart | `buyerId` | Có `isGuest` |
  | Shopify | `customer.id` | Null với khách không đăng nhập |
  | TikTok Shop | `user_id` | Chỉ có nghĩa trong TikTok Shop |
  | Shopee | `buyer_user_id` | Chỉ có nghĩa trong Shopee |
  | Amazon | Không có mã buyer ổn định | Chỉ có thông tin giao hàng bị hạn chế |

  Không mã nào dùng chung được giữa các Channel, nên feature vẫn tra theo `sku_id` + `storefront_id`.
- Tiền giữ nguyên số gốc kèm mã tiền tệ gốc (USD, GBP, EUR, VND theo Storefront), quy ra USD bằng bảng tỷ giá ngày ở [mục 7](#7-dữ-liệu-tham-chiếu). Tỷ giá sinh từ chuỗi giả lập dao động nhẹ quanh giá trị gốc. Shopify giữ cả tiền của store (`currency`) và tiền khách trả (`presentment_currency`).
- Thời gian lưu UTC.
  - `event_time` là thời điểm sự kiện xảy ra, nằm sẵn trong payload gốc (ví dụ `update_time` của TikTok Shop, `LastUpdateDate` của Amazon).
  - `ingested_at` là thời điểm hệ thống nhận. Với stream là timestamp của Kafka record lúc generator gửi. Với Batch là `export_date` hoặc `fetched_at`.
  - Chênh lệch giữa hai mốc là late arrival.

## 10. Cấu hình

Toàn bộ tham số nằm trong một file YAML. Tên khóa và giá trị dưới đây là **đề xuất** cho toàn bộ generator. Các khóa đã có code, kèm ý nghĩa từng khóa, xem [mục 15](#các-khóa-cấu-hình-đang-dùng).

```yaml
seed: 42
scale:
  n_skus: 2000
  start_date: 2026-01-01     # ngày mô phỏng 1
  days_history: 180          # từ ngày 1 tới cutoff
  days_stream: 30            # sau cutoff
  orders_per_day: 4000
  sku_zipf_exponent: 0.8     # SKU bán chạy nhất khoảng 5% Order Line (tính với 2.000 SKU)
storefront_weights:          # skew theo Storefront
  amazon_us: 0.45
  tiktok_shop_us: 0.15
  amazon_uk: 0.12
  amazon_de: 0.10
  shopify: 0.08
  walmart_us: 0.07
  shopee_vn: 0.03
labels:
  base_return_rate: 0.11
  window_days: 30
offline_problems:
  duplicate_rate: 0.02
  overlap_days: 3
  amazon_orders_api_switch_day: 100    # v0 sang 2026-01-01
  small_file_days: [45, 46, 47]
streaming_problems:
  pacing:
    default_seconds_per_sim_day: 60
    segments:                          # phát chậm quanh sự kiện cần quan sát
      - {from_day: 195, to_day: 195, seconds_per_sim_day: 600}
  duplicate_rate: {tiktok_shop_us: 0.015, shopify: 0.015}
  amazon_late: {median_minutes: 5, share_over_1h: 0.05}
  polling_batch_minutes: [5, 15]
  burst_multiplier: [5, 8]
drift:
  prime_day: {day: 120, days: 2}
  tiktok_live: {day: 195, start_hour_utc: 12, duration_hours: 6}
storage:
  minio_bucket: source-exports
  postgres_channels: [walmart_us, shopee_vn]
```

Mọi con số có thể tăng khi chạy trên cloud (ví dụ `n_skus`, `orders_per_day`). `sku_zipf_exponent` cần chỉnh lại sau khi đo để đạt mục tiêu khoảng 5%.

## 11. Bản đáp án và bằng chứng chất lượng

### `run_manifest.json`

Sau mỗi lần chạy, generator ghi `run_manifest.json`: seed, cấu hình, số dòng theo nguồn, tỷ lệ lỗi thật đã chèn (duplicate, late, burst), ngày đổi schema, các đợt drift, dung lượng và số file theo nguồn. Đây là đáp án để đối chiếu.

### Bảng nhãn (`labels`)

Generator xuất riêng bảng nhãn thật, không chỉ ghi trong manifest, để join theo ID với bảng feature khi train.

| Cột | Nghĩa |
|---|---|
| `order_line_id` | Mã Order Line chuẩn của lõi |
| `label` | 1 nếu Order Line bị Return hoặc Refund trong 30 ngày kể từ `delivered_at`, ngược lại 0 |
| `delivered_at` | Ngày giao thật (UTC), để join theo thời điểm |
| `label_available_at` | `delivered_at` + 30 ngày, lúc nhãn thật sự biết được |

- Chỉ có Order Line đã giao. Order bị hủy trước khi giao không có nhãn.
- Bảng của Batch chỉ gồm dòng có `label_available_at` trước `cutoff`. Dòng còn lại có nhãn khi phát Stream.
- Lưu dạng Parquet tại `ground-truth/labels/` trong MinIO, tách khỏi tiền tố `source-exports/` vì đây không phải dữ liệu của Channel nào.
- Để join được với dữ liệu Channel, bảng phụ `ground-truth/order_line_keys/` ánh xạ `order_line_id` sang mã gốc của Channel (ví dụ `OrderItemId` của Amazon).

### Báo cáo chất lượng dữ liệu

Báo cáo chất lượng dữ liệu có hai phần:

- **Module `generator/report.py`** chứa logic đo, có docstring và unit test, dùng DuckDB (nhẹ, có `approx_count_distinct`, đọc được cả MinIO lẫn PostgreSQL). Có `--dsn` thì Walmart và danh mục SKU được đọc từ PostgreSQL, không có thì đọc file `pg-load/`.
- **Lệnh `python -m generator report`** in các bảng đo ra terminal để chụp, hoặc in dạng Markdown (`--markdown`) để dán vào tài liệu. Notebook vẽ biểu đồ để sau: cần thêm Jupyter và thư viện vẽ, trong khi bảng chữ đã đủ cho ảnh chụp.
- **Stream** có hai nguồn số: `stream_manifest.json` giữ số tổng của lần phát, còn `scripts/evidence/measure_order_snapshots_stream.py` phát lại Stream trong bộ nhớ và in ví dụ cụ thể cho từng vấn đề (một cặp tin trùng, một tin trễ nặng nhất, bảng Order mỗi giờ của ngày livestream).
- **Ảnh chụp và báo cáo** tách theo đường thời gian, mỗi bên một tài liệu và một thư mục ảnh: Batch ở [data-quality-report-batch.md](data_quality_report_batch.md) với `docs/images/batch/`, Stream ở [data-quality-report-stream.md](data_quality_report_stream.md) với `docs/images/stream/`. Mỗi ảnh kèm lời giải thích nó chứng minh điều gì.

| Đặc tính cần đo | Báo cáo đo gì |
|---|---|
| Skew distribution (city/category %) | Phần trăm Order theo thành phố, theo danh mục, theo Storefront, theo SKU top, theo ngày |
| Cardinality | `approx_count_distinct` của `order_id`, `order_line_id`, `tracking_number`, mã buyer |
| Schema evolution | Tỷ lệ null của cột `proceeds` (chỉ có ở dạng 2026, do tên khoá đổi) theo từng `export_date`, trước và sau ngày đổi schema |
| Duplicate trước và sau dedup | Tỷ lệ trùng theo công thức ở mục 5, trước và sau khi áp quy tắc dedup (giữ bản mới nhất theo khóa). Báo cáo tự tính cả hai để phần generator không phụ thuộc Spark. Job Spark sau này phải ra cùng con số |
| Streaming burst/late/duplicate | Số message mỗi phút, phân phối `ingested_at - event_time`, tỷ lệ `tts_notification_id` lặp |
| Data volume, format sau lưu | Dung lượng và số file theo nguồn, định dạng từng nguồn |

Mỗi con số đo được đặt cạnh con số trong `run_manifest.json`.

## 12. Kỹ thuật và đóng gói

- Python, số ngẫu nhiên bằng `numpy` `Generator` với một `seed` duy nhất. Cùng seed và cùng cấu hình cho cùng dữ liệu. Riêng thời điểm Kafka nhận message phụ thuộc đồng hồ thật.
- Hai chế độ chạy: sinh Batch (job chạy một lần) và phát Stream (tiến trình chạy lâu, gửi vào Kafka).
- Ghi ra qua `fsspec`/S3 API vào MinIO hoặc bucket cloud, không qua đĩa máy cá nhân.
- Chia theo ngày để ít tốn RAM. Hơn 1 triệu Order Line chạy được trong một process, chưa cần Spark.
- Unit test local chỉ chạy cấu hình rất nhỏ (ví dụ 20 SKU, 3 ngày).
- Mỗi file có đoạn mô tả ở đầu file, mỗi hàm và class có docstring, theo yêu cầu chung của bộ dữ liệu.
- **Docker**: có Docker Compose (MinIO, PostgreSQL, Kafka) cho môi trường thử. Để có số liệu "giảm từ bao nhiêu xuống bao nhiêu", build trước một image đơn giản (một stage, base image đầy đủ) và ghi dung lượng, sau đó build bản tối ưu (multi-stage, base image slim, không giữ cache cài đặt) và ghi dung lượng. Kết quả ghi vào `docs/edai-1/docker.md`.
- Hạ tầng nặng chạy trên cloud, dựng bằng Terraform và `destroy` khi nghỉ.

## 13. Giả định và việc còn mở

**Giả định** (đã ghi rõ, cần xác minh nếu có nguồn):
- Quy luật sinh nhãn và các hệ số.
- Danh sách khoảng 10 mã lý do trả của Amazon.
- Ngày giao của Amazon và Shopee (chỉ thấy proxy).
- Report `GET_FLAT_FILE_RETURNS_DATA_BY_RETURN_DATE` chỉ chứa đơn MFN, hàng trả FBA nằm ở report FBA Returns.
- Ngày Amazon chuyển từ Orders v0 sang Orders 2026-01-01 là ngày tự chọn, không phải lịch thật của Amazon.
- Giá trị `ReturnStatus` của Shopee ngoài `REQUESTED` và `CANCELLED`.
- Danh sách trạng thái return của Walmart, danh sách cột đầy đủ của report FBA Shipments và FBA Returns.
- Notification của Amazon và webhook của Shopify: payload và cách Channel gửi trùng chưa đọc từ tài liệu chính thức.
- Ảnh chụp Listing dạng chung do Seller tự xuất.

**Việc còn mở:**
- Tình trạng REST và GraphQL của Shopify năm 2026.
- Dung lượng đã đo trong manifest nguồn: 100.012.790.412 byte cho Amazon/Walmart/SKU; xem [báo cáo Batch](data_quality_report_batch.md).
- Quy mô Spark đã kiểm bằng `configs/scale-100gb.yaml`; benchmark 20/180 ngày ở [Spark](spark.md), pipeline Airflow dùng cửa sổ nhỏ hơn ghi trong [Airflow](../pipeline_orchestration.md). Không dùng kế hoạch `large.yaml` cũ.
- Bronze/Silver đủ năm Channel, Gold fact, Amazon SCD2 và feature offline đã chạy; schema và phạm vi bằng chứng ở [Schema](../schema_design.md) và [Gold](gold.md).

## 14. Thiết kế và evidence

| Chủ đề | Thiết kế | Evidence / cách kiểm chứng |
|---|---|---|
| Docker & Docker Compose | Mục 12 | `docs/edai-1/docker.md` |
| Optimize Dockerfile | Mục 12 | Dung lượng image trước và sau tối ưu |
| Simulate skew | Mục 5, offline | Ảnh chụp báo cáo, mục 11 |
| Simulate high cardinality | Mục 5, offline | Ảnh chụp `approx_count_distinct` |
| Simulate schema evolution | Mục 5, offline | Ảnh chụp tỷ lệ null theo `export_date` |
| Simulate another offline problem | Mục 5, duplicate 2% | Ảnh chụp tỷ lệ trùng trước và sau dedup |
| Using generator configuration | Mục 10 | File cấu hình và tài liệu từng khóa |
| Store data để ingest vào Bronze | Mục 6, 7, 8 | Ảnh chụp MinIO và PostgreSQL |
| Simulate burst | Mục 5, streaming | Ảnh chụp số message mỗi phút |
| Simulate late arrivals | Mục 5, streaming | Ảnh chụp phân phối độ trễ |
| Simulate another streaming problem | Mục 5, duplicate 1.5% | Ảnh chụp tỷ lệ mã thông báo lặp |
| Using generator configuration (streaming) | Mục 10, `streaming_problems` | File cấu hình |
| Document data characteristic | Mục 6, 8, 11 | Dung lượng, số file, định dạng từ báo cáo |
| Document config generator | Mục 10 | Tài liệu từng khóa |

Các phần sau dùng dữ liệu của generator: dim SCD2 và quan hệ dim–fact (mục 7), bài toán small-file của Lakehouse (mục 8), window của Flink (mục 5, streaming).

## 15. Triển khai

Mục này ghi phần đã có code: code nằm ở đâu, chạy thế nào, và vì sao viết như vậy. Mỗi file trong `generator/` có đoạn mô tả ở đầu file, giải thích kỹ hơn.

### Tiến độ

| Bước | Nội dung | Trạng thái |
|---|---|---|
| 1 | Lõi mô phỏng, Amazon Orders (có đổi schema), Walmart Orders, duplicate do export chồng lấn, bảng nhãn, manifest, Sale Event kiểu Prime Day | Xong |
| 2 | Docker Compose (MinIO, PostgreSQL), bảng `seller.skus`, lệnh nạp PostgreSQL, `report.py`, Dockerfile tối ưu | Xong (notebook để sau) |
| 2b | Amazon Returns và FBA reports, small-file, Listing, tỷ giá | Chưa làm |
| 3 | Stream qua Kafka: burst (livestream TikTok), late (Amazon), duplicate (TikTok Shop), nhịp phát theo đoạn | Xong. Shopify, Walmart và Shopee trong Stream để bước 4 |
| 4 | TikTok Shop, Shopify, Shopee, drift | Ba renderer Batch và Bronze/Silver/Gold đã chạy trên nguồn nhỏ; Stream ba Channel và concept drift chưa làm |

### Chạy thử

```bash
uv sync                                                   # cài thư viện vào .venv
uv run pytest -q                                          # chạy test, vài giây
uv run python -m generator batch --config configs/small.yaml     # dữ liệu nhỏ, khoảng 1 giây, ghi vào ./out/small
uv run python -m generator batch --config configs/default.yaml --output s3://...   # đủ quy mô
uv run python -m generator report --source ./out/small                # báo cáo chất lượng từ file local
```

Chạy cả môi trường nguồn (MinIO, PostgreSQL, sinh và nạp dữ liệu) bằng Docker Compose, xem [Docker.md](../docker.md).

Với cấu hình đầy đủ (2.000 SKU, 180 ngày, 4.000 Order mỗi ngày), một lần chạy trên máy cá nhân mất khoảng 82 giây, dùng tối đa khoảng 800 MB RAM, ghi ra khoảng 485 MB.

### Bản đồ code

Code chia theo đúng ba lớp ở [mục 2](#2-kiến-trúc-ba-lớp). Đọc theo thứ tự dưới đây là dễ hiểu nhất.

| File | Lớp | Việc |
|---|---|---|
| `generator/config.py` | | Đọc YAML vào dataclass, báo lỗi khi gõ sai khóa |
| `generator/core/storefronts.py` | Lõi | Hằng số: 7 Storefront (tiền tệ, thành phố, tỷ lệ Channel giao hàng), 8 danh mục (tỷ lệ trả mục tiêu, khoảng giá) |
| `generator/core/rngs.py` | Lõi | Bộ sinh số ngẫu nhiên riêng cho từng phần, để cùng seed luôn ra cùng dữ liệu |
| `generator/core/catalog.py` | Lõi | Sinh SKU, SKU Defect Propensity, độ phổ biến Zipf và phân phối của ngày sale |
| `generator/core/model.py` | Lõi | `Order`, `OrderLine`, `Buyer`, và `Order.status_at(t)` |
| `generator/core/returns.py` | Lõi | Hàm logistic tính xác suất trả, độ trễ trả, nhãn |
| `generator/core/simulate.py` | Lõi | Sinh Order từng ngày cùng toàn bộ vòng đời |
| `generator/delivery/batch_exports.py` | Giao hàng | Order nằm ở ngày export nào, chèn duplicate |
| `generator/channels/amazon.py`, `walmart.py` | Renderer | Vẽ Order thành bản ghi gốc của Channel |
| `generator/channels/tiktok.py` | Renderer | Webhook TikTok Shop type 1 (đổi trạng thái đơn) cho Stream |
| `generator/delivery/stream_events.py` | Giao hàng | Đổi trạng thái sau `cutoff` thành message: lúc đến, trễ, bản trùng, nhịp phát |
| `generator/publish_order_snapshot_events.py` | | Lệnh `generator.publish_order_snapshot_events`: phát vào Kafka hoặc ra file, đo và ghi `stream_manifest.json` |
| `generator/generate_order_snapshots.py` | | Nối ba lớp, ghi file, ghi manifest |
| `generator/storage.py` | | Ghi và đọc thư mục local hoặc MinIO qua fsspec |
| `generator/load_postgres.py` | | Lệnh `load-postgres`: nạp `pg-load/` vào `walmart.orders` và `seller.skus` bằng `COPY` |
| `generator/report.py` | | Lệnh `report`: đo lại chất lượng dữ liệu Batch bằng DuckDB, đặt cạnh đáp án trong manifest |
| `scripts/evidence/measure_order_snapshots_stream.py` | | Phát lại Stream trong bộ nhớ, in ví dụ cụ thể của trùng, trễ và burst để chụp ảnh |

### Bốn ý chính của cách làm

**1. Lõi sinh cả tương lai của Order, renderer chỉ "chụp ảnh".** Khi tạo một Order, lõi quyết định luôn lúc thanh toán, gửi, giao, có bị trả không. Kết quả lưu thành `timeline`, danh sách các lần đổi Order Status. Renderer gọi `order.status_at(t)` để biết Order trông thế nào tại thời điểm `t`. Nhờ vậy cùng một Order có thể xuất ở nhiều ngày export với trạng thái khác nhau, và sau này Stream chỉ cần chụp tiếp sau `cutoff`.

**2. Mỗi Order xuất đúng một lần, rồi mới chèn duplicate.** Bản chính của một Order nằm ở ngày export chứa lần cập nhật cuối của nó trước `cutoff`. Khi chưa chèn lỗi, tỷ lệ trùng theo khóa bằng 0, nên tỷ lệ trùng đo được đúng bằng phần đã chèn. Với xác suất `p = r / (1 - r)` (r là `duplicate_rate`), Order có thêm một bản: bản y hệt ở ngày export muộn hơn, hoặc bản cũ hơn ở ngày export sớm hơn. Công thức cho `p` giải thích ở đầu `batch_exports.py`.

**3. Sale Event đổi tham số của lõi, không sửa dữ liệu sau khi sinh.** Trong hai ngày Prime Day, lõi nhân số Order của các Storefront Amazon lên, tăng tỷ lệ và mức giảm giá, và dồn nhu cầu vào một nhóm SKU deal. Mức giảm giá nằm sẵn trong hàm tính xác suất trả, nên tỷ lệ trả của mấy ngày này tự nhích lên mà không cần chỉnh riêng. Đó là covariate drift: feature đổi phân phối, còn quan hệ giữa feature và nhãn thì giữ nguyên.

**4. Mã gốc của Channel suy ra từ mã chuẩn bằng hàm băm.** `AmazonOrderId`, `purchaseOrderId`, ASIN, mã vận đơn không được rút ngẫu nhiên mà băm từ mã chuẩn (`ORD-017-00042`) cùng seed. Cùng một Order luôn có cùng mã gốc ở mọi lần export, ở cả Batch lẫn Stream, và renderer không cần lưu bảng tra.

### Đo được ở lần chạy đầy đủ (Batch)

| Chỉ số | Mục tiêu | Đo được |
|---|---|---|
| Tỷ lệ trùng `amazon.orders` | 2% | 1,98% (4.819 bản cũ hơn, 4.933 bản y hệt) |
| Tỷ lệ trùng `walmart.orders` | 2% | 1,98% |
| Tỷ lệ trả toàn bộ | khoảng 10 đến 12% | 11,2% |
| Tỷ lệ trả quần áo | khoảng 22% | 23,3% |
| Tỷ lệ trả đồ gia dụng | khoảng 4% | 4,6% |
| Order Amazon mỗi ngày trong Sale Event | gấp 4 lần ngày thường | 10.642 so với 2.679, tức gấp 3,97 lần |
| Mức giảm giá trung bình của Amazon trong Sale Event | khoảng 21% | 21,0% so với 4,4% ngày thường |
| Tỷ lệ trả của Amazon trong Sale Event | cao hơn ngày thường | 13,5% so với 11,3% |
| SKU bán chạy nhất trong Order Line Amazon | khoảng 5% | 5,4% (đo ở lần chạy 60 ngày) |

Số của tỷ lệ trả khớp mục tiêu vì mọi tác động trong hàm logistic được tâm hóa (trừ giá trị trung bình), giải thích ở đầu `returns.py`.

### Stream

Lệnh `generator.publish_order_snapshot_events` sinh lại khoảng 20 ngày cuối trước `cutoff` (để bắt Order cũ còn đổi trạng thái), rồi chạy tiếp `days_stream` ngày. Mỗi lần Order Amazon hoặc TikTok Shop đổi trạng thái trong khoảng `[cutoff, stream_end)` thành một message:

| Topic | Dạng | Khóa dedup | Vấn đề |
|---|---|---|---|
| `amazon.order-notifications` | Notification `ORDER_CHANGE` (giả định, chưa đối chiếu tài liệu chính thức) | `NotificationMetadata.NotificationId` | Late: `EventTime` là lúc đổi trạng thái, `PublishTime` là lúc đến |
| `tiktok_shop.order-webhooks` | Webhook type 1, theo channel-sources.md mục 5 | `tts_notification_id` | Duplicate, burst |

Khóa Kafka là mã đơn gốc, nên message của một Order vào cùng partition. Message được gửi theo thứ tự lúc đến, nên message Amazon trễ đứng sau message có `event_time` muộn hơn.

**Burst nằm ở lõi, không ở lớp giao hàng.** Giống Prime Day, livestream đổi tham số của lõi: trong vài giờ, số Order TikTok Shop gấp `live_multiplier` lần. Hệ số rút một lần trong khoảng `burst_multiplier` và ghi vào `stream_manifest.json`. Order thêm dùng bộ sinh số riêng, nên các Order khác của ngày đó không đổi.

Số đo ở lần chạy `configs/default.yaml` vào Kafka qua Compose (`STREAM_SPEED=60`, 43,7 giây). Số message trong Kafka khớp số đếm. Báo cáo đầy đủ kèm cách kiểm chứng: [data-quality-report-stream.md](data_quality_report_stream.md).

| Chỉ số | Mục tiêu | Đo được |
|---|---|---|
| Message TikTok Shop | | 93.554 |
| Message Amazon | | 399.410 |
| Tỷ lệ trùng TikTok Shop | 1,5% | 1,49% |
| Tỷ lệ trùng Amazon | 0 | 0 |
| Độ trễ trung vị Amazon | vài phút | 5,3 phút |
| Message Amazon trễ hơn 1 giờ | 5% | 4,98% |
| Message Amazon đến sau message có `event_time` muộn hơn quá 1 giờ | có | 19.729 |
| Order TikTok Shop mới mỗi giờ trong livestream so với cùng giờ ngày khác | gấp 5 đến 8 lần (đáp án 5,45) | 148,5 so với 25,2, tức gấp 5,89 lần |

`measured_ratio` so với trung bình các ngày Stream khác, nên nó chịu ảnh hưởng của việc ngày livestream vốn đông hay vắng. So với chính ngày đó khi tắt livestream thì tỷ lệ là 5,09. Xem [data-quality-report-stream.md](data_quality_report_stream.md#burst-của-livestream-tiktok).

### Đơn giản hóa so với Channel thật

- Amazon v0 thật tách Order Item sang lời gọi `getOrderItems`. Ở đây `OrderItems` nằm chung bản ghi, một dòng JSON mỗi Order.
- Mọi Order Line của một Order cùng trạng thái, không có giao từng phần.
- Mỗi Order Line được quyết định Return độc lập, không có tương quan giữa các dòng trong cùng Order. Ngoài đời, buyer đã trả một món thì dễ trả thêm món khác trong cùng đơn (mua nhiều size để thử, không hài lòng cả đơn). Vì vậy dữ liệu chưa có tín hiệu kiểu "cả đơn cùng bị trả" cho mô hình học.
- Walmart ghi ra `pg-load/walmart.orders/`, là các dòng sẽ nạp vào bảng `walmart.orders` bằng lệnh `load-postgres`.
- Tỷ giá tạm cố định trong `price_factor` của Storefront. Bảng tỷ giá ngày làm ở bước 2b.
- Notification Amazon trong Stream luôn dùng giá trị trạng thái kiểu v0 (`Shipped`), không đổi theo ngày chuyển schema.
- Livestream chưa có concept drift (tỷ lệ trả quần áo tăng). Phần này làm ở bước 4.


### Các khóa cấu hình đang dùng

Generator đọc YAML vào dataclass trong `generator/config.py`. Khóa không có trong file thì lấy giá trị mặc định. Khóa gõ sai làm lệnh dừng với lỗi, không bị bỏ qua. Hai file mẫu: `configs/small.yaml` (chạy thử vài giây) và `configs/default.yaml` (đủ quy mô).

| Khóa | Mặc định | Ý nghĩa | Vấn đề dữ liệu liên quan |
|---|---|---|---|
| `seed` | 42 | Hạt giống ngẫu nhiên. Cùng seed và cùng cấu hình cho cùng dữ liệu | |
| `scale.n_skus` | 2000 | Số SKU trong danh mục của Seller | Cardinality của SKU, ASIN |
| `scale.start_date` | 2026-01-01 | Ngày mô phỏng 1 | |
| `scale.days_history` | 180 | Số ngày lịch sử. `cutoff` là 00:00 UTC ngay sau ngày cuối | Volume |
| `scale.days_stream` | 30 | Số ngày Stream sau `cutoff` | |
| `scale.orders_per_day` | 4000 | Số Order mỗi ngày trên cả 7 Storefront, trước khi nhân Sale Event | Volume, cardinality |
| `scale.sku_zipf_exponent` | 0.8 | Độ lệch của phân phối SKU. Lớn hơn thì SKU bán chạy nhất chiếm nhiều hơn | Skew theo SKU |
| `storefront_weights.<storefront>` | Amazon US 0.45, ... | Tỷ trọng Order theo Storefront, phải cộng lại bằng 1 | Skew theo Storefront |
| `labels.window_days` | 30 | Cửa sổ Return Label tính từ ngày giao | |
| `offline_problems.duplicate_rate` | 0.02 | Tỷ lệ trùng theo khóa trong mỗi dataset, nằm trong [0, 0.5) | Duplicate |
| `offline_problems.overlap_days` | 3 | Export lấy lại bao nhiêu ngày gần nhất. Bản trùng rơi vào khoảng này | Duplicate |
| `offline_problems.amazon_orders_api_switch_day` | 100 | Từ ngày export này Amazon ghi theo Orders 2026-01-01 | Schema evolution |
| `drift.prime_day.day`, `.days` | 120, 2 | Ngày bắt đầu và độ dài Sale Event trên Storefront Amazon | Skew theo ngày, covariate drift |
| `drift.prime_day.order_multiplier` | 4 | Số Order Amazon trong ngày sale gấp mấy lần ngày thường | Skew theo ngày |
| `drift.prime_day.discount_share` | 0.7 | Tỷ lệ Order Line Amazon được giảm giá trong ngày sale | Covariate drift |
| `drift.prime_day.deal_skus` | 50 | Số SKU hút phần lớn nhu cầu trong ngày sale | Skew theo SKU |
| `streaming_problems.pacing.default_seconds_per_sim_day` | 60 | Số giây thật cho mỗi ngày mô phỏng khi phát Stream | Burst thấy được trên Flink UI |
| `streaming_problems.pacing.segments` | trống | Danh sách `{from_day, to_day, seconds_per_sim_day}` phát với tốc độ riêng | Burst thấy được trên Flink UI |
| `streaming_problems.duplicate_rate.<storefront>` | `tiktok_shop_us: 0.015` | Tỷ lệ trùng theo khóa dedup trong Stream | Duplicate |
| `streaming_problems.amazon_late.median_minutes` | 5 | Độ trễ trung vị của phần notification trễ dưới 1 giờ | Late |
| `streaming_problems.amazon_late.share_over_1h` | 0.05 | Tỷ lệ notification Amazon trễ 1 đến 6 giờ | Late, lệch thứ tự |
| `drift.tiktok_live.day` | không có | Ngày mô phỏng có livestream, phải nằm trong khoảng Stream | Burst |
| `drift.tiktok_live.start_hour_utc`, `.duration_hours` | 12, 6 | Giờ bắt đầu và độ dài buổi live | Burst |
| `drift.tiktok_live.burst_multiplier` | [5, 8] | Khoảng hệ số nhân số Order TikTok Shop trong buổi live | Burst |
| `storage.output_root` | `./out` | Nơi ghi: thư mục local hoặc `s3://bucket`. Lệnh `batch --output` ghi đè khóa này | |

Concept drift của livestream (tỷ lệ trả quần áo tăng) thêm ở bước 4.

### Kết quả chạy

Số liệu dưới đây lấy từ lần chạy `configs/default.yaml` qua Docker Compose: `generator-batch` ghi vào MinIO (102 giây), `generator-load` nạp PostgreSQL (3 giây). Sau đó `python -m generator report --source s3://platform --dsn ... --markdown` chạy trên máy host, đọc Amazon từ MinIO, đọc Walmart và `seller.skus` từ PostgreSQL. Các cột `_truth` lấy từ `run_manifest.json`.

Ảnh chụp màn hình (terminal của lệnh `report`, MinIO console, DBeaver hoặc `psql`) nằm ở `docs/images/batch/` và được nhúng trong [data-quality-report-batch.md](data_quality_report_batch.md).

Cách đọc từng bảng:

- **storefront_skew**: số Order đo từ dữ liệu đã xuất trùng khớp đáp án ở cả ba Storefront Amazon và Walmart. Ba Storefront chưa có renderer để trống.
- **city_skew**: thành phố lớn nhất chiếm khoảng 46 đến 49% Order của mỗi Storefront.
- **category_skew**: tỷ trọng danh mục lệch (apparel 17,7%, beauty 7,6%), và tỷ lệ trả khác nhau theo danh mục (apparel 23%, home 4,6%). Danh mục chỉ có trong `seller.skus`, nên bảng này phải join với PostgreSQL.
- **top_skus**: SKU bán chạy nhất chiếm 5,4% Order Line. Khi Spark group theo SKU, task chứa SKU này sẽ chạy chậm rõ rệt.
- **daily_orders**: hai ngày Sale Event có lượng Order gấp khoảng 3,7 lần trung vị.
- **cardinality**: khoảng 500 nghìn mã Order Amazon và 720 nghìn mã Order Line. HyperLogLog của DuckDB lệch tới hơn 20% ở vài cột, nên bảng in thêm số đếm chính xác. Job Spark sau này sẽ so với chính số đếm này.
- **schema_evolution**: `proceeds` null 100% trước ngày 2026-04-10 và 0% từ ngày đó. `IsPrime` ngược lại. Ranh giới rơi đúng vào một ngày export.
- **duplicates**: tỷ lệ trùng đo được bằng đúng đáp án (1,98% và 1,99%, mục tiêu 2%), và bằng 0 sau khi giữ bản mới nhất theo khóa. Ở Amazon, khoảng một nửa số bản trùng là bản cũ hơn (trạng thái cũ), nên dedup chỉ bỏ bản giống hệt là không đủ.
- **volume**: Amazon 388 MB JSON Lines trong 180 file, mỗi ngày export một file. Bảng `walmart.orders` trong PostgreSQL 74 MB.

#### Bảng `storefront_skew`

Order theo Storefront. `orders_exported` trống nghĩa là Channel chưa có renderer.

| storefront_id | config_weight | orders_truth | pct_truth | orders_exported |
|---|---|---|---|---|
| amazon_us | 0.45 | 334046 | 45.38 | 334046 |
| tiktok_shop_us | 0.15 | 108042 | 14.68 |  |
| amazon_uk | 0.12 | 89332 | 12.14 | 89332 |
| amazon_de | 0.1 | 74778 | 10.16 | 74778 |
| shopify | 0.08 | 57673 | 7.83 |  |
| walmart_us | 0.07 | 50499 | 6.86 | 50499 |
| shopee_vn | 0.03 | 21759 | 2.96 |  |

#### Bảng `city_skew`

Vài thành phố lớn chiếm phần lớn Order của mỗi Storefront.

| storefront_id | city | orders | pct |
|---|---|---|---|
| amazon_de | Berlin | 36723 | 49.11 |
| amazon_de | Hamburg | 15019 | 20.08 |
| amazon_de | Munich | 8836 | 11.82 |
| amazon_de | Cologne | 6112 | 8.17 |
| amazon_de | Frankfurt | 4404 | 5.89 |
| amazon_de | Stuttgart | 3684 | 4.93 |
| amazon_uk | London | 43798 | 49.03 |
| amazon_uk | Manchester | 17889 | 20.03 |
| amazon_uk | Birmingham | 10376 | 11.62 |
| amazon_uk | Leeds | 7406 | 8.29 |
| amazon_uk | Glasgow | 5668 | 6.34 |
| amazon_uk | Bristol | 4195 | 4.7 |
| amazon_us | New York | 154019 | 46.11 |
| amazon_us | Los Angeles | 62144 | 18.6 |
| amazon_us | Chicago | 36338 | 10.88 |
| amazon_us | Houston | 25409 | 7.61 |
| amazon_us | Phoenix | 18969 | 5.68 |
| amazon_us | Seattle | 14825 | 4.44 |
| amazon_us | Miami | 12121 | 3.63 |
| amazon_us | Denver | 10221 | 3.06 |
| walmart_us | Dallas | 24843 | 49.2 |
| walmart_us | Houston | 9851 | 19.51 |
| walmart_us | Atlanta | 6055 | 11.99 |
| walmart_us | Chicago | 4142 | 8.2 |
| walmart_us | Phoenix | 3112 | 6.16 |
| walmart_us | Orlando | 2496 | 4.94 |

#### Bảng `category_skew`

Order Line theo danh mục. `return_pct_truth` là tỷ lệ trả thật từ manifest.

| category | order_lines | pct | return_pct_truth |
|---|---|---|---|
| apparel | 140711 | 17.71 | 23.32 |
| toys | 115003 | 14.48 | 6.69 |
| electronics | 112797 | 14.2 | 8.1 |
| accessories | 105104 | 13.23 | 10.16 |
| sports | 90499 | 11.39 | 8.3 |
| home | 85100 | 10.71 | 4.57 |
| footwear | 84430 | 10.63 | 16.08 |
| beauty | 60717 | 7.64 | 5.83 |

#### Bảng `top_skus`

SKU bán chạy nhất nên chiếm khoảng 5% Order Line: tạo task chậm cho Spark.

| sku | order_lines | pct |
|---|---|---|
| ELE-01103 | 43073 | 5.42 |
| TOY-01453 | 24485 | 3.08 |
| SPT-01304 | 18065 | 2.27 |
| ACC-00934 | 14265 | 1.8 |
| TOY-01474 | 11904 | 1.5 |
| FTW-00683 | 10132 | 1.28 |
| ACC-00929 | 9094 | 1.14 |
| TOY-01568 | 8137 | 1.02 |
| HOM-01853 | 7522 | 0.95 |
| SPT-01238 | 7264 | 0.91 |

#### Bảng `daily_orders`

Ngày nhiều Order nhất (Amazon và Walmart). Ngày Sale Event trong manifest: ['2026-04-30', '2026-05-01'].

| day | orders | x_median |
|---|---|---|
| 2026-05-01 | 10950 | 3.7 |
| 2026-04-30 | 10887 | 3.68 |
| 2026-03-13 | 3119 | 1.05 |
| 2026-03-07 | 3079 | 1.04 |
| 2026-04-29 | 3077 | 1.04 |

#### Bảng `cardinality`

Số giá trị khác nhau: ước lượng HyperLogLog của DuckDB và số đếm chính xác. HyperLogLog của DuckDB dùng ít bộ đếm nên có thể lệch hơn 10%.

| dataset | column | approx_distinct | exact_distinct | approx_err_pct | rows |
|---|---|---|---|---|---|
| amazon.orders | order_id | 385608 | 498156 | -22.59 | 508227 |
| amazon.orders | line_id | 818537 | 721099 | 13.51 | 721099 |
| amazon.orders | asin | 1844 | 2000 | -7.8 | 721099 |
| amazon.orders | sku | 1526 | 2000 | -23.7 | 721099 |
| walmart.orders | order_id | 50400 | 50499 | -0.2 | 51523 |
| walmart.orders | buyerId | 21549 | 21719 | -0.78 | 51523 |
| walmart.orders | line_id | 68348 | 73262 | -6.71 | 73262 |
| walmart.orders | tracking_number | 52956 | 48629 | 8.9 | 73262 |

#### Bảng `schema_evolution`

amazon.orders đổi schema từ export_date 2026-04-10: cột `proceeds` null ở partition cũ và các cột chỉ có ở v0 null ở partition mới — do tên khoá đổi, không phải vì có field mới.

| period | export_dates | rows | pct_null_proceeds | pct_null_IsPrime | pct_null_AmazonOrderId |
|---|---|---|---|---|---|
| before (Orders v0) | 99 | 247213 | 100.0 | 0.0 | 0.0 |
| after (Orders 2026) | 81 | 261014 | 0.0 | 100.0 | 100.0 |

#### Bảng `schema_evolution_daily`

Tỷ lệ null theo export_date quanh ngày đổi schema 2026-04-10.

| export_date | rows | pct_null_proceeds | pct_null_IsPrime |
|---|---|---|---|
| 2026-04-07 | 2786 | 100.0 | 0.0 |
| 2026-04-08 | 2632 | 100.0 | 0.0 |
| 2026-04-09 | 2819 | 100.0 | 0.0 |
| 2026-04-10 | 2688 | 0.0 | 100.0 |
| 2026-04-11 | 2676 | 0.0 | 100.0 |
| 2026-04-12 | 2707 | 0.0 | 100.0 |

#### Bảng `duplicates`

Tỷ lệ trùng = 1 - count(distinct khóa) / count(*). Dedup giữ bản cập nhật mới nhất theo khóa. Walmart: mỗi lần kéo có `fetched_at` riêng, nên bản trùng nào cũng thành bản cũ hơn.

| dataset | rows_before | distinct_keys | dup_pct_before | identical_copies | older_versions | rows_after | dup_pct_after | dup_pct_truth |
|---|---|---|---|---|---|---|---|---|
| amazon.orders | 508227 | 498156 | 1.98 | 5108 | 4963 | 498156 | 0.0 | 1.98 |
| walmart.orders | 51523 | 50499 | 1.99 | 0 | 1024 | 50499 | 0.0 | 1.99 |

#### Bảng `volume`

Dung lượng sau khi lưu, theo nơi lưu và định dạng.

Số `files` dưới đây đo trước khi mỗi ngày mô phỏng có file riêng; nay mỗi thư mục
`export_date=` có nhiều file part nên số file lớn hơn khoảng một bậc, còn `size_mb`
gần như không đổi. Chạy lại `uv run python -m generator report` để lấy số mới.

| dataset | storage | format | files | size_mb |
|---|---|---|---|---|
| amazon.orders | object store | JSON Lines | 180 | 388.02 |
| walmart.orders | PostgreSQL (trung chuyển) | JSON Lines | 180 | 59.66 |
| seller.skus | PostgreSQL (trung chuyển) | JSON Lines | 1 | 0.22 |
| labels | object store | Parquet | 1 | 13.26 |
| order_line_keys | object store | Parquet | 2 | 21.72 |
| walmart.orders | PostgreSQL (bảng, gồm index) | heap, cột JSONB |  | 73.61 |
| seller.skus | PostgreSQL (bảng, gồm index) | heap |  | 0.31 |

## Trên AWS (2026-10-27 đến 2026-10-28)

Generator chạy trên EC2, batch lưu S3/PostgreSQL, stream phát Kafka Compose.
[Receipt batch](../../evidence/aws/data-platform-aws-20261027.json) ghi 141.774.479.087 byte raw S3;
[manifest stream](../../evidence/aws/stream-manifest-20261028.json) ghi Amazon 399.410 và TikTok 93.554 message.

![DataHub lọc AWS S3: bốn dataset nguồn Amazon, Shopee, Shopify và TikTok Shop ở source-exports](../../evidence/aws/datahub/s3-sources.png)

![Flink dedup_tiktok_state_proof: cạnh HASH tới Deduplicate, source gửi 93.554 record, job FINISHED](../../evidence/aws/flink-dedup-graph-20261028.png)

Ảnh metadata nguồn S3 và Flink nhận record xác nhận các phần của đường đi dữ liệu;
số đặc tính AWS và giới hạn được ghi tại [batch AWS](data_quality_report_batch.md#trên-aws-2026-10-27)
và [stream AWS](data_quality_report_stream.md#trên-aws-2026-10-28).
[Code generator](../../../generator/__main__.py), [batch config](../../../configs/scale-100gb.yaml), [stream config](../../../configs/default.yaml).
Chưa có report AWS riêng về skew/cardinality/schema/duplicate batch; không dùng histogram local thay capture cloud.
