# Nguồn và mức tin cậy của dạng dữ liệu từng Channel

Tài liệu này ghi lại generator dựa vào tài liệu nào để mô hình hóa dạng dữ liệu gốc của từng Channel, field nào đã xác minh, field nào chỉ là giả định. Research thực hiện ngày 2026-09-21.

## Mục lục

1. [Cách đọc mức tin cậy](#1-cách-đọc-mức-tin-cậy)
2. [Amazon](#2-amazon)
3. [Shopify](#3-shopify)
4. [Walmart](#4-walmart)
5. [TikTok Shop](#5-tiktok-shop)
6. [Shopee](#6-shopee)
7. [Hệ quả cho generator](#7-hệ-quả-cho-generator)

## 1. Cách đọc mức tin cậy

| Mức | Nghĩa |
|---|---|
| **Cao** | Đọc trực tiếp từ trang tài liệu hoặc model chính thức của Channel trong phiên research |
| **Trung bình** | Từ nguồn thứ cấp (SDK mã nguồn mở, blog, tóm tắt kết quả tìm kiếm), chưa đối chiếu được với trang chính thức |
| **Thấp** | Suy luận hoặc nhớ từ trước, chưa có nguồn trong phiên research. Không dùng để mô hình hóa cho tới khi xác minh |

Nguyên tắc: generator chỉ mô hình hóa field ở mức Cao hoặc Trung bình. Mọi field mức Trung bình được đánh dấu `assumption` trong config và trong manifest.

## 2. Amazon

**Orders v0.** Nguồn: model chính thức `ordersV0.json` trong repo `amzn/selling-partner-api-models`. Mức: **Cao**.

- Order: `AmazonOrderId` (định dạng 3-7-7), `PurchaseDate`, `LastUpdateDate`, `OrderStatus`, `FulfillmentChannel` (`AFN` hoặc `MFN`), `NumberOfItemsShipped`, `NumberOfItemsUnshipped`, `BuyerInfo`, `ShippingAddress`, `IsReplacementOrder`, `IsPrime`, `IsBusinessOrder`.
- `OrderStatus`: `PendingAvailability`, `Pending`, `Unshipped`, `PartiallyShipped`, `Shipped`, `InvoiceUnconfirmed`, `Canceled`, `Unfulfillable`.
- OrderItem: `OrderItemId`, `ASIN`, `SellerSKU`, `Title`, `QuantityOrdered`, `QuantityShipped`, `ItemPrice`, `ItemTax`, `PromotionDiscount`.

**Orders 2026-01-01** (phiên bản mới, cùng repo). Mức: **Cao**.

- Hai thao tác: `searchOrders` và `getOrder`. Định danh và thời gian: `orderId`, `createdTime`, `lastUpdatedTime`.
- Trạng thái fulfillment: `PENDING_AVAILABILITY`, `PENDING`, `UNSHIPPED`, `PARTIALLY_SHIPPED`, `SHIPPED`, `CANCELLED`, `UNFULFILLABLE`.
- Có `fulfillment.shipByWindow`, `fulfillment.deliverByWindow` (cửa sổ dự kiến), `packages`, `orderItems[].product` (ASIN, SKU), `proceeds`, `promotion`, `cancellation`.

**Dùng cho schema evolution.** Dạng 2026 **không có field mới** — mọi khoá của nó đều truy được về một khoá v0, chỉ đổi tên, lồng cấp, đổi kiểu hoặc bị bỏ. Đổi tên: `AmazonOrderId` thành `orderId`, `PurchaseDate` thành `createdTime`, `LastUpdateDate` thành `lastUpdatedTime`, và `OrderTotal` thành `proceeds.grandTotal` (lồng thêm một cấp). Đổi cấu trúc và cách viết giá trị: `OrderStatus: "Shipped"` thành `fulfillment.status: "SHIPPED"`. Đổi kiểu: `IsPrime` (bool) thành mảng `programs`. Bỏ khoá (chỉ có ở v0): `NumberOfItemsShipped`, `NumberOfItemsUnshipped`, `IsBusinessOrder`, `IsReplacementOrder`, `OrderItems[].QuantityShipped`. Ngày chuyển phiên bản trong generator là ngày tự chọn, không phải lịch ngừng hỗ trợ thật của Amazon (chưa xác minh lịch đó).

**Notification.** Generator giả định Amazon gửi thông báo đổi trạng thái đơn và thông báo hay đến trễ. Payload và cách gửi chưa đọc từ tài liệu chính thức. Mức: **Thấp**, ghi là giả định.

**Khoảng trống quan trọng:** cả hai phiên bản Orders chỉ có ngày giao **dự kiến**, không có ngày giao **thực tế** và không có trạng thái "đã giao". Đã tìm thêm ở các nguồn khác:

- **Đơn Seller tự giao (MFN):** `EarliestDeliveryDate` và `LatestDeliveryDate` trong Orders, là ngày dự kiến. Mức: **Trung bình** (thảo luận trong repo chính thức, người bảo trì xác nhận "không phải ngày giao thực tế").
- **Đơn FBA:** report `GET_AMAZON_FULFILLED_SHIPMENTS_DATA_GENERAL` có `shipment-date`, `estimated-arrival-date`, `carrier`, `tracking-number`. Mức: **Cao** (trang Reports chính thức), ngày vẫn là dự kiến. Report này bị hạn chế, cần Restricted Data Token, lấy tối đa 1 tháng mỗi lần.
- **Tracking thực tế:** `getTracking` chỉ dùng cho kiện mua qua Amazon Buy Shipping, giữ tối đa 45 ngày. Mức: **Trung bình** (kết quả tìm kiếm). Không áp dụng cho FBA và cho Seller tự dùng carrier ngoài.

Kết luận: SP-API không cung cấp ngày giao thực tế cho mọi đơn. Xem mục 7 cho cách generator xử lý.

**Report đơn hàng.** Nguồn: trang FBA Reports chính thức. Mức: **Cao**. `GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL` (đơn có cập nhật trong khoảng ngày, dùng theo dõi đơn) và `GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL` (đơn đặt trong khoảng ngày, mọi trạng thái). Bản "by last update" là nguồn tự nhiên cho export chồng lấn ở kịch bản duplicate offline.

**Returns report.** Nguồn: trang Returns Reports chính thức. Mức: **Cao**.

- `GET_FLAT_FILE_RETURNS_DATA_BY_RETURN_DATE` (TSV): `Order ID`, `Order date`, `Return request date`, `Amazon RMA ID`, `ASIN`, `Merchant SKU`, `Item Name`, `Return quantity`, `Return Reason`, `Refunded Amount`. Lấy tối đa 60 ngày mỗi lần yêu cầu. Có bản XML tương đương (`GET_XML_RETURNS_DATA_BY_RETURN_DATE`) với thêm `return_reason_code` và `label_details` (`tracking_id`, `return_carrier`, `label_cost`).
- `GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA`: hàng trả đã về kho FBA, có `order-id`, `reason`, `disposition`. Cập nhật hàng ngày. Danh sách cột đầy đủ chưa có.
- Còn có report cho Seller Fulfilled Prime (`GET_CSV_MFN_PRIME_RETURNS_REPORT`, `GET_XML_MFN_PRIME_RETURNS_REPORT`), không dùng cho dự án.
- Cả hai nguồn chính có ngày yêu cầu trả, không có ngày hàng về tới Seller, nên `Return request date` là mốc của nhãn.
- Generator dùng report flat file cho đơn MFN và report FBA Returns cho đơn FBA. Việc chia như vậy dựa trên suy luận: report flat file có RMA và nhãn trả hàng trả trước, là đặc điểm của đơn Seller tự giao. Mức: **Thấp**, ghi là giả định.

## 3. Shopify

**Order.** Nguồn: trang tài liệu chính thức Admin REST API, resource Order. Mức: **Cao**.

- Order: `id`, `name`, `email`, `created_at`, `updated_at`, `closed_at`, `cancelled_at`, `cancel_reason`, `customer`, `shipping_address`, `total_price`, `currency`, `presentment_currency`.
- `financial_status`: `pending`, `authorized`, `partially_paid`, `paid`, `partially_refunded`, `refunded`, `voided`.
- `fulfillment_status`: `fulfilled`, `partial`, `not_eligible`, `restocked`, `null`.
- Line item: `id`, `sku`, `variant_id`, `product_id`, `quantity`, `current_quantity`, `price`, `price_set`, `total_discount`, `fulfillment_status`.
- Refund: `id`, `order_id`, `created_at`, `processed_at`, `refund_line_items`, `transactions`.

**REST là legacy.** Nguồn: changelog chính thức của Shopify, qua kết quả tìm kiếm. Mức: **Cao** về sự kiện, **Trung bình** về chi tiết. REST Admin API được đánh dấu legacy từ 2024-10-01. Từ 2025-04-01 mọi ứng dụng công khai mới phải dùng GraphQL. Ứng dụng đang có vẫn được dùng REST, lộ trình di chuyển chưa thấy nêu. Chưa xác minh tình trạng năm 2026.

Không có "đã giao" trong Order. Trạng thái giao hàng nằm ở object fulfillment riêng. Generator giả định Order có danh sách `fulfillments` mang trạng thái giao hàng. Tên field chưa đọc từ tài liệu chính thức, mức **Thấp**.

Webhook trùng: có thảo luận cộng đồng về việc Shopify gọi webhook nhiều lần cho một đơn. Mức **Trung bình**. Mã định danh của mỗi lần gửi webhook (để dedup) chưa xác minh, mức **Thấp**.

**Buyer.** `customer` có thể null với khách không đăng nhập.

**Renderer Batch trong repo (2026-10-13).** `generator/channels/shopify.py` xuất một
response REST `order` trên mỗi dòng JSONL, gồm `line_items` và `customer=null` cho
khách guest. Nó giữ `price` trước giảm giá và `total_discount` của từng dòng;
`fulfillment_status` chỉ nói hàng đã được fulfillment, không suy ra ngày giao
thật. Dạng REST được chọn cho bộ dữ liệu giả lập hiện có; khi nối ứng dụng Shopify
thật, cần xem lại yêu cầu GraphQL theo phiên bản API đang dùng.
Source export nhỏ ở `out/channels-small` đã qua DP1 Bronze với 910 response.

Đối chiếu với [Shopify Order resource](https://shopify.dev/docs/api/admin-rest/latest/resources/order).

## 4. Walmart

**Order.** Nguồn: trang tài liệu chính thức Walmart Marketplace (Get all orders, Order management event). Mức: **Cao**.

- Order: `purchaseOrderId`, `customerOrderId`, `customerEmailId`, `orderDate` (Unix epoch), `buyerId`, `isGuest`, `orderType` (`REGULAR`, `REPLACEMENT`, `PREORDER`), `shippingInfo`.
- Order line: `orderLines.orderLine[]` với `lineNumber`, `item` (`productName`, `sku`, ...), `charges` (`chargeType`: `PRODUCT` hoặc `SHIPPING`, `chargeAmount`: `{currency, amount}`, `tax`), `orderLineQuantity`, `orderLineStatuses`, `statusDate`.
- Status: `Created`, `Acknowledged`, `Shipped`, `Delivered`, `Cancelled`.
- Webhook `ORDER_UPDATES`: `source.eventType`, `source.eventTime` (ISO 8601 UTC), `source.eventId` (khóa idempotency). Sub-event: `PO_CREATED`, `PO_LINE_AUTOCANCELLED`, `INTENT_TO_CANCEL`, `FRAUD_CANCEL`.

**Return.** Nguồn: trang tài liệu chính thức "Retrieve return order details". Mức: **Cao** cho tên field, **Trung bình** cho enum (trang chỉ có bản mẫu).

- `returnOrderId`, `customerOrderId`, `returnOrderLines` (SKU, lý do, trạng thái hoàn tiền, tracking), `status`, `totalRefundAmount`, `returnOrderDate`, `returnByDate`, `refundMode`, `returnType` (ví dụ `REPLACEMENT`).
- Giá trị trạng thái thấy trong bản mẫu: `REFUND_INITIATED`, `REFUND_COMPLETED`, `NOT_SHIPPED`, `COMPLETED`. Chưa có danh sách đầy đủ.

Điều chỉnh so với thiết kế cũ: Walmart có cả webhook lẫn polling. Việc dùng polling cho Walmart là **lựa chọn thiết kế** của dự án, không phải giới hạn của Channel.

### Ranh giới tin cậy: nguồn của Walmart là PostgreSQL, không phải file JSONL

**a) Đường đi dữ liệu Walmart.** Dữ liệu Walmart đi qua ba chặng, mỗi chặng do một bên khác nhau giữ. Phân biệt được ba chặng là điều kiện để biết "nguồn" thật nằm ở đâu.

```
  file JSONL                 PostgreSQL                  Spark                  Bronze
  /data/raw/pg-load/         walmart.orders              (đọc qua JDBC)         raw_walmart_orders
  walmart.orders/            (DB phòng ban khác)                                (lakehouse.bronze,
  export_date=*/             purchase_order_id,                                 Iceberg)
  part-*.jsonl               raw_payload JSONB, fetched_at

        └───── chặng ① ─────►┘       └───── chặng ② ─────►┘      └───── chặng ③ ─────►┘
        generator load-postgres      Spark đọc, chỉ đọc về      Spark ghi ra Iceberg
```

- **Chặng ①** — `generator load-postgres` nạp file JSONL vào bảng `walmart.orders`. Đây là bước nạp vào DB của phòng ban khác, chỉ chạy một lần khi dựng nguồn.
- **Chặng ②** — Spark đọc `walmart.orders` qua JDBC. Data team **chỉ đọc về**, không sở hữu và không sửa bảng.
- **Chặng ③** — Spark ghi thành bảng Bronze `raw_walmart_orders` trên Iceberg. Đây là cái hệ thống sở hữu.

**b) Nguồn thật là chặng nào và vì sao.** Nguồn thật là **`walmart.orders` trong PostgreSQL** — ranh giới nằm ngay giữa chặng ① và chặng ②. Lý do: PostgreSQL là **DB của một phòng ban khác**, data team **không sở hữu** nó, chỉ **đọc về**. File JSONL chỉ là phương tiện nạp vào DB đó. Vì không sở hữu, data team không được đổi cách nguồn lưu, nên **ranh giới tin cậy dừng ở PostgreSQL**, không phải ở file JSONL.

**c) Hệ quả quan sát được.** Cột `raw_payload` khai kiểu `JSONB`, nên khi PostgreSQL lưu lại, chuỗi JSON bị chuẩn hoá: **thứ tự key gốc** (`purchaseOrderId`, `customerOrderId`, ...) **thành thứ tự alphabet** (`buyerId`, `isGuest`, `orderDate`, ...), và **thêm khoảng trắng sau dấu hai chấm**. **Không mất giá trị nào** — chỉ thứ tự key và khoảng trắng đổi, nội dung key/value giữ nguyên. Chuẩn JSON vốn **không quy định thứ tự key**, nên hai cách viết là cùng một JSON.

**d) Vì sao đây KHÔNG phải lỗi.** Bronze chỉ chép đúng cái mà nguồn đang giữ. Với Amazon, nguồn là **file** nên chép **nguyên từng byte**. Với Walmart, nguồn là **PostgreSQL** nên chép đúng thứ PostgreSQL đưa ra, kể cả khi JSONB đã xếp lại key. Hai bên trông khác nhau vì **nguồn khác nhau**, không phải vì một nguồn làm sai.

## 5. TikTok Shop

Nguồn: trang tài liệu chính thức `partner.tiktokshop.com`, đọc qua trình duyệt (công cụ tải trực tiếp trả về trang rỗng). Mức: **Cao** trừ khi ghi khác.

**Trạng thái đơn (API).** `UNPAID`, `ON_HOLD`, `AWAITING_SHIPMENT`, `PARTIALLY_SHIPPING`, `AWAITING_COLLECTION`, `IN_TRANSIT`, `DELIVERED`, `COMPLETED`, `CANCELLED`. Ý nghĩa đáng chú ý: `ON_HOLD` là giai đoạn buyer còn hủy được không cần seller duyệt, `COMPLETED` là khi không còn được trả hay hoàn tiền nữa.

**Get Order List** (`POST /order/202309/orders/search`). Lọc theo `order_status`, `create_time_ge`/`create_time_lt`, `update_time_ge`/`update_time_lt` (Unix timestamp, giây), `shipping_type` (`TIKTOK` hoặc `SELLER`, giống Fulfillment Method), `buyer_user_id`, `is_buyer_request_cancel`. Phân trang bằng `page_token`, tối đa 100 mỗi trang, sắp xếp theo `create_time` hoặc `update_time`.

**Get Order Detail** (`GET /order/202309/orders`). Tên field dưới đây đọc từ trang phiên bản **202309** (217 field). Trang phiên bản 202507 cũng có nhưng không mở được phần response, nên chưa biết hai phiên bản khác nhau ở đâu. Generator dùng 202309. Tên field trong `data.orders[]`:
- Cấp đơn: `id`, `status`, `user_id`, `create_time`, `paid_time`, `update_time`, `rts_time`, `rts_sla_time`, `tts_sla_time`, `delivery_sla_time`, `shipping_due_time`, `cancel_reason`, `cancellation_initiator` (`SELLER` hoặc `BUYER`), `fulfillment_type`, `delivery_type`, `shipping_provider`, `shipping_provider_id`, `tracking_number`, `buyer_message`, `seller_note`, `is_sample_order`, `warehouse_id`, `payment_method_name`, `recipient_address` (có `postal_code`, `region_code`; không có khi `UNPAID` hoặc `ON_HOLD`), `packages[]`, `line_items[]`.
- `payment`: `currency`, `sub_total`, `shipping_fee`, `seller_discount`, `platform_discount`, `total_amount`, `original_total_product_price`, `tax`, `product_tax`, `shipping_fee_tax`, ...
- `line_items[]`: `id`, `sku_id`, `product_id`, `seller_sku`, `product_name`, `sku_name`, `original_price`, `sale_price`, `seller_discount`, `platform_discount`, `display_status`, `cancel_user`, `cancel_reason`, `package_id`, `package_status`, `tracking_number`, `shipping_provider_name`, `is_gift`, `item_tax`, `sku_type`, `combined_listing_skus`.
- Không thấy field ngày giao thực tế ở cấp đơn hay dòng đơn (chỉ có các mốc SLA). Cần kiểm tra thêm ở `packages` và webhook.

**Webhook.** Envelope: `type` (số), `tts_notification_id`, `shop_id`, `timestamp` (giây), `data`.
- Type 1 `Order status change`: `data` gồm `order_id`, `order_status`, `is_on_hold_order`, `update_time`. Giá trị `order_status` trong webhook: `UNPAID`, `ON_HOLD`, `AWAITING_SHIPMENT`, `AWAITING_COLLECTION`, `CANCEL`, `IN_TRANSIT`, `DELIVERED`, `COMPLETED`.
- Type 12 `Return status change`: `data` gồm `order_id`, `return_role` (`BUYER`, `SELLER`, `SYSTEM`), `return_type` (`REFUND`, `REPLACEMENT`, `RETURN_AND_REFUND`), `return_status`, `return_id`, `create_time`, `update_time`. Giá trị `return_status` thấy được: `RETURN_OR_REFUND_REQUEST_PENDING`, `REFUND_OR_RETURN_REQUEST_REJECT`, `AWAITING_BUYER_SHIP`, `BUYER_SHIPPED_ITEM`, `RETURN_OR_REFUND_REQUEST_CANCEL`, `RETURN_OR_REFUND_REQUEST_COMPLETE`, `RETURN_OR_REFUND_REQUEST_SUCCESS`, và nhóm `REPLACEMENT_REQUEST_*`.
- Còn có webhook riêng: `Cancellation status change` (11), `Package update` (4), `Aftersales Request Status Update` (64), `RMA Status Update` (65), `Refund Success` (67). Chưa đọc chi tiết.

**Không nhất quán giữa API và webhook:** trạng thái hủy là `CANCELLED` ở API nhưng `CANCEL` ở webhook. Đây là chi tiết nên giữ nguyên trong generator, vì nó buộc lớp ánh xạ trạng thái ở Silver xử lý cả hai.

**Giao nhận webhook.** Mức: **Trung bình**, từ hướng dẫn của Hookdeck (nguồn thứ cấp). At-least-once, thử lại 4 lần (2 phút, 30 phút, 3 giờ, 12 giờ), khử trùng bằng `tts_notification_id`.

**Search Returns** (`POST /return_refund/202602/returns/search`). Nguồn Return cho Batch. Lọc theo `return_ids`, `order_ids`, `buyer_user_ids`, `return_types`, `return_status`, `create_time_ge`/`create_time_lt`, `update_time_ge`/`update_time_lt`.
- `data.return_orders[]`: `order_id`, `return_id`, `return_type`, `return_status`, `arbitration_status`, `role`, `return_reason`, `return_reason_text`, `return_method`, `return_tracking_number`, `return_provider_name`, `can_buyer_keep_item`, `is_quick_refund`, `create_time`, `update_time`, `refund_amount`, `partial_refund`, `return_line_items[]`.
- `return_line_items[]`: `return_line_item_id`, `order_line_item_id`, `sku_id`, `sku_name`, `product_name`, `seller_sku`, `refund_amount`. Có `order_line_item_id` nên nhãn gắn được tới đúng Order Line.
- `return_type`: `REFUND`, `RETURN_AND_REFUND`, `REPLACEMENT`, `EXCHANGE`.
- `return_status`: `RETURN_OR_REFUND_REQUEST_PENDING`, `REFUND_OR_RETURN_REQUEST_REJECT`, `AWAITING_BUYER_SHIP`, `BUYER_SHIPPED_ITEM`, `REJECT_RECEIVE_PACKAGE`, `RETURN_OR_REFUND_REQUEST_SUCCESS`, `RETURN_OR_REFUND_REQUEST_CANCEL`, `RETURN_OR_REFUND_REQUEST_COMPLETE`, `AWAITING_BUYER_RESPONSE`, `PARTIAL_REFUND`, nhóm `REPLACEMENT_REQUEST_*` và `EXCHANGE_REQUEST_*`.

**Phiên bản API.** Get Order Detail có hai phiên bản (202309 và 202507) nhưng chưa đọc được khác biệt, nên **không** dùng TikTok Shop cho kịch bản schema evolution. Kịch bản đó dùng Amazon Orders v0 và Orders 2026-01-01, cả hai đã có model chính thức.

**Buyer.** `user_id` ở cấp đơn là mã buyer trong phạm vi TikTok Shop, không dùng xuyên Channel được.

**Renderer Batch trong repo (2026-10-13).** `generator/channels/tiktok_batch.py` vẽ
Get Order Detail 202309 thành một response `data.orders` chứa một Order trên mỗi
dòng JSONL. Cùng một Order dùng chung ID với webhook TikTok Shop đã có. Snapshot
giữ trạng thái API `CANCELLED`; webhook vẫn giữ chữ `CANCEL`. `payment.seller_discount`
gộp toàn bộ giảm giá của lõi mô phỏng, `platform_discount=0` vì lõi chưa phân biệt
hai bên trả giảm giá. `line_items.quantity` là giả định cần đối chiếu khi mở rộng
API thực. Source export nhỏ nằm ở `out/channels-small`; DP1 đã nạp và kiểm
1.810 response vào Bronze. Silver được mô tả trong `silver-five-channels.md`.

Trang chính thức dùng để đối chiếu trạng thái và trường lọc theo thời gian:
[Get Order List 202309](https://partner.tiktokshop.com/docv2/page/get-order-list-202309).

## 6. Shopee

Nguồn: trang tài liệu chính thức `open.shopee.com`, đọc qua trình duyệt. Mức: **Cao** cho mục đọc được dưới đây.

**Trạng thái đơn** (từ `get_order_list`): `UNPAID`, `READY_TO_SHIP`, `PROCESSED`, `SHIPPED`, `COMPLETED`, `IN_CANCEL`, `CANCELLED`, `INVOICE_PENDING`. Hai trạng thái `IN_CANCEL` và `INVOICE_PENDING` không nằm trong danh sách của SDK mã nguồn mở đã dùng ở lần research trước, nên SDK đó không đủ.

**`get_order_detail`** (`response.order_list[]`):
- Cấp đơn: `order_sn`, `region`, `currency`, `cod`, `total_amount`, `order_status`, `shipping_carrier`, `payment_method`, `estimated_shipping_fee`, `actual_shipping_fee`, `message_to_seller`, `create_time`, `update_time`, `days_to_ship`, `ship_by_date`, `buyer_user_id`, `buyer_username`, `note`, `pay_time`, `dropshipper`, `split_up`, `buyer_cancel_reason`, `cancel_by` (`buyer`, `seller`, `system` hoặc `Ops`), `cancel_reason`, `recipient_address` (`zipcode`, `state`, `city`, ...), `package_list`, `item_list`.
- `item_list[]`: `item_id`, `item_name`, `item_sku`, `model_id`, `model_name`, `model_sku`, `model_quantity_purchased`, `model_original_price`, `model_discounted_price`, `order_item_id`, `promotion_type`, `promotion_id`, `wholesale`, `add_on_deal`, `flash_sale`.
- Thời gian là Unix timestamp. Không thấy ngày giao thực tế ở cấp đơn trong danh sách field đã đọc.

**`get_return_list`** (module Returns, trang chính thức). Mức: **Cao** cho tên field:
- `return` (danh sách) có `return_sn`, `reason`, `text_reason`, `refund_amount`, `currency`, `create_time`, `update_time`, `status`, `due_date`, `tracking_number`, `dispute_reason`, `needs_logistics`, `amount_before_discount`, `user` (`username`, `email`), `item[]` (`model_id`, `name`, `amount`, `images`), kèm cờ `more` để phân trang.
- Bộ lọc gồm `status` (ví dụ `REQUESTED`), `negotiation_status` (ví dụ `TERMINATED`), `seller_proof_status` (ví dụ `PENDING`).
- Giá trị của `status` nằm ở mục "Data Definition - ReturnStatus", chưa đọc được. Chỉ xác nhận `REQUESTED` và `CANCELLED` là giá trị hợp lệ. Danh sách `REQUESTED`, `PROCESSING`, `ACCEPTED`, `COMPLETED`, `CANCELLED` từ nguồn thứ cấp (mức **Trung bình**).

**Push (webhook) của Shopee: không cần.** Thiết kế của dự án giả định Shopee đến theo polling (xem [Domain.md](../../shared/domain.md)), nên payload push không cần cho generator. Push code 3 (cập nhật trạng thái đơn) chỉ từng thấy qua thảo luận nhà phát triển, mức **Thấp**, không dùng.

**Renderer Batch trong repo (2026-10-13).** `generator/channels/shopee.py` xuất
`response.order_list` gồm một Order trên mỗi dòng JSONL, có `item_list`, giá
VND dạng số nguyên và `order_item_id` để nối ground truth. Vì schema Order chưa có
ngày giao thật, renderer chiếu cả trạng thái lõi `DELIVERED` và `COMPLETED` sang
`COMPLETED`; `update_time` chỉ là lúc trạng thái hiện tại đổi, không mặc nhiên là
`delivered_at`.

Source export nhỏ của Shopee nằm ở `out/channels-small`; DP1 đã nạp và kiểm
354 response vào Bronze.

## 7. Hệ quả cho generator

1. **Cả 5 Channel đều đã có nguồn chính thức cho đơn hàng và trả hàng.** Còn thiếu: `ReturnStatus` đầy đủ của Shopee, `packages` và các webhook cancel/refund của TikTok Shop, khác biệt giữa hai phiên bản Get Order Detail của TikTok Shop, cột đầy đủ của các report FBA, danh sách trạng thái return của Walmart, notification của Amazon, fulfillment và mã webhook của Shopify. Các mục này ghi là giả định khi dùng.
2. **Ngày giao thực tế không có sẵn ở Amazon, Shopee, Shopify, TikTok Shop** trong các field đã đọc. Return Label cần ngày giao nên quy tắc chung là: lõi mô phỏng luôn có `delivered_at`, mỗi renderer thể hiện theo cách Channel thật có:
   - TikTok Shop và Walmart: trạng thái `DELIVERED` (kèm `update_time` hoặc `statusDate`).
   - Shopify: event fulfillment.
   - Amazon: chỉ có ngày dự kiến (`estimated-arrival-date` cho FBA, `EarliestDeliveryDate`/`LatestDeliveryDate` cho MFN). Generator dùng ngày dự kiến này làm mốc quan sát được, và `delivered_at` thật chỉ có trong ground truth của lõi. Đây là **giả định**: nhãn Amazon dùng ngày giao thật của lõi để tính cửa sổ 30 ngày, còn feature và pipeline chỉ thấy ngày dự kiến.
   - Shopee: chỉ có `update_time` khi sang `COMPLETED`, coi là proxy tương tự Amazon, ghi là giả định.
3. **Return có API hoặc report riêng ở mọi Channel.** Amazon: Returns report flat file (MFN) và FBA Returns report. Walmart: return orders API. TikTok Shop: Search Returns (Batch) và webhook type 12 (Stream). Shopee: `get_return_list`. Shopify: object refund trong Order (chưa có object return).
4. **Mã buyer theo Channel** (Walmart `buyerId`, Shopify `customer.id`, TikTok Shop `user_id`, Shopee `buyer_user_id`) chỉ có nghĩa trong từng Channel. Amazon không có mã buyer ổn định.
5. **Không nhất quán tên trạng thái giữa API và webhook** của cùng một Channel (TikTok Shop `CANCELLED` và `CANCEL`) được giữ nguyên trong generator.
6. **Walmart webhook có `eventId`**, nên duplicate ở Walmart có thể mô hình hóa qua `eventId` nếu sau này đổi Walmart từ polling sang webhook.
7. **Mở rộng renderer không đổi số đo của Channel cũ.** TikTok Shop, Shopify và
   Shopee dùng luồng RNG export riêng theo Order. Amazon/Walmart vẫn dùng luồng
   cũ của lần chạy 100 GB; manifest nhỏ sau khi thêm cả ba Channel có cùng
   `rows`, `unique_keys`, `rows_by_placement` và `bytes` của hai nguồn cũ như
   `out/small/run_manifest.json`. Số file khác vì bản cũ dùng cách ghi file
   trước cơ chế resume.
