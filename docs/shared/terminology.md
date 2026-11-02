# Data & AI Platform Multi-channel Seller Data Platform

Nền tảng dữ liệu phía seller, gom dữ liệu bán hàng từ nhiều nền tảng thương mại điện tử bên ngoài (Amazon, TikTok Shop, Shopify, Walmart, Shopee) vào một lakehouse. Dữ liệu hoàn toàn được sinh giả lập, lấy Amazon làm mô hình tham chiếu.

Q52–Q55 (2026-10-18) mở rộng nguồn partial lifecycle, Listing history,
inventory nhỏ và hàng đợi kiểm tra. Thuật ngữ inventory bổ sung ở
[GLOSSARY.md](../../GLOSSARY.md). Q56–Q59 đã chốt label v2, pool topology và
queue ba nhóm. Định nghĩa hiện hành theo [contract nhãn](../edai-1/reference/return_label_contract.md)
và thiết kế mở rộng; code/proof cũ còn theo
contract ban đầu tới khi được migrate có version.

## Language

**Channel**:
Một nền tảng bán hàng bên ngoài mà hệ thống thu thập dữ liệu từ đó (Amazon, TikTok Shop, Shopify, Walmart, Shopee).
_Avoid_: Platform, sàn, source, store

**Marketplace**:
Một storefront theo quốc gia bên trong một Channel bán theo quốc gia, ví dụ Amazon DE. Không phải Channel nào cũng có (TikTok Shop không bán theo Marketplace).
_Avoid_: Site, region, store, country

**Storefront**:
Sự hiện diện bán hàng cụ thể của Seller trên một Channel, gồm Channel và, nếu Channel đó có, cả Marketplace. Ví dụ: Amazon DE, TikTok Shop, một store Shopify.
_Avoid_: Shop, store, site

**Seller**:
Merchant duy nhất sở hữu mọi Storefront mà hệ thống phục vụ.
_Avoid_: Vendor, merchant, tenant, brand

### Sản phẩm

**SKU**:
Mã sản phẩm do chính Seller đặt, là khóa chuẩn xuyên Channel.
_Avoid_: Product, item, ASIN, variant

**Listing**:
Bản đăng bán của một SKU trên một Storefront, mang giá và trạng thái riêng (Amazon US có ASIN, Shopify có variant ID). Một SKU có nhiều Listing.
_Avoid_: Offer, product page

### Đơn hàng

**Order**:
Đơn vị mà Channel giao cho Seller xử lý (Amazon `AmazonOrderId`, Walmart `purchaseOrderId`, một đơn Shopify, TikTok Shop hay Shopee). Gồm một hoặc nhiều Order Line.
_Avoid_: Purchase, transaction, customer order

**Order Line**:
Một sản phẩm với số lượng cụ thể bên trong một Order.
_Avoid_: Item, line item, order item

**Order Status**:
Trạng thái chuẩn của Order, độc lập với Channel: `pending_payment` → `awaiting_shipment` → `shipped` → `delivered` → `completed`, cùng hai nhánh phụ `cancelled` và `returned`. Trạng thái gốc của Channel được giữ riêng và ánh xạ sang trạng thái chuẩn.
_Avoid_: State, fulfillment status, financial status

**Return**:
Buyer gửi trả một Order Line sau khi đã giao.
_Avoid_: RMA

**Refund**:
Tiền được hoàn cho buyer cho một Order Line, có hoặc không kèm Return.
_Avoid_: Chargeback, reimbursement

**Fulfillment Method**:
Bên chịu trách nhiệm giao hàng cho một Order Line: Seller tự giao hay Channel giao (như FBM và FBA của Amazon). Áp dụng cho mọi Channel.
_Avoid_: Shipping type, logistics mode

### Nhãn dự đoán

**Return Label**:
Nhãn v2 của một Order Line, dương nếu có yêu cầu Return đã được chấp nhận hoặc khoản Refund tiền hàng thành công trong 30 ngày từ lần giao phần hàng liên quan. Positive/negative chính thức cùng chờ chốt giao/hủy, đủ maturity và coverage; line chưa giao không có nhãn, thiếu dữ liệu là unknown.
_Avoid_: Churn, target

**Delivered At**:
Thời điểm phần hàng thực sự được giao cho buyer; một Order Line giao nhiều đợt có nhiều mốc tương ứng. Nhãn v2 xét cửa sổ riêng từng phần giao, còn cohort của line dùng ngày giao cuối phần thực giao khi lượng còn lại đã giao/hủy; độ phủ nguồn phải được công bố rõ.
_Avoid_: Delivery date, arrival date

**SKU Defect Propensity**:
Xu hướng bị trả của một SKU do chính sản phẩm gây ra (chật, lỗi, không đúng mô tả), giống nhau ở mọi Storefront. Chỉ generator biết, không xuất hiện thành cột dữ liệu, chỉ suy ra được từ lịch sử Return.
_Avoid_: Quality score, defect rate

### Đường thời gian dữ liệu

**Cutoff**:
Mốc "hôm nay" trong thế giới giả lập. Mọi thứ xảy ra trước Cutoff thuộc Batch, mọi thứ xảy ra sau thuộc Stream.
_Avoid_: Snapshot date, split date

**Export Date**:
Ngày Channel xuất một lô dữ liệu cho Seller. Dữ liệu được chia partition theo mốc này chứ không theo ngày tạo Order, vì export chồng lấn và thay đổi schema đều gắn với ngày export. Một Order có thể xuất hiện ở nhiều Export Date.
_Avoid_: Load date, ingestion date, partition date

**Sale Event**:
Đợt khuyến mãi làm lượng Order, mức giảm giá và phân phối SKU đổi đột ngột ở một hoặc vài Storefront, ví dụ Prime Day của Amazon. Nguồn của covariate drift.
_Avoid_: Promotion, campaign, flash sale

**Batch**:
Lịch sử của mọi Channel tính đến Cutoff, mỗi Channel ở dạng gốc của nó, nhận một lần.
_Avoid_: Backfill, historical load

**Stream**:
Chuỗi sự kiện xảy ra sau Cutoff trong cùng thế giới giả lập với Batch: Order mới, đổi Order Status của Order đã có trong Batch, và Return đến muộn.
_Avoid_: Replay, live feed

### Tầng dữ liệu (Medallion)

**Bronze**:
Bản sao trung thực của dữ liệu Channel giao cho Seller, chỉ đổi sang định dạng cột và thêm cột kỹ thuật (Export Date, tên file nguồn, thời điểm nạp). Không sửa tên cột, không gộp schema, không bỏ bản trùng. Nhiều dạng schema của cùng feed Orders trong một Channel nằm chung, việc quy tên khoá thuộc Silver. Q52–Q54 bổ sung các feed khác nên nguồn gốc phải phân biệt cả Channel và feed (Orders, Shipment, Return, Inventory, Listing); không hiểu quy tắc Orders cũ là mọi feed của Channel chỉ có một bảng. Bảng ở tầng này mang tiền tố `raw_`.
_Avoid_: Raw, landing, staging

**Silver**:
Dữ liệu đã được làm sạch và quy về ngôn ngữ chung của hệ thống: chuẩn hóa schema theo feed, ánh xạ trạng thái và khử duplicate theo identity/version. Phân biệt projection hiện hành với lịch sử quan sát phục vụ as-of; grain theo thực thể hoặc event (Order, Order Line, Shipment, Return, Inventory...), không xóa lịch sử chỉ để giữ latest. Bảng ở tầng này mang tiền tố `stg_`.
_Avoid_: Cleansed, conformed, curated

**Gold**:
Bảng phục vụ một câu hỏi nghiệp vụ cụ thể, đã tổng hợp hoặc ghép sẵn: ví dụ doanh thu theo Storefront theo ngày, tỷ lệ Return theo SKU, hay bảng feature cho Return Label. Người dùng cuối và mô hình đọc tầng này, không đọc Silver. Bảng ở tầng này mang tiền tố theo vai: `dim_` cho bảng chiều, `fact_` cho bảng sự kiện, `feat_` cho bảng feature, `obt_` cho bảng gộp phẳng một mục đích.
_Avoid_: Mart, serving layer, aggregate
