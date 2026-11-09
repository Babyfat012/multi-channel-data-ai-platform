# Data & AI Platform

Các định nghĩa nền về Seller, SKU, Listing, Storefront, Order Line và Return
Label tiếp tục dùng [thuật ngữ Data & AI Platform](docs/shared/terminology.md). Các mục dưới đây bổ
sung ngôn ngữ cho phần mở rộng phân tích phía Seller.

## Language

**Data Platform**:
Phần hệ thống cung cấp dữ liệu bán hàng đã chuẩn hóa, có kiểm tra chất lượng và truy vết để phân tích và làm đầu vào cho ML Platform, LLM Platform.
_Avoid_: DE, Data Engineering (khi dùng làm tên phần của hệ thống)

**ML Platform**:
Phần hệ thống quản lý feature và vòng đời mô hình dự đoán, từ huấn luyện đến phục vụ và giám sát.
_Avoid_: MLE, Feature + ML platform

**LLM Platform**:
Phần hệ thống cung cấp hội thoại và hỗ trợ Seller bằng LLM, truy xuất tài liệu và các agent sử dụng dữ liệu, mô hình.
_Avoid_: LLM/Agent platform, tầng 3

**SKU Insight**:
Thông tin phân tích về tình hình của một SKU, giúp Seller nhận biết kết quả
và những thay đổi đáng chú ý khi xem SKU đó.
_Avoid_: Product recommendation, dự đoán trả hàng

**Seller Recommendation**:
Gợi ý hành động dành cho Seller dựa trên dữ liệu hoặc thông tin nghiệp vụ
liên quan; phân biệt với gợi ý sản phẩm cho người mua.
_Avoid_: Product recommendation, kết luận nguyên nhân

**Giá trị hàng đặt**:
Giá trị tiền hàng sau discount, theo tiền gốc. Q60: headline sales trừ phần
hủy trước ship; ordered, canceled và product Refund là measures riêng.
Đặt300/hủy100/Refund100 cho headline200. Chỉ số không xác nhận tiền đã thu.
_Avoid_: Lợi nhuận, doanh thu thực thu, paid GMV

**Return** (trả hàng):
Khách **gửi hàng về** cho Seller (yêu cầu được chấp nhận). Thường kèm hoàn tiền nhưng không bắt buộc:
đổi hàng, voucher/store credit, hoặc hàng gửi về bị từ chối sau kiểm thì có Return mà không có Refund.
_Avoid_: Coi Return là đồng nghĩa với hoàn tiền

**Refund** (hoàn tiền):
**Tiền về** cho khách. Có thể đi kèm Return, hoặc **refund-only** (returnless refund: khách giữ hàng).
Chỉ Refund thành phần sản phẩm (`component = product`) mới liên quan nhãn; shipping/tax/fee/goodwill thì không.
_Avoid_: Coi mọi Refund là có hàng gửi về

**Ba ca hậu mãi** (`positive_kind` của nhãn v2):
`return_and_refund` (hàng về + tiền về), `refund_only` (tiền về, khách giữ hàng), `return_only` (hàng về,
không có tiền về). Return và Refund là **hai sự kiện độc lập**; "đi cùng nhau" = cùng Order Line và cùng nằm
trong cửa sổ 30 ngày, chưa bắt buộc nối theo `return_id` (không phải Channel nào cũng gửi liên kết đó).

**Tỷ lệ Return-hoặc-Refund lịch sử** (`return_or_refund_rate_90d`; tên cũ `return_rate_90d`):
Tỷ lệ Order Line có nhãn v2 dương trong nhóm đã chốt giao/hủy, đủ maturity
của mọi phần giao và đủ coverage tại cutoff. Luôn đi kèm tử/mẫu số, cohort
theo lần giao cuối phần thực giao và phiên bản nhãn. Đếm cả refund-only, nên **không** phải tỷ lệ hàng
vật lý bị gửi về; muốn tách thì lọc theo `positive_kind`.
_Avoid_: Xác suất trả hàng dự đoán, tỷ lệ trên mọi đơn mới đặt, "tỷ lệ trả hàng" (thiếu chữ Refund)

**Điểm rủi ro trả hàng**:
Tín hiệu xếp hạng giúp Seller biết SKU/Listing nào nên kiểm tra trước, suy ra
tại thời điểm đặt đơn với điều kiện đơn sẽ được giao. Là thứ hạng kèm căn cứ,
không phải xác suất vô điều kiện cho mọi đơn mới.
_Avoid_: Xác suất trả hàng, dự báo chắc chắn

**Chế độ xem trực tiếp**:
Trạng thái hoạt động gần đây lấy từ Stream (đơn mới, đổi Order Status, Return
đến muộn), tách biệt với số liệu chính thức theo ngày.
_Avoid_: Số liệu chính thức, báo cáo chốt

**Số liệu chính thức theo ngày**:
Con số chốt theo ngày hoàn chỉnh, gắn nhãn thời điểm dữ liệu và phiên bản
công bố. Khi có sửa đổi hợp lệ, lần công bố mới cập nhật số hiện hành và
giữ phiên bản cũ để truy lại.
_Avoid_: Live, real-time

**Phiên bản công bố**:
Một lần công bố dữ liệu đã qua kiểm tra, gồm mọi đầu ra bắt buộc của cùng một
lần refresh; yêu cầu phục vụ ghim vào đúng một phiên bản để không trộn số.
_Avoid_: Snapshot, deployment, release

**Phiên bản dataset**:
Một lần sinh dữ liệu giả lập cố định (catalog, cutoff, cấu hình, lịch sử),
được định danh để proof, feature và model trỏ về đúng một bộ dữ liệu tái lập.
_Avoid_: Snapshot ngày, data version ẩn

**Nguồn Seller operational giả lập**:
Feed OMS/WMS/carrier/post-sales được simulator xuất riêng theo Q61, có
authority, liên kết native identity và thời điểm quan sát; đi qua Bronze/Silver.
Không làm mọi Channel/pool có coverage đầy đủ.
_Avoid_: API Channel thật, ground truth dùng trực tiếp làm feature

**Inventory Pool**:
Nhóm tồn vật lý của Seller được dùng cấp hàng cho một hoặc nhiều Storefront.
Nhiều Storefront cùng sử dụng một pool không làm tăng lượng tồn vật lý của nó.
_Avoid_: Tồn của Channel, tổng tồn theo Storefront

**Location**:
Địa điểm giữ hoặc xử lý hàng của một Inventory Pool, như kho Seller hoặc kho
do Channel vận hành.
_Avoid_: Storefront, Marketplace, địa chỉ buyer

**Reservation**:
Phần số lượng của một SKU trong pool được giữ cho một nhu cầu cụ thể và chưa
được giải phóng hoặc xuất giao.
_Avoid_: Đã bán, đã giao, tồn vật lý giảm

**Inbound**:
Lượng hàng dự kiến đi vào pool/location nhưng chưa được xác nhận nhận thực tế.
_Avoid_: Tồn sẵn bán, hàng đã nhận

**Hàng đợi kiểm tra SKU**:
Danh sách SKU trong từng Storefront cần Seller xem xét, kèm lý do, độ phủ
và bằng chứng để drilldown. Ba nhóm return-risk, inventory/Listing
availability và biến động sales có thứ tự ưu tiên riêng, không tự thực hiện hành động bán hàng.
_Avoid_: Danh sách hủy đơn, sản phẩm đề xuất cho buyer, xác suất thiệt hại
