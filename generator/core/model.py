"""Các đối tượng "sự thật" mà lõi mô phỏng sinh ra.

Lõi không biết Channel trông ra sao. Nó chỉ biết:
- Order được tạo lúc nào, ở Storefront nào, gồm những Order Line nào.
- Order đi qua các Order Status chuẩn vào những thời điểm nào (`timeline`).
- Order Line nào bị trả, trả lúc nào, và nhãn thật là gì.

Renderer của từng Channel đọc các đối tượng này và "vẽ" chúng thành dạng gốc.
Hàm quan trọng nhất cho renderer là `Order.status_at(t)`: trạng thái của Order
tại một thời điểm bất kỳ, nhờ đó cùng một Order có thể được xuất ở nhiều
thời điểm khác nhau (bản export cũ, bản export mới, ảnh chụp tại cutoff).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

# Order Status chuẩn, theo thứ tự tiến triển (xem docs/shared/domain.md, mục 4).
PENDING_PAYMENT = "pending_payment"
AWAITING_SHIPMENT = "awaiting_shipment"
SHIPPED = "shipped"
DELIVERED = "delivered"
COMPLETED = "completed"
CANCELLED = "cancelled"

# Fulfillment Method chuẩn: ai giao hàng.
FULFILLED_BY_CHANNEL = "channel"  # Amazon FBA, Walmart WFS, ...
FULFILLED_BY_SELLER = "seller"  # Amazon MFN, Seller tự giao


@dataclass(slots=True)
class Buyer:
    """Người mua, lõi biết đầy đủ. Renderer chỉ lộ phần Channel thật sự lộ."""

    buyer_id: str
    country: str
    city: str
    postal_code: str
    is_guest: bool


@dataclass(slots=True)
class Sku:
    """Một SKU trong danh mục của Seller.

    `defect_propensity` là "độ lỗi" ẩn: cộng thẳng vào logit xác suất trả.
    Nó không bao giờ được ghi ra dữ liệu Channel, chỉ suy ra được từ lịch sử trả hàng.
    """

    sku_id: str
    name: str
    category: str
    base_price_usd: float
    unit_cost_usd: float
    defect_propensity: float


@dataclass(slots=True)
class OrderLine:
    """Một dòng sản phẩm trong Order.

    Tiền (`unit_price`, `unit_discount`) tính theo tiền tệ gốc của Storefront.
    `return_requested_at` là None nếu dòng không bị trả.
    `label` là None khi chưa từng giao (Order bị hủy), còn lại là 0 hoặc 1.
    """

    order_line_id: str
    line_number: int
    sku_id: str
    quantity: int
    unit_price: float
    unit_discount: float
    return_requested_at: datetime | None = None
    label: int | None = None


@dataclass(slots=True)
class StatusChange:
    """Order chuyển sang `status` vào lúc `at`."""

    at: datetime
    status: str


@dataclass(slots=True)
class Order:
    """Một Order và toàn bộ vòng đời của nó.

    `timeline` luôn sắp theo thời gian và bắt đầu bằng `pending_payment`.
    `promised_delivery` là cửa sổ ngày giao dự kiến Channel báo lúc đặt hàng,
    `delivered_at` là ngày giao thật (None nếu bị hủy). Hai giá trị này khác nhau,
    vì Amazon chỉ cho Seller thấy ngày dự kiến (xem generator-design.md, mục 4).
    """

    order_id: str
    storefront_id: str
    currency: str
    buyer: Buyer
    created_at: datetime
    fulfillment: str
    is_prime: bool
    promised_delivery: tuple[datetime, datetime]
    delivered_at: datetime | None
    timeline: list[StatusChange] = field(default_factory=list)
    lines: list[OrderLine] = field(default_factory=list)

    def status_at(self, t: datetime) -> StatusChange:
        """Trả về lần đổi trạng thái gần nhất tính đến thời điểm `t`.

        Ví dụ: Order tạo ngày 1, giao ngày 5. Hỏi trạng thái ngày 3 sẽ ra `shipped`.
        Gọi với `t` trước lúc tạo Order là lỗi, vì khi đó Order chưa tồn tại.
        """
        if t < self.created_at:
            raise ValueError(f"Order {self.order_id} chưa tồn tại tại {t}")
        current = self.timeline[0]
        for change in self.timeline[1:]:
            if change.at > t:
                break
            current = change
        return current

    @property
    def shipped_at(self) -> datetime | None:
        """Thời điểm giao cho đơn vị vận chuyển, None nếu chưa bao giờ gửi."""
        return next((c.at for c in self.timeline if c.status == SHIPPED), None)
