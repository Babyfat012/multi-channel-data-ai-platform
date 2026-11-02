"""Bảng ánh xạ của Silver (trạng thái, fulfillment, quốc gia), không phụ thuộc pyspark.

Job Spark tra các dict này bằng `create_map`: khoá lạ ra NULL (riêng quốc gia giữ nguyên).
Để ở Python thuần thì `pytest` kiểm được mà không cần Spark. `channel_status` giữ chữ
nguồn, `order_status` là trạng thái chuẩn. Cột chỉ có một bản (`fulfillment_method`,
`country`) thì phải chuẩn hoá.
"""

from __future__ import annotations

# Trạng thái chuẩn (docs/shared/terminology.md).
PENDING_PAYMENT = "pending_payment"
AWAITING_SHIPMENT = "awaiting_shipment"
SHIPPED = "shipped"
DELIVERED = "delivered"
COMPLETED = "completed"
CANCELLED = "cancelled"

# Amazon v0 `OrderStatus`. "Shipped" gộp cả shipped/delivered/completed: Amazon không báo
# "đã giao" cho Seller.
AMAZON_V0_STATUS: dict[str, str] = {
    "Pending": PENDING_PAYMENT,
    "Unshipped": AWAITING_SHIPMENT,
    "Shipped": SHIPPED,
    "Canceled": CANCELLED,
}

# Amazon 2026 `fulfillment.status`. Bảng riêng vì "Canceled" và "CANCELLED" khác cả cách viết.
AMAZON_2026_STATUS: dict[str, str] = {
    "PENDING": PENDING_PAYMENT,
    "UNSHIPPED": AWAITING_SHIPMENT,
    "SHIPPED": SHIPPED,
    "CANCELLED": CANCELLED,
}

# Walmart "Delivered" gộp cả delivered lẫn completed. Chọn `delivered` vì luôn đúng:
# đơn completed nào cũng đã qua delivered.
WALMART_STATUS: dict[str, str] = {
    "Created": PENDING_PAYMENT,
    "Acknowledged": AWAITING_SHIPMENT,
    "Shipped": SHIPPED,
    "Delivered": DELIVERED,
    "Cancelled": CANCELLED,
}

# Bên giao hàng.
BY_CHANNEL = "channel"  # Channel giao (Amazon FBA / AFN)
BY_SELLER = "seller"  # Seller tự giao (Amazon FBM / MFN)

# v0 dùng AFN/MFN, 2026 dùng AMAZON/MERCHANT; bốn chữ không trùng nên chung một bảng.
AMAZON_FULFILLMENT: dict[str, str] = {
    "AFN": BY_CHANNEL,
    "AMAZON": BY_CHANNEL,
    "MFN": BY_SELLER,
    "MERCHANT": BY_SELLER,
}

# Walmart ghi "USA"; Amazon đã dùng mã 2 chữ.
COUNTRY_ALIASES: dict[str, str] = {"USA": "US"}
