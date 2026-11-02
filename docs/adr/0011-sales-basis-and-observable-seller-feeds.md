---
status: accepted
---

# Sales sau partial cancellation và nguồn Seller quan sát được

Ngày 2026-10-18, sau đề xuất Q60/Q61 người dùng yêu cầu “Hiểu, xử lý”.
Quyết định bổ sung phần partial cancellation của D16 và nguồn cho label v2;
không đổi Q56/Q57, ngân sách, quyền tư vấn hoặc quy tắc Cloud Q33.

Q60: default sales/trend là tiền hàng sau discount trừ phần hủy trước ship.
Giữ original ordered, basis sau source correction, canceled và product
Refund riêng theo cùng currency/cutoff. Line3×100, hủy1, giao2, Refund1:
ordered300/canceled100/headline200/product Refund100. Refund không tự giảm
headline lần nữa. Đây không phải tiền thu, accounting revenue hoặc profit.
Snapshot giảm quantity không đủ chứng minh cancellation.

Q61: xuất lát nhỏ nguồn Seller OMS/WMS/carrier/post-sales giả lập, authority
theo scope/pool và bridge native ID tường minh. Nguồn đi qua Bronze/Silver,
giữ observation delay/coverage; không lấy ground truth ẩn làm feature,
không giả fields native Orders hoặc tích hợp API thật. Thiếu capability
vẫn unknown/unsupported; không nâng mọi pool Channel thành quan sát đủ.

Trade-off: thêm feed/schema/reconciliation và tail làm tăng dữ liệu/POC,
đổi lại có nguồn quan sát kiểm chứng được cho quantity/money/label v2.
Không chọn suy delivery/refund từ trạng thái Order cuối vì không chứng minh
được lịch sử và linkage. [Source contract](../edai-1/reference/source_contract_order_lifecycle.md),
[publication contract](../edai-1/reference/run_publication_contract.md) và
readiness quy định phần còn phải thực hiện.
