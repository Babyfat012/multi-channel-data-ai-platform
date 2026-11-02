---
status: accepted
---

# Cửa sổ theo phần giao và Return Label phiên bản 2

Ngày 2026-10-18, người dùng đồng ý Q56–Q59. Q56/Q57 làm rõ và thay phần
ngữ nghĩa nhãn Q41 trước đó: mỗi shipment allocation có cửa sổ 30 ngày;
Order Line dương nếu có Return request đã được chấp nhận hoặc product
Refund thành công, liên kết được với phần giao và nằm trong cửa sổ đó.
Chọn cách này thay neo cả line vào lần giao đầu/cuối, vì hai cách kia có
thể bỏ hoặc tính nhầm sự kiện của một đợt giao khác.

Giữ một Order Line là một mẫu. Cohort theo lần giao cuối phần thực giao
khi lượng còn lại đã giao/hủy; train/eval chính thức chỉ dùng line đủ
maturity và coverage cho cả positive/negative. Thiếu linkage hoặc feed thì
unknown; chưa giao thì không có label. Phiên bản nhãn mới là
`return-or-refund-line-v2`; proof/model cũ không tự chuyển sang v2. Target
đo yêu cầu Return được chấp nhận hoặc hoàn tiền hàng thành công, không phải
tỷ lệ hàng vật lý cuối cùng được nhận lại.

Bổ sung 2026-10-19 (operator nêu, grilling chốt): Return và Refund là hai sự kiện độc lập (ba ca
return_and_refund / refund_only / return_only). Nhãn **giữ gộp có chủ ý** — giá: một mô hình học trên nhãn này
không tự phân biệt "khách giữ hàng" với "hàng về kho"; bù lại bằng cột `positive_kind` và tên feature
`return_or_refund_rate_90d`. Ghép Return–Refund theo `return_id` để sau, khi mọi Channel gửi liên kết.

Q58 chốt pool Seller dùng chung và pool Channel riêng, movement authority,
snapshot đối soát, return chờ kiểm trước sellable. Q59 chốt ba nhóm hàng
đợi độc lập return-risk, inventory/Listing availability và biến động sales;
không có một điểm thiệt hại tổng hợp xuyên currency. Chi tiết vận hành và
counterexample ở [contract nhãn](../edai-1/reference/return_label_contract.md) và
thiết kế scope. Đây là quyết định thiết kế,
chưa là evidence implementation hoặc POC đạt.
