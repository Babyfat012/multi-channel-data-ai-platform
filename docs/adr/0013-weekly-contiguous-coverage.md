# Coverage scale theo tuần nối tiếp

Quyết định ngày 2026-10-19 (bản sửa coverage theo ADR-0013): nguồn scale phát envelope theo tuần,
không chồng và không cộng dồn, trên mỗi feed. Trước đây coverage chỉ xuất cuối
observation horizon, sau mọi Order; nhãn cuối kỳ có đủ nhưng feature PIT của
Order gốc luôn count0. Quyết định này thay nhịp phát coverage của scale, mở rộng
hợp interval trong policy v2; fixture và expected L01–L17 giữ nguyên.

Tuần là mức cân bằng: coverage ngày giảm chờ nhưng tăng khoảng bảy lần số
metadata/file; coverage cộng dồn giúp policy đơn giản nhưng mỗi envelope hash
lại lịch sử, tổng chi phí bậc hai theo horizon. Tuần nối tiếp hash mỗi dòng một
lần, quan sát sau cuối tuần +1 ngày hoặc sau dòng trễ cuối +1µs, lấy mốc muộn hơn.
Đuôi cuối phủ đúng follow-up; cả tuần không có sự kiện vẫn cần claim nguồn.

Giá phải trả là phải ghép union interval theo từng kind, chờ đóng tuần và lateness,
và giữ nhiều envelope hơn trước. Gap, partial receipt hay unresolved scope vẫn
unknown; không bù bằng thời gian đã trôi qua. Silver chọn tập effective trong
interval, bucket theo ngày UTC trước join để không nhân mọi dòng với mọi tuần.
Gold lọc interval giao cửa sổ line, giữ mọi revision/correction liên quan và kiểm
authority trước lọc, nên input không tăng theo tuần tail. History revision của
chính line vẫn có chi phí riêng; giới hạn driver keyless 10.000 giữ nguyên.

Kiểm Python thuần seed7/60 ngày/4 đơn mỗi ngày/3 SKU: 131/367 target thật có
`labeled_count_90d > 0`, tối đa 30 coverage/line. Spark do người vận hành kiểm riêng;
xem [Gold](../edai-1/reference/gold_order_lifecycle.md) và [contract nhãn](../edai-1/reference/return_label_contract.md).
