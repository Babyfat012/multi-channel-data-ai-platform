# Silver cho ba Channel Batch mới

TikTok Shop, Shopify và Shopee đã có source export JSONL ở
`out/channels-small/source-exports`. DP1 nạp từng response nguyên văn vào bảng
Bronze riêng; DP2 đọc Bronze và ghi một bảng Silver riêng cho từng Channel.
Đây là dữ liệu nhỏ để kiểm đường ống, không thay thế bản raw 100 GB của
Amazon/Walmart ở `/data/raw`. Thư mục `out/` không được commit; tạo lại bằng:

```bash
.venv/bin/python -m generator batch --config configs/small.yaml --output ./out/channels-small
env -u POSTGRES_PASSWORD bash scripts/setup/setup_dp1_file_channels.sh
env -u POSTGRES_PASSWORD bash scripts/setup/setup_dp2_file_channels.sh
```

| Channel | Khóa Order | Khóa Order Line | Thời gian nguồn | Tiền nguồn |
|---|---|---|---|---|
| TikTok Shop | `data.orders[0].id` | `line_items[].id` | Unix giây | Chuỗi theo currency |
| Shopify | `order.id` | `line_items[].id` | ISO 8601 | Chuỗi theo currency |
| Shopee | `response.order_list[0].order_sn` | `item_list[].order_item_id` | Unix giây | Số nguyên VND |

Job `spark_jobs/silver/build_file_order_snapshots.py` nổ mảng thành Order Line và khử bản trùng
theo cặp `(order_id, order_item_id)`. Bản có `last_updated_time` mới nhất thắng;
khi hòa, chọn `export_date` rồi `source_file` mới nhất. `export_date` chỉ là thời
điểm lấy snapshot, không phải một phần khóa Order Line. Silver giữ
`source_file`, `export_date`, `processed_at` và `run_id` để truy được bản thắng.

Ba bảng Silver có cùng các cột chính: `order_id`, `order_item_id`, `created_at`,
`last_updated_time`, `order_status`, `channel_status`, `seller_sku`, `quantity`,
`line_gross_amount`, `line_discount_amount`, `line_net_amount`, `currency`, địa
chỉ và provenance. `line_net_amount = line_gross_amount - line_discount_amount`.
Shopee giữ số tiền VND ở đơn vị nguyên, không tự chia cho 100.

Ánh xạ trạng thái cố ý bảo thủ:

- TikTok Shop có trạng thái API `DELIVERED` và `COMPLETED` riêng; Silver giữ cả
  trạng thái gốc và dạng chuẩn hóa. `update_time` của snapshot không mặc nhiên
  là ngày giao thực tế của mọi Order.
- Shopify chỉ có `financial_status`, `fulfillment_status`, `closed_at` và
  `cancelled_at` trong response đang mô phỏng. Silver suy ra trạng thái từ tổ hợp
  này; `fulfilled` được gọi là `shipped`, `closed_at` của renderer hiện tại được
  gọi là `completed`. Cách chiếu này gắn với renderer giả lập, không phải cam kết
  rằng mọi Shop thực có cùng quy trình.
- Shopee dùng `COMPLETED` cho cả lõi đã giao và hoàn tất; Silver không dựng
  `delivered_at` từ `update_time`.

`spark_jobs/silver/validate_file_order_snapshots.py` kiểm số Order riêng biệt giữa Bronze và
Silver, khóa Order Line không trùng, trường bắt buộc và phép tính tiền. Lần chạy
DP2 năm Channel đã thành công; số đo và ảnh UI ghi ở `docs/edai-1/pipeline_orchestration.md`.
