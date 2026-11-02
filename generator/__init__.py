"""Data generator giả lập dữ liệu bán hàng đa kênh của Seller ACME.

Ba lớp (xem docs/edai-1/reference/generator_design.md, mục 2):
- `generator.core`: lõi mô phỏng, sinh "sự thật" chung không mang dạng Channel nào.
- `generator.channels`: renderer, vẽ sự thật thành dạng gốc của từng Channel.
- `generator.delivery`: lớp giao hàng, chèn lỗi (trùng, trễ, theo lô) sau cùng.
"""
