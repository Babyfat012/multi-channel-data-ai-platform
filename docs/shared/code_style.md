# Quy ước viết code

Mục tiêu: người mới mở file là đọc được, không phải đọc qua một bài luận.

## Công cụ

- `uv run ruff format .` tự xuống dòng, thụt lề, dấu nháy (dòng tối đa 100 ký tự).
- `uv run ruff check --fix .` sắp import, bỏ import thừa, bắt vài lỗi hay gặp.
- Cả hai chạy trong pre-commit và CI (`.github/workflows/ci.yml`). Cấu hình ở `pyproject.toml`.

## Chú thích

- **Đầu file**: một dòng nói file làm gì. Thêm vài dòng cho điều không nhìn code thấy được,
  và cách chạy nếu là job.
- **Docstring hàm**: một dòng là đủ. Thêm đoạn thứ hai chỉ khi có một "vì sao" không hiển nhiên.
- **Comment**: giải thích *vì sao*, không lặp lại *code làm gì*.
- Một file dùng một thứ tiếng (tiếng Việt).

Không viết vào code:

- Số dòng của file khác (`walmart.py:103`) hay số mục tài liệu: sửa file kia là sai ngay.
  Trỏ tới tên tài liệu là đủ, ví dụ `docs/edai-1/reference/silver.md`.
- Lịch sử quyết định ("chốt ngày ...", "Q3"): để trong `docs/` hoặc ADR.
- Số đo của một lần chạy (GB, số dòng, giây): để trong tài liệu bằng chứng.
- Chữ IN HOA để nhấn mạnh.
- Tên người/công cụ làm việc (người vận hành, brief, ...).

## Ví dụ

Trước:

```python
def dedup(df):
    """Bỏ bản trùng theo khoá `(order_id, order_item_id)`, giữ bản mới nhất.

    Thứ tự tie-break, chốt ở `docs/edai-1/reference/silver.md` mục 3.1:
      1. `last_updated_time` lớn nhất — "luôn lấy cái mới nhất";
      2. hoà thì `export_date` lớn nhất.

    Vì sao `order_id` MỘT MÌNH là sai: ở mức Order Line, một `order_id` nằm trên nhiều dòng
    THẬT (một đơn nhiều món), nên dedup chỉ theo `order_id` sẽ xoá Order Line thật. ...
    """
```

Sau:

```python
def dedup(df):
    """Giữ bản mới nhất của mỗi `(order_id, order_item_id)`.

    Thứ tự: `last_updated_time` lớn nhất, hoà thì `export_date` lớn nhất. Khoá phải có
    `order_item_id`: chỉ theo `order_id` sẽ xoá nhầm các line khác của cùng đơn.
    """
```
