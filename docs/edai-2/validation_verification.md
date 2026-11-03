# Validation và Verification

EDAI-2, dòng 28–32: coverage >90%, EP/BVA, mutation >80%, property-based và Locust.

## Kiểm chứng code, 31/10/2026

PR testing chạy trên máy phát triển, không gắn nhãn test chạy AWS. `serving/app.py` đạt **98%**, tổng **96%**, 86 test pass. [Test API](../../serving/tests/test_app.py) dùng fixture, MagicMock/AsyncMock và httpx MockTransport. EP/BVA kiểm số SKU 0/1/50/100/101 và độ dài 0/1/64/65; Hypothesis kiểm idempotency khi gọi lại request hoặc gửi SKU lặp. [Model properties](../../tests/unit/test_model_properties.py) kiểm prediction/training nhất quán và split theo thời gian.

![Coverage: 86 pass, app.py 98%, tổng 96%](../evidence/serving-coverage-20261031.png)

## Mutation testing (mutmut)

Cấu hình [`[tool.mutmut]`](../../pyproject.toml) chọn `serving/app.py`, dùng bộ test
[`serving/tests/test_app.py`](../../serving/tests/test_app.py) và sao chép `rag` để test các nhánh RAG.
Chạy offline với dependency đã cache (mutmut 3.3.1):

```bash
UV_OFFLINE=1 uv run --offline --no-project --python 3.12 --with-requirements serving/requirements-dev.txt mutmut run --max-children 2
UV_OFFLINE=1 uv run --offline --no-project --python 3.12 --with-requirements serving/requirements-dev.txt mutmut results
```

Lần chạy 2026-10-31 (laptop, 4 tiến trình, ~2,5 phút): **171 mutant** trên `serving/app.py`.

| Kết quả | Số | Nghĩa |
|---|---:|---|
| killed | 138 | test fail khi code bị đổi ⇒ test bắt được |
| timeout | 19 | test chạy quá giới hạn thời gian của mutmut (code bị đổi làm request treo) — coi là bắt được |
| no tests | 9 | `load_store` nạp Feast thật; test thay bằng `store_factory` giả nên không chạy hàm này (cố ý) |
| survived | 5 | cả 5 là **mutant tương đương** (dưới) |

**Mutation score** = (killed + timeout) / (171 − no tests) = 157/162 = **96,9%** (chỉ tính killed: 138/162 = 85,2%).

5 mutant sống, xem bằng `mutmut show <tên>`:
- `x_fetch_features__mutmut_29/32/33`: `zip(VIEWS.values(), results, strict=True)` → `strict=None`/`False`/bỏ.
  `results` sinh từ `asyncio.gather` trên chính `VIEWS` nên hai dãy luôn dài bằng nhau; `strict` không bao giờ đổi
  hành vi ⇒ tương đương, không viết được test giết nó.
- `x_create_app__mutmut_17/25`: `host="0.0.0.0"` của MCP → `None` / `"XX0.0.0.0XX"`. SDK MCP chỉ bật chặn
  DNS-rebinding khi host là `localhost`/`127.0.0.1`; mọi giá trị khác cho cùng hành vi ⇒ tương đương.

![mutmut serving/app.py: 171 mutant, 138 killed, 19 timeout, 9 no tests, 5 survived tương đương; score 96,9%](../evidence/mutmut-serving-20261031.png)

Ảnh dựng từ [output thật](../evidence/mutmut-serving-20261031.txt) (giữ đủ danh sách timeout/no tests); bỏ dòng
timeout/no tests trong ảnh cho gọn. Lần chạy đầu trong sandbox của công cụ AI bị treo vì sandbox chặn socket nội bộ
(`TestClient` cần), không phải lỗi test.

Mutmut 3.3.1 có bộ chọn tên mutant ở `mutmut run [MUTANT_NAMES]...`, không có tùy chọn chọn dòng
theo Git diff trong `mutmut run --help`. Lần này mutate **toàn bộ `serving/app.py`**, file thay đổi nhiều
của Web API, theo cấu hình trên; không khẳng định chỉ mutate các dòng đổi. Để giới hạn theo file ở lần
sau, dùng `git diff --name-only <base> -- '*.py'` lấy danh sách file đổi, đối chiếu và cập nhật
`paths_to_mutate` tương ứng trước khi chạy; cách này vẫn không giới hạn từng dòng trong file.



#### Load test trên EKS

Locust chạy trên laptop (Việt Nam), đi qua `kubectl port-forward svc/feature-api` tới cụm ở Sydney
(ap-southeast-2); 20 user, 60 s, cùng [`serving/locustfile.py`](../../serving/locustfile.py), SKU thật
`APP-00042,APP-00482,ELE-01103`. Đi port-forward để tránh rate limit 50 r/s của ingress.

| Request | Số | Lỗi | p50 | p95 | p99 | req/s |
|---|---:|---:|---:|---:|---:|---:|
| `POST /features` | 1.999 | 0 | 250 ms | 270 ms | 290 ms | 33,6 |
| `POST /features [batch]` | 530 | 0 | 250 ms | 270 ms | 280 ms | 8,9 |
| `GET /healthz` | 475 | 0 | 250 ms | 270 ms | 280 ms | 8,0 |
| **Tổng** | **3.004** | **0** | 250 ms | 270 ms | 290 ms | **50,5** |

Báo cáo HTML: [`evidence/aws/load-test-feature-api-20261030.html`](../evidence/aws/load-test-feature-api-20261030.html).

Đọc số: `/healthz` không làm gì cũng mất ~250 ms ⇒ gần hết là đường mạng laptop → Sydney, không phải API.
Phía server (`http_request_duration_seconds` của pod nhận tải): `/features` trung bình **1,8 ms**
(4,57 s / 2.536 request). Port-forward dồn hết vào 1 pod; req/s bị chặn bởi 20 user × ~250 ms mạng, không phải
bởi API. 0 lỗi.

[Mục lục phần này](README.md) · [Mục lục chung](../README.md)
