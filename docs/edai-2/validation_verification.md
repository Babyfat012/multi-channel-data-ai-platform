# Validation và Verification

EDAI-2, dòng 28–32: coverage >90%, EP/BVA, mutation >80%, property-based và Locust.

**Kết luận:** coverage (tỉ lệ dòng code được test chạy qua) đạt 96%, mutation score vượt ngưỡng 80%, và load test Locust trên EKS
chạy 3.004 request không lỗi. Test chạy trên máy phát triển; riêng load test gọi vào cụm EKS.

Giải thích ngắn các loại kiểm thử:

- **EP/BVA** (Equivalence Partitioning / Boundary Value Analysis): chia đầu vào thành các nhóm tương đương và thử đúng các giá trị ở biên, ví dụ 0, 1, giới hạn và giới hạn + 1.
- **Property-based** (thư viện Hypothesis): thay vì viết từng ví dụ, khai báo một tính chất luôn phải đúng rồi để thư viện tự sinh hàng trăm đầu vào để thử.
- **Mutation testing** (mutmut): cố ý sửa code thành nhiều bản lỗi nhỏ (gọi là mutant) rồi chạy test. Test làm tốt nếu phát hiện được phần lớn các mutant này.
- **Locust**: công cụ load test, giả lập nhiều người dùng gọi API cùng lúc.

## Kiểm chứng code

Test của PR chạy trên máy phát triển, không được ghi là test chạy trên AWS. Kết quả: 86 test pass, coverage của `serving/app.py` đạt **98%**, toàn bộ đạt **96%**.

- [Test API](../../serving/tests/test_app.py) dùng fixture, MagicMock/AsyncMock và httpx MockTransport để thay các dịch vụ thật.
- EP/BVA thử số SKU 0/1/50/100/101 và độ dài 0/1/64/65.
- Hypothesis kiểm idempotency: gọi lại cùng request, hoặc gửi SKU bị lặp, vẫn ra cùng kết quả.
- [Model properties](../../tests/unit/test_model_properties.py) kiểm dự đoán và huấn luyện cho kết quả nhất quán, và việc chia dữ liệu theo thời gian.

![Coverage: 86 pass, app.py 98%, tổng 96%](../evidence/serving-coverage-20261031.png)

### Ảnh từng loại kiểm thử

Các ảnh dưới là **output terminal thật** của lần chạy trên máy phát triển. Script
[`capture_testing_evidence.py`](../../scripts/evidence/capture_testing_evidence.py) chạy test rồi chụp lại, kèm file `.txt` cùng tên.
Lần chạy này sau khi test và code đã thay đổi, nên số khác ảnh coverage ở trên: **90 test pass, `serving/app.py` 96%, tổng 96%** (vẫn trên ngưỡng 90%).

**Coverage** (`pytest --cov=serving`): [`testing-coverage-20261104.txt`](../evidence/testing-coverage-20261104.txt)

![pytest-cov serving: 90 passed, app.py 96%, tổng 96%](../evidence/testing-coverage-20261104.png)

**EP/BVA**: 53 test theo nhóm tương đương và giá trị biên (số SKU 0/1/100/101, định dạng SKU, body sai kiểu, `top_k`, trường dự đoán thiếu hoặc sai):

![pytest -v EP/BVA: 53 test boundaries, sku_format, bad_body, validation PASSED](../evidence/testing-epbva-20261104.png)

**Property-based (hypothesis)**, kiểm hai nhóm tính chất:

- API (`test_idempotent`).
- Model: dự đoán không đổi với cùng đầu vào; khi chia dữ liệu theo thời gian thì tập train không chứa dữ liệu tương lai; huấn luyện lại trên cùng dữ liệu ra cùng một model.

![hypothesis API: 50 ví dụ, 0 lỗi](../evidence/testing-hypothesis-20261104.png)

![hypothesis model: 5 test, 100–200 ví dụ mỗi test, 0 lỗi](../evidence/testing-hypothesis-model-20261104.png)

Ảnh mutation (mutmut) nằm ở mục dưới. **Locust** chạy trên EKS, ảnh báo cáo nằm ở mục [Load test trên EKS](#load-test-trên-eks).

## Mutation testing (mutmut)

### Kết quả lịch sử: toàn bộ file

Cấu hình [`[tool.mutmut]`](../../pyproject.toml) chọn file `serving/app.py` để tạo mutant, dùng bộ test
[`serving/tests/test_app.py`](../../serving/tests/test_app.py), và chép kèm thư mục `rag` để test được các nhánh RAG.
Chạy offline với dependency đã có sẵn trong cache (mutmut 3.3.1):

```bash
UV_OFFLINE=1 uv run --offline --no-project --python 3.12 --with-requirements serving/requirements-dev.txt mutmut run --max-children 2
UV_OFFLINE=1 uv run --offline --no-project --python 3.12 --with-requirements serving/requirements-dev.txt mutmut results
```

Lần chạy (laptop, 4 tiến trình, ~2,5 phút): **171 mutant** trên `serving/app.py`.

| Kết quả | Số | Nghĩa |
|---|---:|---|
| killed | 138 | test fail khi code bị đổi, tức là test đã bắt được lỗi |
| timeout | 19 | test chạy quá giới hạn thời gian của mutmut (code bị đổi làm request treo) — coi là bắt được |
| no tests | 9 | `load_store` nạp Feast thật; test thay bằng `store_factory` giả nên không chạy hàm này (cố ý) |
| survived | 5 | cả 5 là **mutant tương đương** (dưới) |

**Mutation score** = (killed + timeout) / (171 − no tests) = 157/162 = **96,9%**. Nếu chỉ tính killed: 138/162 = 85,2%.

5 mutant còn sống (test không bắt được), xem bằng `mutmut show <tên>`. Cả 5 là mutant tương đương: bản sửa không làm thay đổi hành vi của code, nên không test nào có thể bắt được.

- `x_fetch_features__mutmut_29/32/33`: đổi `zip(VIEWS.values(), results, strict=True)` thành `strict=None`, `strict=False`, hoặc bỏ hẳn `strict`.
  `results` sinh ra từ `asyncio.gather` trên chính `VIEWS`, nên hai dãy luôn dài bằng nhau. Do đó `strict` không bao giờ làm đổi hành vi.
- `x_create_app__mutmut_17/25`: đổi `host="0.0.0.0"` của MCP thành `None` hoặc `"XX0.0.0.0XX"`. SDK MCP chỉ bật chặn
  DNS-rebinding khi host là `localhost`/`127.0.0.1`; mọi giá trị khác đều cho cùng hành vi.

![mutmut serving/app.py: 171 mutant, 138 killed, 19 timeout, 9 no tests, 5 survived tương đương; score 96,9%](../evidence/mutmut-serving-20261031.png)

Ảnh dựng từ [output thật](../evidence/mutmut-serving-20261031.txt). File output giữ đủ danh sách timeout/no tests; trong ảnh bỏ các dòng đó cho gọn.
Lần chạy đầu trong sandbox của công cụ AI bị treo, vì sandbox chặn socket nội bộ mà `TestClient` cần dùng; đây không phải lỗi của test.

Lần chạy này mutate toàn bộ `serving/app.py`, lúc đó chưa có bước chọn file theo Git diff. Kết quả được giữ nguyên
làm bằng chứng lịch sử, không dùng để chứng minh việc chỉ mutate phần code thay đổi.

### Chọn file thay đổi theo Git

Mục tiêu: chỉ chạy mutation trên những file vừa thay đổi, thay vì cả repo. Script [`run_changed_mutation.py`](../../scripts/evidence/run_changed_mutation.py) làm như sau:

1. Nhận hai mốc Git `--base` và `--head`, đổi chúng thành commit SHA đầy đủ.
2. Lấy danh sách file Python được thêm hoặc sửa bằng `git diff --no-renames --diff-filter=AM --name-only`.
   File đổi tên được coi như xoá file cũ và thêm file mới; file bị xoá thì không mutate.
3. Bỏ các file test, rồi chỉ giữ những file nằm trong `paths_to_mutate` của cấu hình Web API (hiện là `serving/app.py`).
4. Ghi toàn bộ danh sách file được chọn và file bị loại vào manifest.

Các module DAG, ML và feature publication nằm ngoài phạm vi Web API, nên không được tính là đã kiểm chứng bằng lần chạy này.

Script xuất code và test **đúng tại commit head** ra một thư mục tạm, rồi tạo cấu hình mutmut riêng chỉ chứa các file
được chọn. Working tree và cấu hình gốc không bị sửa; thay đổi chưa commit không có trong bản xuất này.
`also_copy` gồm `rag` và `feature_repo` để API import được các module nội bộ; các module này không bị mutate.
Lưu ý: script chọn theo **file thay đổi** và vẫn mutate toàn bộ file đó, không giới hạn ở từng dòng thay đổi.

Chạy từ gốc repo với Python 3.12 và dependency trong `serving/requirements-dev.txt`:

```bash
# Xem trước phạm vi, chưa chạy mutation.
uv run --no-project --python 3.12 --with-requirements serving/requirements-dev.txt \
  python scripts/evidence/run_changed_mutation.py \
  --base '89cd44e^' --head 89cd44e --dry-run --output out/mutation-plan

# Chạy thật trên cùng hai mốc; output phải là thư mục mới.
uv run --no-project --python 3.12 --with-requirements serving/requirements-dev.txt \
  python scripts/evidence/run_changed_mutation.py \
  --base '89cd44e^' --head 89cd44e --max-children 2 --output out/mutation-changed
```

Khi kiểm một thay đổi khác, thay hai ref bằng base/head cần đánh giá. Script so trực tiếp hai commit,
không tự tìm merge-base.

Trạng thái ghi trong manifest:

- Không có file phù hợp: ghi `skipped` (không ghi `passed`).
- Chạy `--dry-run` và có file phù hợp: ghi `planned`.

Thư mục output giữ lại log `run.txt`, kết quả từng mutant `results.txt`, cấu hình
`mutation-config.toml` và `manifest.json`. Manifest có SHA của code, SHA của cấu hình và phiên bản mutmut.

Ngưỡng đạt của script là **killed / tổng mutant >80%**. Cách tính này chặt hơn báo cáo lịch sử ở trên: mẫu số tính cả `no tests`, và timeout không được cộng
vào killed. Manifest ghi thêm hai cách tính có loại `no tests` để so với báo cáo lịch sử.
Script trả lỗi khi còn mutant chưa kiểm, khi chạy lỗi, khi kết quả rỗng, hoặc khi score không đạt. Trạng thái skip không được tính là đạt rubric.

### Kết quả local: file được chọn từ Git diff

So base `3cd3fcd` (`89cd44e^`) với head `89cd44e`: commit này thêm/sửa 11 file Python,
nhưng chỉ **`serving/app.py`** nằm trong phạm vi Web API nên chỉ file này được chọn. Test API và các module ngoài phạm vi được liệt kê
ở `excluded_files`, không được tính là đã mutation-test trong lần này.
Mutmut 3.3.1 chạy 2 tiến trình, xong trong khoảng 3 phút 31 giây, exit code 0.

| Kết quả | Số mutant |
|---|---:|
| Killed | 219 |
| Survived | 12 |
| No tests | 14 |
| Timeout | 0 |
| Tổng | 245 |

**Score theo cách tính chặt = 219/245 = 89,4%, vượt ngưỡng 80%** (mẫu số tính cả `no tests`).
Nếu loại `no tests`, score = 219/231 = **94,8%**.

Giới hạn: chưa kết luận được 12 mutant còn sống có phải là mutant tương đương hay không;
14 mutant không có test chạy qua vẫn là chỗ thiếu của bộ test. Các trạng thái còn lại bằng 0, không có mutant nào chưa kiểm.

Bằng chứng: [manifest và SHA code](../evidence/mutation-changed-serving-20261103.json),
[log chạy](../evidence/mutation-changed-serving-20261103.txt),
[kết quả từng mutant](../evidence/mutation-changed-serving-results-20261103.txt),
[cấu hình thực chạy](../evidence/mutation-changed-serving-config-20261103.toml).
Đây là kiểm chứng chạy local, giới hạn ở các file thay đổi trong phạm vi Web API. Nó không giới hạn theo từng dòng, và không phải lần chạy trên Jenkins/AWS.

**Chạy lại, có ảnh.** Cùng lệnh và cùng hai mốc (`89cd44e^` .. `89cd44e`), xong trong 2 phút 20 giây, exit code 0. Kết quả **trùng hệt lần trước**:
245 mutant, 219 killed, 14 no tests, 12 survived, 0 timeout (score 89,4%; loại no tests 94,8%), cùng SHA code
([manifest](../evidence/mutation-changed-serving-20261104.json), [log](../evidence/mutation-changed-serving-20261104.txt), [từng mutant](../evidence/mutation-changed-serving-results-20261104.txt)).
Chạy lại ra cùng kết quả nghĩa là phép kiểm lặp lại được. Ghi chú: SHA cấu hình runner trong manifest khác nhau giữa hai lần, dù file cấu hình lưu lại giống hệt nhau; chưa xác định được nguyên nhân.

![Mutation theo Git diff: lệnh run_changed_mutation, chọn serving/app.py, 245 mutant, 219 killed, 14 no tests, 12 survived, status passed, exit 0](../evidence/testing-mutation-changed-20261104.png)

Ảnh là output terminal đã lọc bỏ ký tự spinner, dựng từ `run.txt` và `manifest.json` của đúng lần chạy này bằng
[`capture_testing_evidence.py --mutation-dir`](../../scripts/evidence/capture_testing_evidence.py). Không chạy lại mutation chỉ để chụp ảnh.

#### Load test trên EKS

**Kết luận:** Feature API trên EKS chịu được 20 user đồng thời trong 60 s, không lỗi. Độ trễ ~250 ms người dùng thấy gần như hoàn toàn là do đường mạng
từ Việt Nam sang Sydney; bản thân API chỉ mất trung bình 1,8 ms mỗi request (xem phần đọc số ở cuối mục).

Cách chạy: Locust chạy trên laptop (Việt Nam), đi qua `kubectl port-forward svc/feature-api` tới cụm ở Sydney
(ap-southeast-2). 20 user, 60 s, dùng [`serving/locustfile.py`](../../serving/locustfile.py) với SKU thật
`APP-00042,APP-00482,ELE-01103`. Dùng port-forward thay vì ingress để tránh rate limit 50 request/giây của ingress.

| Request | Số | Lỗi | p50 | p95 | p99 | req/s |
|---|---:|---:|---:|---:|---:|---:|
| `POST /features` | 1.999 | 0 | 250 ms | 270 ms | 290 ms | 33,6 |
| `POST /features [batch]` | 530 | 0 | 250 ms | 270 ms | 280 ms | 8,9 |
| `GET /healthz` | 475 | 0 | 250 ms | 270 ms | 280 ms | 8,0 |
| **Tổng** | **3.004** | **0** | 250 ms | 270 ms | 290 ms | **50,5** |

Báo cáo HTML: [`evidence/aws/load-test-feature-api-20261030.html`](../evidence/aws/load-test-feature-api-20261030.html).

**Chạy lại** (cùng cấu hình, sau khi node EKS được bật lại): 59 giây, 20 user, đi qua `kubectl port-forward svc/feature-api` (host `127.0.0.1:8000`),
báo cáo HTML [locust-feature-api-eks.html](../evidence/aws/locust-feature-api-eks.html), số liệu [locust-feature-api-eks_stats.csv](../evidence/aws/locust-feature-api-eks_stats.csv).

| Request | Số | Lỗi | p50 | p95 | p99 | req/s |
|---|---:|---:|---:|---:|---:|---:|
| `POST /features` | 2.052 | 0 | 250 ms | 260 ms | 290 ms | 34,6 |
| `POST /features [batch]` | 525 | 0 | 250 ms | 260 ms | 280 ms | 8,9 |
| `GET /healthz` | 488 | 0 | 240 ms | 260 ms | 310 ms | 8,2 |
| **Tổng** | **3.065** | **0** | 250 ms | 260 ms | 290 ms | **51,7** |

![Báo cáo Locust trên EKS: 3.065 request, 0 lỗi, p50 250 ms, RPS ổn định khoảng 53, 20 user](../evidence/aws/locust-feature-api-eks.png)

Hai lần chạy cho kết quả gần như trùng nhau (3.004 và 3.065 request, đều không lỗi, p50 250 ms), nên hiệu năng ổn định giữa hai lần bật cụm.
Trên biểu đồ thời gian phản hồi, p95 khoảng 800 ms ở giây đầu tiên (lúc kết nối còn đang khởi tạo), sau đó về mức ổn định khoảng 260 ms.

Cách đọc số:

- `/healthz` gần như không xử lý gì mà vẫn mất ~250 ms. Vậy gần hết thời gian là đường mạng từ laptop tới Sydney, không phải do API.
- Đo phía server (metric `http_request_duration_seconds` của pod nhận tải): `/features` mất trung bình **1,8 ms**
  (4,57 s cho 2.536 request).
- Port-forward dồn toàn bộ tải vào 1 pod. Số request/giây bị giới hạn bởi 20 user, mỗi request chờ mạng ~250 ms, chứ không phải bởi API.
- Không có request lỗi.

[Mục lục phần này](README.md) · [Mục lục chung](../README.md)
