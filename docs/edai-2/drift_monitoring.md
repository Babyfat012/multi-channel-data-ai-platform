# Drift Monitoring trên AWS

EDAI-2, dòng 57: novel idea — phát hiện drift theo Channel (kênh bán).

Drift là khi dữ liệu mới có phân phối khác dữ liệu lúc train model. Khi đó điểm dự đoán của model có thể không còn đáng tin.
Ở đây một Job Evidently chạy trên EKS so dữ liệu mới với dữ liệu tham chiếu và báo động khi lệch quá ngưỡng.
Job được chạy khi cần kiểm, chưa tự động train lại model khi có drift.

Các kết quả dưới đây là của những lần chạy trên AWS; tài liệu không khẳng định dịch vụ hiện vẫn đang bật.

<a id="trên-aws-pipeline-a--2026-10-31"></a>
## Drift trên EKS

**Kết luận:** phép so trên dữ liệu ổn định không báo động. Phép so giả lập việc đơn hàng dồn về một sàn (chỉ Shopee)
báo drift đúng ở cột `seller_sku`, với điểm 0,2028, vượt ngưỡng 0,2. Chạy lại Job lần hai cho ra đúng các số này.

Cách làm:

- Dữ liệu: bảng `training_lines` trên S3 của môi trường AWS (`s3://platform-665116832975-lakehouse/feature-store/training_lines`, lấy mẫu 2‰).
- Hai phép so, cả hai đều lấy 30 ngày đầu làm dữ liệu tham chiếu:
  - `stable`: so với toàn bộ phần dữ liệu sau đó. Đây là trường hợp không có gì thay đổi, nên kỳ vọng không báo động.
  - `channel_shift`: so với phần dữ liệu sau đó nhưng **chỉ giữ kênh Shopee**, mô phỏng cơ cấu đơn dồn về một sàn. Kỳ vọng có báo động.
  - Lô "đợt Sale" cũ chỉ là dữ liệu tham khảo, không dùng cho phép đo AWS này.
- Nơi chạy: Job [`infra/k8s/ml/drift-job.yaml`](../../infra/k8s/ml/drift-job.yaml) trên EKS, context `platform-apps`, namespace `kubeflow`, tên Job `mle-drift`.
  Job dùng service account `pipeline-runner`; qua Pod Identity, account này chỉ được đọc `feature-store/*`.
- Phiên bản: **Evidently 0.7.23 và pandas 2.3.3**. pandas 3 làm Evidently lỗi khi import, nên lần chạy này dùng pandas 2.3.3.

Kết quả: [mle-drift-20261031.json](../evidence/aws/mle-drift-20261031.json).

Điểm drift dùng khoảng cách JS (Jensen-Shannon): 0 là hai phân phối giống hệt, càng lớn càng lệch. Một cột bị coi là drift khi điểm vượt ngưỡng của cột đó.

| Phép so | Dòng tham chiếu / hiện tại | JS `seller_sku` (ngưỡng 0,2) | JS `label` (ngưỡng 0,1) | Alert |
|---|---:|---:|---:|---|
| `stable` | 72.109 / 287.348 | 0,0664 | 0,0008 | không |
| `channel_shift` | 72.109 / 8.320 | **0,2028 > 0,2** | 0,0298 | **có: seller_sku** |

Cách đọc: lô `stable` không báo động. Lô `channel_shift` chỉ giữ Shopee nên tỉ lệ các SKU thay đổi, làm cột `seller_sku` vượt ngưỡng;
cột `label` (nhãn trả hàng) vẫn dưới ngưỡng. Các số này đo trên dữ liệu S3 của AWS; số của lô Sale chạy local không được dùng thay.

### Từng bước và ảnh

Quy trình có bốn bước. Bốn ảnh ngay dưới đây chạy **ở local trên dữ liệu mô phỏng** (60 ngày, 400 dòng mỗi ngày, kênh Shopee dồn về nửa sau danh mục SKU).
Mục đích là chứng minh quy trình chạy được trước khi đưa lên EKS. Điểm JS trong các ảnh này **không phải** số đo AWS ở bảng trên.

1. **Dữ liệu tham chiếu và hiện tại:** từ `training_lines`, lấy 30 ngày đầu làm tham chiếu. Với `stable`, dữ liệu hiện tại là phần sau; với `channel_shift`, là phần sau nhưng chỉ của Shopee.

   ![Bước 1 (local, mô phỏng): hai cửa sổ tham chiếu và hiện tại của stable và channel_shift, ngày, số dòng, nhãn dương, SKU, kênh](../evidence/drift-data-local-20261104.png)
2. **Báo cáo `stable`:** Evidently so từng cột bằng khoảng cách Jensen-Shannon. Lô có cùng phân phối nên không cột nào vượt ngưỡng.

   ![Evidently stable (local, mô phỏng): seller_sku không drift](../evidence/drift-stable-local-20261104.png)

3. **Báo cáo `channel_shift`:** cột `seller_sku` vượt ngưỡng 0,2, cột `label` thì không.

   ![Evidently channel_shift (local, mô phỏng): seller_sku drift, JS 0,649](../evidence/drift-channel_shift-local-20261104.png)

4. **Bảng kết quả và alert** (cùng cấu trúc với `mle-drift-20261031.json`):

   ![Bảng JS theo cột: stable ổn định, channel_shift alert](../evidence/drift-summary-local-20261104.png)

**Báo cáo Evidently từ EKS.** Để có ảnh báo cáo thật từ EKS, Job [`mle-drift`](../../infra/k8s/ml/drift-job.yaml) được chạy lại (namespace `kubeflow`, đầu vào vẫn là `training_lines` trên S3 như lần trước):

1. Pod được giữ lại 15 phút sau khi chạy xong, đủ thời gian chép báo cáo HTML ra bằng `kubectl -n kubeflow cp <pod>:/tmp/drift ./drift-eks`.
2. Chụp ảnh báo cáo bằng [`capture_drift_evidence.py`](../../scripts/evidence/capture_drift_evidence.py) (`--html-dir drift-eks --summary drift-eks/summary.json`).

Kết quả lần chạy lại **trùng với lần trước đến từng chữ số thập phân** ([drift-summary-eks.json](../evidence/aws/drift-summary-eks.json)), nghĩa là phép đo ổn định và chạy lại được.

**Bước 1 trên EKS: dữ liệu tham chiếu và hiện tại.** Ảnh dưới dựng từ chính tập `training_lines` trên S3 (đầu vào của Job, 359.457 dòng), bằng hàm `ml.drift.comparisons` với `--reference-days 30`.
Số dòng tính ra **đúng bằng** `reference_rows`/`current_rows` trong summary của Job (stable 72.109 / 287.348; channel_shift 72.109 / 8.320).
Vì vậy ảnh mô tả đúng hai khoảng dữ liệu mà Evidently đã so:

- Tham chiếu: 01/01–30/01. Hiện tại: từ 31/01.
- Ở `channel_shift`, dữ liệu hiện tại chỉ còn Shopee: 8.320 dòng, 1.678 SKU, tỉ lệ nhãn dương 8,75% (tham chiếu là 11,28%).

![Bước 1 trên EKS: hai cửa sổ dữ liệu của stable và channel_shift từ training_lines trên S3, số dòng khớp summary của Job](../evidence/aws/drift-data-aws.png)

![Evidently trên EKS, phép so stable: seller_sku và label không drift](../evidence/aws/drift-stable-aws.png)

![Evidently trên EKS, phép so channel_shift: seller_sku drift, JS 0,2028 vượt ngưỡng 0,2; label không drift](../evidence/aws/drift-channel_shift-aws.png)

![Bảng Jensen-Shannon theo cột từ summary.json của EKS: stable không alert, channel_shift có alert](../evidence/aws/drift-summary-aws.png)

Lưu ý khi đọc ảnh `channel_shift`: biểu đồ phân phối của `seller_sku` gần như trống, vì cột này có hàng nghìn giá trị khác nhau và mỗi giá trị chỉ chiếm tỉ lệ rất nhỏ.
Thông tin chính là điểm Jensen-Shannon và kết luận `Detected`. Cột `Alert` trong bảng tóm tắt là kết luận của cả phép so, không phải của từng cột.

<details>
<summary>Tham khảo: thiết kế, hướng dẫn phát triển và ghi nhận trước triển khai AWS</summary>

Các ghi nhận local giữ nguyên môi trường, không dùng thay bằng chứng cloud.

<!-- Mục đích: theo dõi data drift đầu vào model bằng Evidently, có proof chạy được. -->
## ML-N2: theo dõi data drift (Evidently)

Evidently so phân phối dữ liệu đầu vào của một lô mới với lô tham chiếu (giai đoạn train) và cảnh báo khi lệch.
Cảnh báo này là tín hiệu để kiểm lại model **trước khi** tin vào điểm dự đoán, ví dụ khi đợt Sale làm thay đổi cơ cấu SKU.

| Thành phần | File |
|---|---|
| Kiểm drift + tóm tắt | [`ml/drift.py`](../../ml/drift.py), test [`tests/unit/test_drift.py`](../../tests/unit/test_drift.py) |
| Lô "đợt Sale" | `generator.generate_order_lifecycle --sku-zipf-s 0.3` (SKU khuyến mãi dàn đều) |
| Kết quả | [`evidence/mle-drift-summary-20261021.json`](../evidence/mle-drift-summary-20261021.json), HTML ở `out/drift/` |

## Cách đo

- Cột: `seller_sku`, `label`, đều là cột phân loại. Phương pháp: khoảng cách **Jensen-Shannon**.
- Ngưỡng: `seller_sku` là 0,2. Cột này có khoảng 100 giá trị; chỉ riêng nhiễu do lấy mẫu khoảng 1.000 dòng đã cho điểm 0,14, nên ngưỡng phải cao hơn mức đó.
  `label` dùng ngưỡng mặc định 0,1.
- `alert` bật khi có ít nhất một cột bị drift.

**Bẫy đã gặp:** khi dữ liệu trên 1.000 dòng, Evidently tự đổi phương pháp từ chi-square sang Jensen-Shannon.
Hai phương pháp đọc ngược nhau: chi-square trả p-value, số **nhỏ** nghĩa là drift; Jensen-Shannon trả khoảng cách, số **lớn** nghĩa là drift.
Bản code đầu đọc mọi số như p-value, nên kết luận ngược hẳn: báo `label` drift và bỏ sót `seller_sku`.
Lỗi được phát hiện khi tính lại bằng chi-square của scipy. Cách sửa: xác định chiều so dựa vào tên phương pháp ghi trong metric; test phủ cả hai chiều và các giá trị sát ngưỡng.

## Proof

| Lô so với tham chiếu (30 ngày đầu, 2.307 line) | `seller_sku` | `label` | alert |
|---|---|---|---|
| 15 ngày sau, cùng phân phối (1.165 line) | 0,136 | 0,005 | **không** |
| Đợt Sale 5 ngày, zipf 0,3 (1.228 line) | **0,436** | 0,001 | **có** |

Kiểm tra bằng tay: SKU đứng đầu chiếm 27,9% lô tham chiếu nhưng chỉ 3,3% trong đợt Sale, còn tỷ lệ trả hàng gần như
giữ nguyên (0,299 / 0,306 / 0,300). Điều này khớp với kết quả chỉ `seller_sku` bị drift.

## Chạy

```bash
uv run --no-project --with evidently==0.7.23 --with s3fs==2026.9.0 --with pyarrow \
  env PYTHONPATH=. AWS_ENDPOINT_URL_S3=http://localhost:9000 AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
  python -m ml.drift --reference s3://platform/feature-store/training_lines \
    --sale-truth out/order_lifecycle_sale/_truth/lines.jsonl --out out/drift
```

</details>

[Mục lục phần này](README.md) · [Mục lục chung](../README.md)
