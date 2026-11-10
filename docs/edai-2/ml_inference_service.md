# ML Inference Service trên EKS

EDAI-2, dòng 15: KServe với model-inside-image.

Code / cấu hình: [InferenceService](../../infra/k8s/ml/return-risk.yaml), [deployment script](../../scripts/run/deploy_return_risk.sh).

Các kết quả dưới đây thuộc các lần chạy AWS, không xác nhận dịch vụ đang bật.

## Trên EKS

InferenceService [`return-risk`](../../infra/k8s/ml/return-risk.yaml) chạy ở namespace `ml-serving`. Cách đóng gói là
model-inside-image: file model nằm sẵn trong image, không tải lúc khởi động. Image ECR chứa model MLflow `return-risk`
v1 (alias `production`), build bằng [`deploy_return_risk.sh`](../../scripts/run/deploy_return_risk.sh).

Kiểm thử: gọi thẳng predictor qua port-forward với một đơn APP-00042, Amazon US, 1 cái, 25 USD. Kết quả `score` 0,1661,
`abstain` false, `model_version` "1". Đây cũng là số mà Feature API `/predict` và model-agent trả về
(xem [Web API](web_api_mcp.md#trên-eks)).

![KServe trên EKS: inferenceservice return-risk READY True, pod predictor Running, POST :predict trả score 0.1661 model_version 1](../evidence/aws/kserve-pod-predict-20261031.png)

**Nhiều replica và tự scale.** Cấu hình:
- `minReplicas: 2`, `maxReplicas: 3`.
- `autoscalerClass: external`, vì EKS không có metrics-server để HPA scale theo CPU.
- KEDA scale theo metric `return_risk_predictions_total`, ngưỡng 5 dự đoán/giây cho mỗi pod.

Kết quả khi bắn tải vào `:predict` trong 150 giây: lên 3 pod sau khoảng 48 s, về lại 2 pod sau khoảng 495 s
([log](../evidence/aws/return-risk-autoscale.txt), [ảnh trong Agents](agents.md#model-agent-đủ-bộ-như-data-agent)).

<details>
<summary>Tham khảo: thiết kế, hướng dẫn phát triển và ghi nhận trước triển khai AWS</summary>

Các ghi nhận local giữ nguyên môi trường, không dùng thay bằng chứng cloud.

<!-- Mục đích: phục vụ model return-risk bằng KServe (model-inside-image) -->
## Serve model return-risk — KServe

Mục tiêu: KServe phục vụ model vừa train theo kiểu **model-inside-image**. Kết quả cần có: pod KServe, file YAML và
ảnh gọi thử thành công.

## Hợp đồng gọi

`POST http://return-risk-predictor.ml-serving.svc.cluster.local/v1/models/return-risk:predict`

```json
{"instances": [{"seller_sku": "sku-00002", "channel": "tiktok_shop", "country": "GB",
                "ordered_basis_qty": 3, "ordered_product_net_minor": 30000}]}
```

Trả về `{"predictions": [{"seller_sku", "score", "abstain", "reason", "model_version"}]}`.

- `score` là tín hiệu để **xếp hạng**, không phải xác suất (Q38).
- Người gọi chỉ gửi thuộc tính của đơn. Predictor **tự đọc** feature SKU từ Feast online (Redis), nên danh sách
  feature chỉ được khai ở một chỗ.
- `abstain` nghĩa là model từ chối khẳng định. SKU có dưới 20 line có nhãn trong 90 ngày thì trả
  `abstain: true, reason: insufficient_history`. Quy tắc này chỉ áp dụng khi `labeled_count_90d` có giá trị.
- Nếu count bị thiếu (`null`/NaN), predictor không gọi LightGBM cho dòng đó mà trả `score: null, abstain: true,
  reason: feature_data_unavailable`. Predictor không tự coi count thiếu là 0.
- Trong một batch có cả dòng đủ và thiếu count, chỉ dòng có count được chấm; thứ tự kết quả giữ đúng như đầu vào.
  Các feature nullable khác giữ cách xử lý hiện tại.
- Cách xử lý count thiếu là thay đổi trong repo, chưa kiểm chứng khi triển khai trên AWS.

| Thành phần | File |
|---|---|
| Predictor | [`ml/serve.py`](../../ml/serve.py), test [`tests/unit/test_serve.py`](../../tests/unit/test_serve.py) |
| Image (model nằm trong image) | [`docker/Dockerfile.serve`](../../docker/Dockerfile.serve) |
| InferenceService (RawDeployment) | [`infra/k8s/ml/return-risk.yaml`](../../infra/k8s/ml/return-risk.yaml) |
| Deploy: MLflow alias `production` → image → kind → apply | [`scripts/run/deploy_return_risk.sh`](../../scripts/run/deploy_return_risk.sh) |

## Cài KServe (local)

RawDeployment: không cần Knative/Istio, chỉ cert-manager + controller.

```bash
K="kubectl --context kind-platform"
$K apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.21.2/cert-manager.yaml
$K -n cert-manager wait --for=condition=Available deploy --all --timeout=300s
helm --kube-context kind-platform install kserve-crd oci://ghcr.io/kserve/charts/kserve-crd --version v0.21.0
helm --kube-context kind-platform install kserve oci://ghcr.io/kserve/charts/kserve-resources --version v0.21.0 \
  -n kserve --create-namespace --set kserve.controller.deploymentMode=RawDeployment
scripts/run/deploy_return_risk.sh
```

## Bằng chứng

Deploy (kind, KServe v0.21.0 RawDeployment): `return-risk` READY = True, pod
`return-risk-predictor-…` Running, image `platform-return-risk:v3` = MLflow `return-risk` v3 (alias
`production`, PR-AUC 0,341 vs baseline 0,329 vs prevalence 0,312).

```text
GET  /v1/models/return-risk          → {"name":"return-risk","ready":true}
POST /v1/models/return-risk:predict  → sku-00002 0,346 · sku-00099 0,305 · sku-00003 0,254
```

Thứ tự điểm khớp với tỷ lệ trả hàng thật trong dữ liệu (sku-00002 khoảng 54%, sku-00003 khoảng 7%).
Cả ba đều `abstain: insufficient_history`, và điều đó là **đúng**: Redis giữ ảnh chụp mới nhất, còn đơn cuối cùng
đặt ngày 2026-02-14, nên trong cửa sổ 90 ngày không còn line nào có nhãn. Model vẫn trả score để tham khảo nhưng
không khẳng định.

Lỗi gặp khi deploy: client MLflow tải artifact bằng presigned URL trỏ tới `minio:9000`, nhưng tên này chỉ tồn tại
trong mạng Docker. Cách sửa: script tải thẳng `model.lgb` qua endpoint proxy `/api/2.0/mlflow-artifacts/...`.

## Metric

Pod tự khai annotation `prometheus.io/scrape|port=8080|path=/metrics`. Không dùng cờ `enable-prometheus-scraping`
của KServe, vì cờ đó trỏ tới cổng 9091 của một sidecar gộp metric, mà chế độ RawDeployment không có sidecar này.
Ngoài histogram độ trễ có sẵn của KServe (`request_predict_seconds`…), predictor xuất thêm metric nghiệp vụ:

| Metric | Ý nghĩa |
|---|---|
| `return_risk_predictions_total{abstain}` | số dòng đã chấm, tách theo có abstain hay không |
| `return_risk_errors_total{reason}` | request lỗi (vd `missing_field` → HTTP **400**, không phải 500) |
| `return_risk_feast_lookup_seconds` | thời gian đọc feature online |

Kiểm qua Prometheus (route `metrics.`): gửi 5 request, mỗi request 2 dòng, và thêm 1 request thiếu trường.
Prometheus đếm được `predictions{abstain="true"} = 10`, `errors{reason="missing_field"} = 1`, `feast_lookup_count = 5`.

**Bẫy:** nếu deploy lại với cùng tag `v3`, node kind vẫn giữ image cũ theo tag đó, nên pod chạy code cũ. Script
giờ gắn tag dạng `v<version>-<băm code+model>` (ví dụ `v3-d18e5f42`), mỗi bản build có một tag riêng.

</details>

[Mục lục phần này](README.md) · [Mục lục chung](../README.md)
