# ML Inference Service trên EKS

EDAI-2, dòng 15: KServe với model-inside-image.

Code / cấu hình: [InferenceService](../../infra/k8s/ml/return-risk.yaml), [deployment script](../../scripts/run/deploy_return_risk.sh).

Các kết quả dưới đây thuộc các lần chạy AWS, không xác nhận dịch vụ đang bật.

## Trên EKS

InferenceService [`return-risk`](../../infra/k8s/ml/return-risk.yaml) ở namespace `ml-serving` (model-inside-image: image
ECR chứa model MLflow `return-risk` v1 `production`, build bằng [`deploy_return_risk.sh`](../../scripts/run/deploy_return_risk.sh)). Gọi thẳng predictor qua
port-forward, đơn APP-00042 Amazon US 1 cái 25 USD ⇒ `score` 0,1661, `abstain` false, `model_version` "1" — cùng số
Feature API `/predict` và model-agent trả (xem [Web API](web_api_mcp.md#trên-eks)).

![KServe trên EKS: inferenceservice return-risk READY True, pod predictor Running, POST :predict trả score 0.1661 model_version 1](../evidence/aws/kserve-pod-predict-20261031.png)

**Multi-replica và autoscale.** `minReplicas: 2`, `maxReplicas: 3`, `autoscalerClass: external` (EKS không có metrics-server cho HPA CPU), KEDA scale theo `return_risk_predictions_total`, ngưỡng 5 dự đoán/giây mỗi pod. Tải `:predict` 150 giây: lên 3 pod sau ~48 s, về 2 sau ~495 s ([log](../evidence/aws/return-risk-autoscale.txt), [ảnh trong Agents](agents.md#model-agent-đủ-bộ-như-data-agent)).

<details>
<summary>Tham khảo: thiết kế, hướng dẫn phát triển và ghi nhận trước triển khai AWS</summary>

Các ghi nhận local giữ nguyên môi trường, không dùng thay bằng chứng cloud.

<!-- Mục đích: phục vụ model return-risk bằng KServe (model-inside-image) -->
## Serve model return-risk — KServe

KServe serve model vừa train, **model-inside-image**; có pod KServe + YAML +
ảnh gọi thử thành công.

## Hợp đồng gọi

`POST http://return-risk-predictor.ml-serving.svc.cluster.local/v1/models/return-risk:predict`

```json
{"instances": [{"seller_sku": "sku-00002", "channel": "tiktok_shop", "country": "GB",
                "ordered_basis_qty": 3, "ordered_product_net_minor": 30000}]}
```

Trả về `{"predictions": [{"seller_sku", "score", "abstain", "reason", "model_version"}]}`.
`score` là tín hiệu **xếp hạng**, không phải xác suất (Q38). Người gọi chỉ gửi thuộc tính đơn:
predictor **tự đọc** feature SKU từ Feast online (Redis), nên danh sách feature chỉ nằm một chỗ.
SKU có < 20 line có nhãn trong 90 ngày ⇒ `abstain: true, reason: insufficient_history`.
Điều này chỉ áp dụng khi `labeled_count_90d` có giá trị. Nếu count thiếu (`null`/NaN),
predictor bỏ qua bước gọi LightGBM cho dòng đó và trả `score: null, abstain: true,
reason: feature_data_unavailable`; không tự đổi count thiếu thành 0. Batch hỗn hợp chỉ
chấm các dòng có count, giữ đúng thứ tự kết quả. Các feature nullable khác giữ cách xử lý
hiện tại. Đây là thay đổi trong repo, chưa kiểm chứng triển khai trên AWS.

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

Thứ tự khớp tỷ lệ trả thật trong dữ liệu (sku-00002 ~54%, sku-00003 ~7%). Cả ba `abstain:
insufficient_history` là **đúng**: Redis giữ ảnh chụp, còn đơn cuối đặt 2026-02-14 ⇒
cửa sổ 90 ngày không còn line có nhãn. Model vẫn trả score để tham khảo nhưng không khẳng định.

Lỗi gặp khi deploy: client MLflow tải artifact bằng presigned URL trỏ `minio:9000` (tên chỉ có trong
mạng Docker) ⇒ script tải `model.lgb` thẳng qua endpoint proxy `/api/2.0/mlflow-artifacts/...`.

## Metric

Pod khai `prometheus.io/scrape|port=8080|path=/metrics` (cờ `enable-prometheus-scraping` của KServe trỏ
cổng 9091 của sidecar gộp metric — RawDeployment không có sidecar đó). Ngoài histogram độ trễ của KServe
(`request_predict_seconds`…), predictor xuất metric nghiệp vụ:

| Metric | Ý nghĩa |
|---|---|
| `return_risk_predictions_total{abstain}` | số dòng đã chấm, tách theo có abstain hay không |
| `return_risk_errors_total{reason}` | request lỗi (vd `missing_field` → HTTP **400**, không phải 500) |
| `return_risk_feast_lookup_seconds` | thời gian đọc feature online |

Kiểm qua Prometheus (route `metrics.`): sau 5 request × 2 dòng + 1 request thiếu trường →
`predictions{abstain="true"} = 10`, `errors{reason="missing_field"} = 1`, `feast_lookup_count = 5`.

**Bẫy:** deploy lại cùng tag `v3` thì node kind giữ image cũ theo tag ⇒ pod chạy code cũ. Script giờ
gắn tag `v<version>-<băm code+model>` (vd `v3-d18e5f42`), mỗi bản build một tag.

</details>

[Mục lục phần này](../README.md) · [Mục lục chung](../../README.md)
