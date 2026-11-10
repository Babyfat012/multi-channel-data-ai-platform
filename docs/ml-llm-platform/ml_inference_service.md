# ML Inference Service on EKS

KServe with model-inside-image.

Code / config: [InferenceService](../../infra/k8s/ml/return-risk.yaml), [deployment script](../../scripts/run/deploy_return_risk.sh).

The results below come from runs on AWS and do not confirm the service is up.

## On EKS

The InferenceService [`return-risk`](../../infra/k8s/ml/return-risk.yaml) runs in namespace `ml-serving`. It is packaged as
model-inside-image: the model file is baked into the image, not downloaded at startup. The ECR image contains the MLflow model `return-risk`
v1 (alias `production`), built with [`deploy_return_risk.sh`](../../scripts/run/deploy_return_risk.sh).

Test: call the predictor directly through port-forward with one order of APP-00042, Amazon US, 1 unit, 25 USD. Result `score` 0.1661,
`abstain` false, `model_version` "1". This is also the number the Feature API `/predict` and model-agent return
(see [Web API](web_api_mcp.md#on-eks)).

![KServe on EKS: inferenceservice return-risk READY True, predictor pod Running, POST :predict returns score 0.1661 model_version 1](../evidence/aws/kserve-pod-predict-20261031.png)

**Multiple replicas and autoscaling.** Configuration:
- `minReplicas: 2`, `maxReplicas: 3`.
- `autoscalerClass: external`, because EKS has no metrics-server for HPA to scale on CPU.
- KEDA scales on the metric `return_risk_predictions_total`, threshold 5 predictions/second per pod.

Result when sending load to `:predict` for 150 seconds: up to 3 pods after about 48 s, back to 2 pods after about 495 s
([log](../evidence/aws/return-risk-autoscale.txt), [screenshots in Agents](agents.md#model-agent-has-the-same-full-set-as-data-agent)).

<details>
<summary>Reference: design, development guide and records from before the AWS deployment</summary>

Local records keep their original environment and are not a substitute for cloud evidence.

<!-- Purpose: serve the return-risk model with KServe (model-inside-image) -->
## Serve model return-risk — KServe

Goal: KServe serves the freshly trained model as **model-inside-image**. Evidence: the KServe pod, the YAML file and
a screenshot of a successful test call.

## Call contract

`POST http://return-risk-predictor.ml-serving.svc.cluster.local/v1/models/return-risk:predict`

```json
{"instances": [{"seller_sku": "sku-00002", "channel": "tiktok_shop", "country": "GB",
                "ordered_basis_qty": 3, "ordered_product_net_minor": 30000}]}
```

Returns `{"predictions": [{"seller_sku", "score", "abstain", "reason", "model_version"}]}`.

- `score` is a **ranking** signal, not a probability (Q38).
- The caller only sends order attributes. The predictor **reads** the SKU features from Feast online (Redis) itself, so the feature
  list is declared in one place only.
- `abstain` means the model declines to make a claim. A SKU with fewer than 20 labeled lines in 90 days returns
  `abstain: true, reason: insufficient_history`. This rule only applies when `labeled_count_90d` has a value.
- If a count is missing (`null`/NaN), the predictor does not call LightGBM for that row and returns `score: null, abstain: true,
  reason: feature_data_unavailable`. The predictor does not treat a missing count as 0.
- In a batch with both rows that have counts and rows that don't, only rows with counts are scored; the result order matches the input.
  Other nullable features keep their current handling.
- The missing-count handling is a change in the repo that has not been verified in an AWS deployment.

| Component | File |
|---|---|
| Predictor | [`ml/serve.py`](../../ml/serve.py), test [`tests/unit/test_serve.py`](../../tests/unit/test_serve.py) |
| Image (model inside the image) | [`docker/Dockerfile.serve`](../../docker/Dockerfile.serve) |
| InferenceService (RawDeployment) | [`infra/k8s/ml/return-risk.yaml`](../../infra/k8s/ml/return-risk.yaml) |
| Deploy: MLflow alias `production` → image → kind → apply | [`scripts/run/deploy_return_risk.sh`](../../scripts/run/deploy_return_risk.sh) |

## Installing KServe (local)

RawDeployment: no Knative/Istio needed, only cert-manager + the controller.

```bash
K="kubectl --context kind-platform"
$K apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.21.2/cert-manager.yaml
$K -n cert-manager wait --for=condition=Available deploy --all --timeout=300s
helm --kube-context kind-platform install kserve-crd oci://ghcr.io/kserve/charts/kserve-crd --version v0.21.0
helm --kube-context kind-platform install kserve oci://ghcr.io/kserve/charts/kserve-resources --version v0.21.0 \
  -n kserve --create-namespace --set kserve.controller.deploymentMode=RawDeployment
scripts/run/deploy_return_risk.sh
```

## Evidence

Deploy (kind, KServe v0.21.0 RawDeployment): `return-risk` READY = True, pod
`return-risk-predictor-…` Running, image `platform-return-risk:v3` = MLflow `return-risk` v3 (alias
`production`, PR-AUC 0.341 vs baseline 0.329 vs prevalence 0.312).

```text
GET  /v1/models/return-risk          → {"name":"return-risk","ready":true}
POST /v1/models/return-risk:predict  → sku-00002 0.346 · sku-00099 0.305 · sku-00003 0.254
```

The score order matches the actual return rates in the data (sku-00002 about 54%, sku-00003 about 7%).
All three are `abstain: insufficient_history`, and that is **correct**: Redis holds the latest snapshot, and the last order
was placed on 2026-02-14, so there are no labeled lines left in the 90-day window. The model still returns a score for reference but
makes no claim.

Problem hit during deploy: the MLflow client downloads artifacts with a presigned URL pointing to `minio:9000`, but that name only exists
inside the Docker network. Fix: the script downloads `model.lgb` directly through the proxy endpoint `/api/2.0/mlflow-artifacts/...`.

## Metric

The pod declares the annotation `prometheus.io/scrape|port=8080|path=/metrics` itself. KServe's `enable-prometheus-scraping` flag is not used,
because it points to port 9091 of a metric-aggregating sidecar, and RawDeployment mode has no such sidecar.
Besides KServe's built-in latency histograms (`request_predict_seconds`…), the predictor exports business metrics:

| Metric | Meaning |
|---|---|
| `return_risk_predictions_total{abstain}` | number of rows scored, split by abstain or not |
| `return_risk_errors_total{reason}` | failed requests (e.g. `missing_field` → HTTP **400**, not 500) |
| `return_risk_feast_lookup_seconds` | time to read online features |

Checked through Prometheus (route `metrics.`): sent 5 requests of 2 rows each, plus 1 request with a missing field.
Prometheus counted `predictions{abstain="true"} = 10`, `errors{reason="missing_field"} = 1`, `feast_lookup_count = 5`.

**Pitfall:** redeploying with the same tag `v3` leaves the kind node holding the old image for that tag, so the pod runs old code. The script
now tags images as `v<version>-<hash of code+model>` (e.g. `v3-d18e5f42`), so each build gets its own tag.

</details>

[Section index](README.md) · [Main index](../README.md)
