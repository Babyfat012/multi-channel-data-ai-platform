# Global Model Config

EDAI-2, dòng 7: cấu hình model dùng chung cho mọi Agent.

Mọi Agent không tự khai model. Chúng trỏ tới **một** `ModelConfig` tên `default-model-config`; đổi model một lần ở đó thì cả ba Agent
(Data, Model, Coordinator) đổi theo, không sửa manifest từng Agent.

Code: [Helm values kagent](../../infra/k8s/agents/kagent-values.yaml) (tạo `default-model-config`), [Data Agent](../../infra/k8s/agents/data-agent.yaml),
[Model Agent](../../infra/k8s/agents/model-agent.yaml), [Coordinator](../../infra/k8s/agents/coordinator.yaml),
[ModelConfig nhánh B của A/B](../../infra/k8s/agents/ab-data-agent.yaml).

## Cấu hình gồm những gì

| Trường | Giá trị | Vì sao |
|---|---|---|
| `provider` | `OpenAI` | llama.cpp server nói API kiểu OpenAI (`/v1/chat/completions`), nên dùng provider OpenAI chuẩn của kagent, không cần provider riêng |
| `model` | `qwen2.5-1.5b-instruct` | Trùng `--alias` của llama.cpp ([LLM Inference](llm_inference_platform.md)); gateway và llama.cpp nhận diện model theo tên này |
| `baseUrl` | `http://agentgateway.agentgateway.svc.cluster.local:3000/v1` | Agent **không gọi thẳng** llama.cpp mà đi qua agentgateway: có API key, rate limit, PII guardrail và số liệu token ([Gateways](agent_gateways.md)) |
| `apiKeySecretRef` / `apiKeySecretKey` | Secret `kagent-agw-key`, khóa `AGW_AGENT_KEY` | Key gateway nằm trong Secret Kubernetes, không nằm trong manifest |
| `temperature` | `0.1` | Agent gọi tool cần đầu ra ổn định, ít ngẫu nhiên |

## Luồng một lời gọi LLM của Agent

```
Agent (data / model / coordinator)
  └─ modelConfig: default-model-config
       └─ POST {baseUrl}/chat/completions  + Authorization: Bearer <AGW_AGENT_KEY>
            └─ agentgateway: route /v1 (API key, PII guardrail, rate limit)
                 └─ Service qwen.llm:8080 (llama.cpp, Qwen2.5-1.5B-Instruct GGUF)
```

Mọi lượt gọi LLM của cả ba Agent đi qua cùng một cửa nên telemetry LLM (token, TTFT, thời gian lượt) gom ở một chỗ:
[Observability](observability.md).

## Đổi model và so hai cấu hình

- **Đổi cho tất cả Agent:** sửa `model` ở `default-model-config` (và alias tương ứng ở llama.cpp). Agent không cần deploy lại manifest.
- **Thử một cấu hình khác cho một Agent:** tạo `ModelConfig` thứ hai rồi để Agent tham chiếu nó. A/B Testing làm đúng cách này:
  `ab-small-model-config` dùng `qwen2.5-0.5b-instruct`, cùng prompt và tool với Data Agent, chỉ khác model; so kết quả ở
  [A/B Testing](ab_testing.md).
- **Giới hạn:** cả ba Agent dùng chung một model CPU 1,5B nên chất lượng gọi tool bị chặn bởi model nhỏ (xem
  [Agent Evaluation](agent_evaluation.md)); ModelConfig chung cũng nghĩa là lỗi model ảnh hưởng mọi Agent cùng lúc.

## Bằng chứng

Ảnh EKS ngày 30/10/2026 thể hiện ba Agent dùng chung model:

![kagent UI EKS: ba Agent dùng qwen2.5-1.5b-instruct](../evidence/aws/kagent-ui-agents-20261030.png)

Kiểm tra bằng lệnh (chưa chụp trên EKS; lệnh chạy được với context EKS):

```bash
kubectl --context $KCTX -n kagent get modelconfig
kubectl --context $KCTX -n kagent get modelconfig default-model-config -o yaml   # provider, model, baseUrl, secret
kubectl --context $KCTX -n kagent get agent -o custom-columns=AGENT:.metadata.name,MODEL:.spec.declarative.modelConfig
```

[Mục lục phần này](README.md) · [Mục lục chung](../README.md)
