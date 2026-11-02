# EDAI-2 — ML và LLM Platform trên AWS

Tài liệu bám theo rubric, tập trung các lần triển khai AWS ngày 27–31/10/2026. Data Platform chạy Compose EC2/S3; ML/LLM chạy EKS `platform-apps`, MLflow/Jenkins/Vault trên EC2. Kết quả ghi rõ ngày chạy và giới hạn, không xác nhận dịch vụ đang bật.

![Kiến trúc triển khai AWS](../diagrams/ml-llm-platform-system-architecture-excalidraw.png)

## Đối chiếu rubric

| Dòng rubric | Tài liệu / bằng chứng | Kết quả và giới hạn |
|---|---|---|
| 3–6 | [LLM Inference Platform](llm_inference_platform.md) | CPU self-host, có benchmark; chưa fine-tune riêng. |
| 7 | [Global Model Config](global_model_config.md) | Có config và ảnh ba Agent dùng chung model EKS. |
| 8, 21, 26 | [Agent Registry](agent_registry.md) | Có UI EKS: 3 Agent, 1 MCP server. |
| 9–10 | [RAG Pipeline](rag_pipeline.md) | 31/10: 6/6 task, có publish và lineage DataHub AWS. |
| 11 | [Feature Store](feature_store.md) | Có DAG materialize AWS và dữ liệu Redis. |
| 12 | [Streaming Feature](stream_features.md) | Có replay/đối soát AWS; model đã train chưa dùng feature này. |
| 13–14 | [Training Pipeline / MLflow](training_pipeline.md) | KFP 5/5; thiếu ảnh hash/artifacts train-test cùng MLflow run. |
| 15 | [ML Inference Service](ml_inference_service.md) | Có pod READY và prediction EKS. |
| 16–18 | [Web API / MCP](web_api_mcp.md) | Có deployment/API; thiếu capture fallback AWS khi bản hỏng. |
| 19–22 | [Data Agent / Model Agent / Sandbox](agents.md) | Data Agent scale 2→3→2; Model Agent thiếu KEDA AWS. |
| 23–24 | [MCP / A2A Gateway](agent_gateways.md) | Smoke 17/17; chưa có rate limit riêng cho MCP. |
| 25–26 | [Coordinator](coordinator.md) | Có chat/publish; thiếu capture scale AWS, CI eval timeout. |
| 27 | [Warm Up](warm_up.md) | 6 Agent/1 worker; chưa đo cold start. |
| 28–32 | [Validation & Verification](validation_verification.md) | PR tests ở máy phát triển; Locust gọi EKS, 3.004 request/0 lỗi. |
| 33–36 | [CI/CD](cicd.md) | 3 pipeline xanh; Coordinator deploy xanh, eval chưa đạt. |
| 37–43 | [External Routing / Gateway](routing_gateway.md) | Có HTTPS/auth; giới hạn gia hạn cert xem tài liệu. |
| 44–45 | [IaC](iac.md) | Có plan No changes và Ansible failed=0 trên AWS. |
| 46–51 | [Observability](observability.md) | Có dashboard metric/log/trace và telemetry EKS. |
| 52–53 | [A/B Testing](ab_testing.md) | Có traffic split, dashboard và kết quả eval AWS. |
| 54 | [Security](security.md) | Có Vault EC2; unseal thủ công sau restart. |
| 56–57 | [Novel Ideas](novel_ideas.md) | Có Agent eval và drift Job AWS; chưa tự retrain. |

Mỗi trang có code/config, kết quả và ảnh có giải thích. Trạng thái và phần thiếu proof AWS: [kiểm kê rubric](../shared/rubric_cloud_audit.md).

## Triển khai và kiểm chứng

[Cloud setup](../shared/cloud_setup.md) · [AWS runbook](../shared/aws_deploy.md) · [IaC](../edai-2/iac.md).

Repository Design (dòng 55): [README dự án](../../README.md), [quy ước code](../shared/code_style.md), mục lục và đối chiếu rubric tại trang này.

[Tham khảo kỹ thuật](reference/README.md) · [Mục lục chung](../README.md).
