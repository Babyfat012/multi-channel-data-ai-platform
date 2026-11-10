# EDAI-2 — ML và LLM Platform trên AWS

Bộ tài liệu này đi theo từng dòng của rubric (bảng tiêu chí chấm) và tập trung vào các lần triển khai trên AWS.
Hạ tầng chia làm hai phần:

- **Data Platform** chạy bằng Docker Compose trên một máy EC2, dữ liệu nằm trên S3.
- **ML/LLM Platform** chạy trên cụm Kubernetes EKS tên `platform-apps`; riêng MLflow, Jenkins và Vault chạy trên EC2.

Mỗi kết quả đều ghi rõ chạy ở môi trường nào và còn giới hạn gì. Tài liệu chỉ ghi lại những lần đã chạy, không khẳng định các dịch vụ hiện vẫn đang bật.

![Kiến trúc triển khai AWS](../diagrams/ml-llm-platform-system-architecture-excalidraw.png)

## Đối chiếu rubric

Cột "Dòng rubric" là số dòng trong bảng tiêu chí. Cột cuối tóm tắt đã chứng minh được gì và còn thiếu gì.

| Dòng rubric | Tài liệu / bằng chứng | Kết quả và giới hạn |
|---|---|---|
| 3–6 | [LLM Inference Platform](llm_inference_platform.md) | Tự host LLM chạy bằng CPU trên EKS. Có ảnh triển khai (pod, PVC, log) và kết quả đo bằng llm-d-benchmark, so baseline với optimized: mỗi cấu hình 3 mức tải, 42 request, không lỗi; ở tải cao optimized giảm TTFT p50 46%. Chưa fine-tune model riêng. |
| 7 | [Global Model Config](global_model_config.md) | Có file cấu hình và ảnh cho thấy ba Agent dùng chung một model trên EKS. |
| 8, 21, 26 | [Agent Registry](agent_registry.md) | Có giao diện trên EKS, đăng ký 3 Agent và 1 MCP server. |
| 9–10 | [RAG Pipeline](rag_pipeline.md) | DAG chạy thành công cả 6/6 task, gồm bước publish lên DataHub; có ảnh lineage (sơ đồ nguồn gốc dữ liệu) trên DataHub AWS. |
| 11 | [Feature Store](feature_store.md) | Có DAG materialize (nạp feature vào kho online) trên AWS và dữ liệu thật trong Redis. |
| 12 | [Streaming Feature](stream_features.md) | Hai job PyFlink trên Flink 2.2 chạy trên AWS: một job ghi online vào Redis, một job ghi offline ra Parquet trên S3. Kiểm tra lại bằng script đếm riêng thì khớp 56.231/56.231 cửa sổ thời gian và 2.000/2.000 SKU, tính tới mốc đã đóng; **chưa có số của mốc cuối**. Model đã train chưa dùng feature này. |
| 13–14 | [Training Pipeline / MLflow](training_pipeline.md) | Pipeline KFP chạy đủ 5/5 bước. Có ảnh tab Tags/Artifacts của run, và mã băm SHA-256 của tập train/test khớp với giá trị ghi trong tag. |
| 15 | [ML Inference Service](ml_inference_service.md) | Có pod ở trạng thái READY và kết quả dự đoán trên EKS. |
| 16–18 | [Web API / MCP](web_api_mcp.md) | Có deployment, API và probe. MCP server (bốn tool) đã tách sang pod riêng `feature-mcp`; smoke test trên EKS sau khi tách đạt 24/24. Feature API và MCP tự scale 2→3→2 pod. Helm rolling update tự rollback khi gặp bản hỏng trên EKS (763/763 request trả 200). |
| 19–22 | [Data Agent / Model Agent / Sandbox](agents.md) | Data Agent và Model Agent có đủ cùng một bộ: chạy nhiều replica, KEDA scale 2→3→2, gọi tool trong Sandbox gVisor, đăng ký vào registry, gọi MCP qua gateway (smoke 24/24). KServe predict scale 2→3→2. Eval: Data 12/12, predict 12/12, **policy 3/5 (dưới ngưỡng nên build đỏ)**. |
| 23–24 | [MCP / A2A Gateway](agent_gateways.md) | Smoke test 24/24, kèm bảng cho phép/từ chối và ảnh ConfigMap cấu hình. Chưa có rate limit riêng cho MCP. |
| 25–26 | [Coordinator](coordinator.md) | Có chat và publish, scale 2 → 3 → 2 trên EKS. Cổng eval trong CI đạt 12/12 (build #9), nhưng chỉ chấm việc định tuyến câu hỏi. |
| 27 | [Warm Up](agent_fleet_warm_pool.md) | 6 Agent trên 1 worker; chưa đo thời gian khởi động nguội (cold start). |
| 28–32 | [Validation & Verification](validation_verification.md) | Test của PR chạy trên máy phát triển. Load test Locust gọi vào EKS: 3.004 request, không lỗi. |
| 33–36 | [CI/CD](cicd.md) | `stream-features` build #7 xanh, triển khai 2 job PyFlink trên AWS. `feature-api-agents` build #13 deploy xanh nhưng **đỏ ở bước eval RAG (3/5)**. Coordinator eval 12/12 (chỉ chấm định tuyến). |
| 37–43 | [External Routing / Gateway](routing_gateway.md) | Có HTTPS và xác thực; giới hạn về gia hạn chứng chỉ xem trong tài liệu. |
| 44–45 | [IaC](iac.md) | `terraform plan` báo No changes và Ansible chạy với `failed=0` trên AWS. |
| 46–51 | [Observability](observability.md) | Có dashboard metric/log/trace và telemetry từ EKS. |
| 52–53 | [A/B Testing](ab_testing.md) | Có chia traffic, dashboard và kết quả eval trên AWS. |
| 54 | [Security](security.md) | Có Vault trên EC2; sau mỗi lần restart phải unseal (mở khoá) bằng tay. |
| 56–57 | [Novel Ideas](agent_eval_and_drift.md) | Có bộ chấm Agent và Job đo drift trên AWS; chưa tự động train lại model. |

Mỗi trang có code/cấu hình, kết quả và ảnh kèm giải thích. Trạng thái từng dòng và những phần còn thiếu bằng chứng AWS: xem [kiểm kê rubric](../shared/aws_validation.md).

## Triển khai và kiểm chứng

[Cloud setup](../shared/cloud_setup.md) · [AWS runbook](../shared/aws_deploy.md) · [IaC](../ml-llm-platform/iac.md).

Repository Design (dòng 55): [README dự án](../../README.md), [quy ước code](../shared/code_style.md), mục lục và bảng đối chiếu rubric ở chính trang này.

[Tham khảo kỹ thuật](reference/README.md) · [Mục lục chung](../README.md).
