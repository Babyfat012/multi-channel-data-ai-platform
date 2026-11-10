# Novel Ideas — Agent Evaluation và Drift Monitoring

EDAI-2, dòng 56–57: hai ý tưởng mới và bằng chứng chạy trên AWS.

| Ý tưởng | Kết quả AWS | Tài liệu |
|---|---|---|
| Bộ chấm Agent tự động (agent eval harness) | Chạy trong Jenkins trên EC2, build `feature-api-agents` #8: đúng 12/12 câu, cổng yêu cầu ≥ 0,95. Có giới hạn về PII và SKU (xem tài liệu). | [Agent Evaluation](agent_evaluation.md) |
| Phát hiện drift theo Channel (kênh bán) | Job Evidently trên EKS. Phép so `stable` không báo động. Phép so `channel_shift` báo drift ở cột `seller_sku`: khoảng cách JS (Jensen-Shannon) 0,2028, vượt ngưỡng 0,2. | [Drift Monitoring](drift_monitoring.md) |

[Mục lục phần này](README.md) · [Mục lục chung](../README.md)
