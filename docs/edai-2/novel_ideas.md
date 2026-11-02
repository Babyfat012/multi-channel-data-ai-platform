# Novel Ideas — Agent Evaluation và Drift Monitoring

EDAI-2, dòng 56–57: ý tưởng và proof trên AWS.

| Ý tưởng | Kết quả AWS | Tài liệu |
|---|---|---|
| Agent eval harness | Jenkins EC2, feature-api-agents #8: 12/12, gate ≥0,95; có giới hạn PII/SKU | [Agent Evaluation](agent_evaluation.md) |
| Drift theo Channel | Evidently EKS: stable không báo, channel_shift có seller_sku JS 0,2028 >0,2 | [Drift Monitoring](drift_monitoring.md) |

[Mục lục phần này](README.md) · [Mục lục chung](../README.md)
