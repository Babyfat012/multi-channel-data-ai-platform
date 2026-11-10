# Agent Evaluation and Drift Monitoring

An automated Agent eval harness and per-Channel drift monitoring, with evidence from runs on AWS.

| Capability | AWS result | Doc |
|---|---|---|
| Automated Agent eval harness | Runs in Jenkins on EC2, build `feature-api-agents` #8: 12/12 questions correct, gate requires ≥ 0.95. Has limitations around PII and SKUs (see the doc). | [Agent Evaluation](agent_evaluation.md) |
| Per-Channel drift detection | Evidently Job on EKS. The `stable` comparison raises no alert. The `channel_shift` comparison reports drift on column `seller_sku`: JS (Jensen-Shannon) distance 0.2028, above the 0.2 threshold. | [Drift Monitoring](drift_monitoring.md) |

[Section index](README.md) · [Main index](../README.md)
