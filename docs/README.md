# Data & AI Platform documentation

The documentation is split into two parts and focuses on the system deployed on AWS in Sydney (`ap-southeast-2`). Results are tied to a date/run and do not confirm that services are currently running.

| Part | Scope | Start here |
|---|---|---|
| Data Platform | Generator, Spark/Flink, lakehouse/warehouse, Airflow, DataHub and schema | [Data Platform](data-platform/README.md) |
| ML/LLM Platform | LLM/Agent, RAG, feature store, training/serving, CI/CD and operations | [ML/LLM Platform](ml-llm-platform/README.md) |

The Data Platform runs Compose on EC2, with raw/lakehouse data in S3. ML/LLM runs on EKS `platform-apps`; MLflow, Jenkins and Vault run on EC2.

## Domain and deployment

[Business domain](shared/domain.md) · [Terminology](shared/terminology.md) · [Cloud setup](shared/cloud_setup.md) · [AWS runbook](shared/aws_deploy.md) · [IaC](ml-llm-platform/iac.md).

## Validation and evidence

[AWS validation status and open gaps](shared/aws_validation.md) · [Data Platform evidence](data-platform/evidence.md).

Screenshots are embedded in the component pages with an explanation of their purpose. `evidence/` holds receipts/logs/metrics; `images/` holds the original screenshots. Identifiers and figures of each run are kept unchanged. Local guides and pre-deployment notes are in the reference sections, with their own environment noted.

## Shared documentation

[Shared index](shared/README.md) · [Diagrams](diagrams/README.md) · [GLOSSARY](../GLOSSARY.md).

When updating: edit the component page, record the date/environment/limits of measurements, keep the original evidence, and check links with `uv run pytest -q tests/unit/test_docstrings.py`.

[Project README](../README.md)
