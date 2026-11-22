# Multi-channel Data & AI Platform

A data and AI platform for a Seller selling on Amazon, TikTok Shop, Shopify, Walmart and Shopee. The system normalizes multi-Channel data, predicts the Return or Refund risk of each Order Line, and provides an agent that answers questions with data. All data is synthetic.

## Contents

- [What does the platform provide?](#what-does-the-platform-provide)
- [Business problem](#business-problem)
- [Architecture](#architecture)
- [Getting started](#getting-started)
- [Documentation](#documentation)
- [Repository layout](#repository-layout)
- [Development and contributing](#development-and-contributing)
- [Validation results and limitations](#validation-results-and-limitations)
- [Known limitations](#known-limitations)
- [License](#license)

## What does the platform provide?

| Part | Function | Main technologies | Guide |
|---|---|---|---|
| **Data Platform** | Sources → Bronze / Silver / Gold → warehouse; batch, streaming, orchestration and governance | Spark, Kafka, Flink, Debezium, Iceberg, Lakekeeper, PostgreSQL, Trino, Airflow, DataHub | [Data Platform](docs/data-platform/README.md) |
| **ML Platform** | Feature store, training, model registry, serving and drift monitoring | Feast, Redis, LightGBM, Kubeflow Pipelines, MLflow, KServe, Evidently | [ML Platform](docs/ml-llm-platform/README.md) |
| **LLM Platform** | LLM inference, RAG, agents, gateway and answer evaluation | llama.cpp, pgvector, agentgateway, kagent, Langfuse, KEDA | [LLM Platform](docs/ml-llm-platform/README.md) |

On AWS: the Data Platform runs on one EC2 instance (Docker Compose) with data in S3; the ML Platform and LLM Platform run on EKS. Infrastructure is built with Terraform, and EC2 is configured with Ansible.

## Business problem

A Seller sells on seven Storefronts: Amazon US/UK/DE, TikTok Shop US, Shopify, Walmart US and Shopee VN. Each Channel exports data in its own schema; the system consolidates it into a Bronze / Silver / Gold lakehouse and then builds features for the problem:

> Will this Order Line be Returned or Refunded within 30 days of delivery?

The agent retrieves features, calls the prediction model and looks up the return policy through MCP. Details: [business domain](docs/shared/domain.md), [terminology](docs/shared/terminology.md), [label contract](docs/data-platform/reference/return_label_contract.md).

## Architecture

One diagram covers the whole platform: the data platform on EC2 (Docker Compose), the AWS managed services (S3 lakehouse, ECR), the ML + LLM platform on EKS, and CI/CD. All marketplace source data is simulated.

[![System architecture](docs/diagrams/system-architecture.png)](docs/diagrams/system-architecture.png)

[PNG](docs/diagrams/system-architecture.png) · [Editable Excalidraw](docs/diagrams/system-architecture.excalidraw) · [Blocks mapped to configuration and docs](docs/diagrams/README.md)

How to read it: each arrow goes from sender to receiver, its colour is the business flow, and its number gives the order inside that flow —
(B) batch, (S) CDC + streaming, (M) features + training, (R) return-policy RAG, (A) prediction API call, (C) AI chat, (K) control, (O) observability, (D) CI/CD.
A red "security" tag marks a security control. Grey dashed strips are pipelines and their steps.

The diagram shows the target operating model: pipelines run on schedules and a newly trained model is registered as a challenger that an ML engineer approves.
In the current code the Airflow DAGs are triggered manually and the training pipeline sets the `production` alias when the model beats the baseline.
Deployment evidence is in [AWS validation](docs/shared/aws_validation.md).

## Getting started

Requirements: Python 3.12+, `uv`, Docker Engine and Docker Compose v2. Run commands from the repository root.
Kubernetes is used for ML/LLM; the AWS deployment uses EKS `platform-apps`.

Start deploying from [Cloud setup](docs/shared/cloud_setup.md), then follow the [AWS runbook](docs/shared/aws_deploy.md) and [IaC](docs/ml-llm-platform/iac.md). Results so far and open items are in the [AWS validation status](docs/shared/aws_validation.md).

<details>
<summary>Set up local data sources for development</summary>

The development environment uses Docker Compose, and kind for Kubernetes.

```bash
cp .env.example .env          # set passwords and ports for your environment
uv sync --frozen
docker compose up -d --build
docker compose wait generator-load
```

This step builds the synthetic data sources with MinIO and PostgreSQL. Processing, ML and Agent services are enabled
per profile and have their own guides; see [Docker Compose](docs/data-platform/docker.md).

</details>

### Choose what to deploy

| Goal | Guide |
|---|---|
| Batch/stream processing, lakehouse and warehouse | [Data Platform](docs/data-platform/README.md) |
| Feature store, training and model serving | [ML Platform](docs/ml-llm-platform/README.md) |
| LLM inference, RAG and Agents | [LLM Platform](docs/ml-llm-platform/README.md) |
| AWS infrastructure and service configuration | [Cloud setup](docs/shared/cloud_setup.md), [AWS runbook](docs/shared/aws_deploy.md), [IaC](docs/ml-llm-platform/iac.md) |

## Documentation

The documentation follows the AWS deployments; each page has code/config, results and screenshot evidence.

| Part | Content | Docs |
|---|---|---|
| Data Platform | Generator, Spark/Flink, storage, orchestration, governance, schema, Trino federation and WAL CDC | [Data Platform](docs/data-platform/README.md) |
| ML/LLM Platform | LLM/Agent, RAG, Feature Store, training/serving, CI/CD, IaC and observability | [ML / LLM Platform](docs/ml-llm-platform/README.md) |
| Shared | Domain, AWS setup/runbook, architecture decisions and code conventions | [Shared](docs/shared/README.md) |
| Validation | AWS evidence and open gaps | [AWS validation status](docs/shared/aws_validation.md) |

[Full documentation index](docs/README.md).

## Repository layout

Top-level folders grouped by platform. Python packages keep their names (they are imported by that name), so the grouping lives in this table and in a one-line `README.md` inside each folder.

| Group | Folder | What it holds |
|---|---|---|
| **Data Platform** | `generator/` | Synthetic data for five Channels (skew, schema evolution, duplicates, late arrival) |
| | `configs/` | Generator data profiles (small → 100 GB), Iceberg maintenance config |
| | `contracts/` | Data contracts and per-Channel schemas |
| | `spark_jobs/` | Spark batch: Bronze, Silver, Gold, CDC, Iceberg maintenance |
| | `flink_jobs/` | Flink streaming: late arrival, dedup, windows, 24h features |
| | `sql/`, `trino/` | Warehouse SQL and Trino federation |
| | `dags/` | Airflow pipelines DP1–DP4, feature materialization, RAG |
| **ML Platform** | `feature_repo/` | Feast: feature views, TTL, stream push to offline/online stores |
| | `ml/` | Training (Kubeflow), evaluation, drift (Evidently), KServe predictor |
| | `serving/` | Feature API (FastAPI) + MCP tools |
| **LLM Platform** | `rag/` | Document chunking, embedding and retrieval |
| **Infrastructure** | `infra/docker/` | All Dockerfiles (the generator image is `infra/docker/Dockerfile`) |
| | `infra/aws/` | Terraform (EC2, S3, ECR, EKS) and Ansible roles for EC2 |
| | `infra/k8s/`, `infra/helm/` | Kubernetes manifests and the serving-api Helm chart (Feature API, Policy API, MCP server) |
| | `infra/jenkins/` | Jenkinsfiles and Jenkins configuration (JCasC) |
| | `docker-compose*.yml` | Compose stacks; kept at the root so `docker compose` finds them |
| **Scripts** | `scripts/data/` | Set up and run the Data Platform (Airflow, Lakekeeper, Debezium, order lifecycle) |
| | `scripts/ml/` | Deploy and run the ML Platform (KServe, Feature API, Redis, training, stream features) |
| | `scripts/llm/` | Deploy the LLM Platform (agents, agentgateway, Langfuse, agentregistry, Substrate) |
| | `scripts/shared/` | Shared: kind registry, images, ingress, TLS, Vault, Jenkins, observability, EKS access |
| | `scripts/evidence/` | Collect evidence from real runs |
| **Quality & docs** | `tests/` | Unit, Spark, Flink tests and test harnesses |
| | `notebooks/` | Analysis notebooks (executed, with output) |
| | `docs/` | Documentation: `data-platform/`, `ml-llm-platform/`, `shared/`, `diagrams/`, `evidence/` |

Script file names say what they do: `setup_*` installs once, `deploy_*` deploys or updates, `run_*` runs a job.

## Development and contributing

Read the [code conventions](docs/shared/code_style.md), the [glossary](GLOSSARY.md) and the docs for the component you are changing.
When submitting a change, describe the problem, the behavior after the fix and how you verified it; update related docs if usage changes.

```bash
uv sync --frozen
uv run pytest -q
uv run ruff format --check .
uv run ruff check .
```

These commands check the Python code and the default test suite. Instructions for testing Spark, the Web API and each service
are in the component docs.

## Validation results and limitations

The project uses synthetic data; measurements are tied to the environment, configuration and data scope of each run.
Run IDs and artifacts are kept in the validation docs to distinguish local results from AWS results.

- [Data Platform — evidence and limitations](docs/data-platform/evidence.md).
- [ML/LLM Platform — evidence and links to AWS results](docs/shared/aws_validation.md).
- [Validation status](docs/shared/aws_validation.md), including open items and weaknesses of the evidence.

## Known limitations

- [Agent eval](docs/ml-llm-platform/agent_evaluation.md): on EKS (Jenkins `feature-api-agents` #13) the Data Agent scores 12/12 and Model Agent predict 12/12, but **Model Agent policy (RAG) scores 3/5, below the 0.95 threshold, so the build is red**: Qwen 1.5B sometimes writes the tool call as text instead of actually calling the tool; calling the tool directly through MCP still returns chunks. The 12-SKU set has `labeled_count_90d` below 7 digits; the `phoneNumber` response mask hides 7-digit counts, which limits the top-seller turn to 9/12. The fix is to remove `phoneNumber` from the response mask; it is pending approval because it removes one guardrail layer.
- [Sandbox Agent](docs/shared/aws_validation.md): calls the tool correctly, but the 1.5B model reads `830847` as `83,084`; the tool-call path is proven, but answer quality from a small CPU model is not guaranteed.
- [Coordinator CI](docs/ml-llm-platform/agent_evaluation.md): the eval gate passes 12/12 on AWS (Jenkins #9) but **only scores routing** (whether `data_agent`/`model_agent` is called and a final answer is given), not numbers, because the 1.5B model often drops digits when copying them. One CPU-based LLM serves every Agent, so it is slow under overlapping load ([measurements](docs/ml-llm-platform/agent_evaluation.md#coordinator-on-aws-2026-11-04)).
- [Detailed screenshots](docs/shared/aws_validation.md): Spark UI Union/Window dedup, Flink RUNNING and late arrival, DBeaver ERD, Helm rollback, and KEDA for Model Agent/Coordinator have screenshots or logs on AWS; Feature API, MCP and KServe scale 2→3→2, measured with short requests inside the cluster, not production load. MCP shares code and image with the Feature API but runs in a separate pod so it can scale and roll back independently.
- [MLflow](docs/ml-llm-platform/training_pipeline.md): has Tags/Artifacts screenshots and a SHA-256 check of train/test against the tags on AWS; the link between an MLflow run and a KFP run is based on timestamps, because the KFP run ID is not recorded in MLflow.
- [Stream features](docs/ml-llm-platform/stream_features.md): two PyFlink jobs (online → Redis, offline → Parquet on S3); `approved_returns_24h` on AWS is replayed from Gold labels at a measured rate of about 60 messages/second on EC2, an estimated ~2 hours for 401,129 messages. The two jobs run independently, so the two stores can temporarily diverge; delivery is at-least-once, and recovery may rewrite a batch. Event time must be in the Kafka record timestamp: if the producer does not set the timestamp, the windows are wrong. The `return-risk` model on AWS was trained before the feature had history, so it does not use this feature yet.
- [Restarting EKS from 0 nodes](docs/shared/aws_deploy.md#restarting-eks-from-0-nodes): the Substrate valkey cluster keeps old pod IPs and must be re-joined with `cluster meet`; node IPs change, so Ansible must be re-run with `feast_redis` and `rag_pg`; `qwen`, scaled to 0 while idle, must be scaled back up before eval. Not automated yet. Redis and pgvector are reached through a single node IP, which changes when the node is replaced.
- [Shopee partitioning](docs/data-platform/data_storage.md): monthly partitioning is applied by a maintenance rewrite; DP1 `createOrReplace` would rewrite daily partitions, so this is not yet pipeline behavior.

## License

The project is released under the [Apache License 2.0](LICENSE).
