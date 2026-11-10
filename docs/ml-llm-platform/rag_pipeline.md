# RAG Pipeline on AWS

A pipeline that turns text into chunks (small passages) and embeddings (numeric vectors), plus its governance (data management) part.

RAG (Retrieval-Augmented Generation) is a way to let an LLM answer from documents: before answering, the system finds the document passages closest in meaning to the question and gives them to the LLM to read.

Code / config: [DAG](../../dags/ml_rag_ingest.py), [pipeline](../../rag/pipeline.py).

The results below come from runs on AWS; the docs do not claim the services are still up.

## Completed results on AWS

**Conclusion:** [DAG ml_rag_ingest](../../dags/ml_rag_ingest.py) ran **all 6/6 tasks successfully**, including the final `publish` step.

The steps in order: fetch the source text, split it into chunks, compute embeddings, load them into pgvector (Postgres with vector search support), validate, then publish the results to DataHub.
Source text, chunks and embeddings are stored on S3; pgvector runs on EKS; Airflow and DataHub run on EC2.

![Airflow on AWS: ml_rag_ingest succeeded, including publish](../evidence/aws/airflow-ml-rag-ingest-publish-20261031.png)

![DataHub on AWS: lineage corpus → S3 raw → chunks → embedding → pgvector → validate](../evidence/aws/datahub-rag-lineage-20261031.png)

The two screenshots above show the pipeline ran on AWS and that the lineage (a map of where data comes from and goes to) is in DataHub.
To make the publish step work, two bugs were fixed: how ASCII characters are escaped in aspect values, and the union type of `DataJobInfo.type`.
Processing and governance code: [rag/pipeline.py](../../rag/pipeline.py); feature store: [Feast RAG](../../rag/feature_repo/feature_store.yaml).

## On AWS

DAG `ml_rag_ingest` runs on Airflow on EC2. Chunks and embeddings are written to S3; the online store (looked up at serving time) is pgvector on EKS (NodePort 30432).

On the first run, the `load` task failed with `type "vector" does not exist`. Cause: the `pgdata` PVC had just been recreated, so the database was new and the
`vector` extension was not enabled. The pgvector image only installs the extension; it does not enable it.

Fix:

- [`pgvector.yaml`](../../infra/k8s/rag/pgvector.yaml) adds an initdb ConfigMap that runs `CREATE EXTENSION IF NOT EXISTS vector`. This command only runs automatically when the PVC is newly created.
- For the running database, enable the extension by hand once.
- The test `test_fresh_pgvector_volume_enables_vector_extension` keeps this bug from coming back.

![ml_rag_ingest on AWS: upload, chunk, embed, load, validate green; publish skipped because DataHub was not running](../evidence/aws/airflow-ml-rag-ingest-20261030.png)

In the run in this screenshot, `publish` was skipped because the Variable `platform_datahub_gms_url` was not set. The run in the section above completed the publish step as well.

### Feature API serving RAG on EKS

Running `deploy_feature_api.sh` with `FEAST_RAG_REGISTRY=s3://…/feature-store/rag-registry.db` enables the `/chunks` endpoint and the MCP tool
`search_policy_chunks`. Sending the question "What is Amazon's return window for apparel?" through the HTTPS ingress returns HTTP 200; the closest
chunk is the "Return window" section of the `amazon-returns` document (30 days).

![Swagger POST /chunks on EKS: 200, top 2 chunks from amazon-returns](../evidence/aws/rag-chunks-swagger-20261030.png)

Two problems hit while enabling it:

1. **The pod did not pick up the new variable.** The script only edits the Secret `feature-api-env`. The pod template did not change, so Kubernetes
   did not create a new pod, and `/chunks` still returned 503. Workaround: run `kubectl rollout restart deployment/feature-api` to recreate the pod.
2. **model-agent did not call the tool.** Qwen 1.5B replied "I'm sorry…" or just wrote out the text "Call search_policy_chunks…" instead of actually calling the tool.
   Calling llama.cpp directly on EKS was tried (temperature 0.1, 3 policy questions, same system prompt):
   with the tool description written in Vietnamese, the model called the tool 0/3 times; in English, 3/3 times.
   An MCP tool's docstring is the description the LLM reads, so the 4 docstrings in
   [`serving/app.py`](../../serving/app.py) were changed to English. The test `test_mcp_lists_tool` fails if a description contains non-ASCII characters.
   Built image `f97cba1-mcpen`, then restarted `model-agent` and `data-agent` so they reload the tool list.

![Before the fix: model-agent answers "no information" and does not call the tool](../evidence/aws/rag-model-agent-no-tool-20261030.png)

![After the fix: model-agent calls search_policy_chunks with query "Amazon return window for apparel"](../evidence/aws/rag-model-agent-tool-call-20261030.png)

![Tool result (3 chunks) and the answer: Amazon, 30 days from the expected delivery date](../evidence/aws/rag-model-agent-answer-20261030.png)

The answer also mentions "Shopee 15 days" (from the 3rd chunk) even though the question did not ask about it: the small model summarizes the off-topic chunk too.

<details>
<summary>Reference: design, development guide and records from before the AWS deployment</summary>

Local records keep their original environment and are not a substitute for cloud evidence.

<!-- Purpose: RAG pipeline text → chunk → embedding → Feast (pgvector), -->
## RAG pipeline — LLM Platform

The pipeline takes text data, splits it into chunks, then stores each chunk's embedding **in the feature store through Feast**.
The pipeline's governance part consists of lineage and validation results in DataHub.

```
rag/corpus/*.md (return policies for 5 Channels + Seller playbook, simulated)
   │ upload
   ▼
s3://platform/rag/raw/*.md ──chunk──► s3://platform/rag/chunks_text.parquet
                                     │ embed (fastembed BAAI/bge-small-en-v1.5, 384 dims, normalized)
                                     ▼
                         s3://platform/rag/chunks.parquet   ← offline source of Feast `platform_rag`
                                     │ load: feast apply + write_to_online_store
                                     ▼
              Postgres + pgvector (kind ns rag)  table platform_rag_policy_chunks   ← online store
                                     │ validate → receipt → publish (DataHub)
                                     ▼
       Feature API: POST /chunks, MCP tool search_policy_chunks → retrieve_online_documents_v2 (cosine)
```

| Part | File |
|---|---|
| Source documents (simulated, with a note at the top of each file) | [`rag/corpus/`](../../rag/corpus/seller-return-playbook.md) |
| Chunking + embedding | [`rag/chunking.py`](../../rag/chunking.py) |
| Pipeline steps + validate + DataHub lineage | [`rag/pipeline.py`](../../rag/pipeline.py) |
| Second Feast (Postgres/pgvector online store) | [`rag/feature_repo/features.py`](../../rag/feature_repo/features.py), [`feature_store.yaml`](../../rag/feature_repo/feature_store.yaml) |
| Airflow DAG | [`dags/ml_rag_ingest.py`](../../dags/ml_rag_ingest.py) |
| pgvector in kind | [`infra/k8s/rag/pgvector.yaml`](../../infra/k8s/rag/pgvector.yaml) |
| Tests (chunk, lineage) | [`rag/tests/`](../../rag/tests/test_chunking.py) |

## Why a second Feast

The SKU feature Feast uses Redis as its online store, and Redis cannot do vector search. Feast supports vector search when
the online store is Postgres with pgvector (`vector_enabled: true`, an `Array(Float32)` field with `vector_index=True`,
`vector_search_metric="cosine"`). So a separate Feast project `platform_rag` was created (registry `s3://platform/feature-store/rag-registry.db`)
so the running SKU Feast is not affected.

## Chunk

Each `## ` section of a document is one chunk. The chunk starts with "Document title — Section title:" so the chunk still makes sense
on its own. Sections longer than 700 characters are split by sentence, with 100 characters of overlap between consecutive pieces.
`chunk_id` is a hash of the content, so a rerun overwrites the same row instead of creating a duplicate.
Verified: running `load` twice still leaves 29 rows in the online table.

## Validate (each check = number of violations, 0 means pass)

| Check | Meaning |
|---|---|
| `online_chunk_count_mismatch` | number of entities in the pgvector table ≠ number of Parquet chunks |
| `duplicate_chunk_ids` | duplicate `chunk_id` |
| `empty_text` | empty chunk |
| `wrong_dimension` | vector not 384-dimensional |
| `not_unit_norm` | vector not normalized (cosine would be wrong) |
| `probe_wrong_top_doc` | the 3 sample questions do not return the right document first |

The validation results (receipt) are written to `out/receipts/rag/<run_id>/validate.json`. The `publish` step turns each check into an assertion
in DataHub, and also registers the datasets (S3 raw, Parquet, pgvector table), the jobs for each DAG task, and the lineage edges connecting them.

## Local results

Each step was run in a Python 3.10 container (same version as Airflow), connected to real MinIO and pgvector.
Result: 29 chunks from 6 documents, **all 6 checks at 0 (no violations)**, and the 3 sample questions all returned the right document
([receipt](../evidence/rag-validate-local-20261021.json)).

A test search for "Shopee buyer says the parcel was damaged, what can I do?" returned 3 chunks, all from `shopee-returns`.
The attached numbers are cosine distances, smaller means closer: Common reasons 0.242 · Dispute flow 0.298 · Return window 0.311.

Problems hit and fixed:
- Feast 0.66 does not accept the `vector_len` key in `feature_store.yaml` (the length is declared on the Field).
- Feast defaults to `sslmode=require`, but local pgvector does not enable TLS. So `sslmode: disable` is set (switch it back on the cloud).
- Postgres's `retrieve_online_documents_v2` picks the right top_k results but returns them unordered. So
  `search()` re-sorts them by `distance`.

## Running through Airflow

The Airflow image was rebuilt: feast-venv adds `feast[postgres]` and `fastembed==0.8.1` (installed with uv, because pip took more than 10 minutes just to resolve
dependencies). Python stays at 3.10 for Spark compatibility. `scripts/setup/setup_airflow.sh` loads the Connection `platform_rag_pg` and the Variable `platform_rag_root`.
Run `rag_local_202610211739`:

| Task | Status | Duration |
|---|---|---|
| upload | success | 2 s |
| chunk | success | 2 s |
| embed | success | 13 s (the model sits in the Airflow volume after the first run) |
| load | success | 26 s (`feast apply` + `write_to_online_store`) |
| validate | success | 11 s — 6/6 checks = 0, the 3 sample questions return the right document |
| publish | skipped | Variable `platform_datahub_gms_url` not set (this local run had no DataHub; the publish step has run on AWS, see above) |

Receipt: [`evidence/rag-validate-airflow-20261021.json`](../evidence/rag-validate-airflow-20261021.json). CI/CD:
[`ci/jenkins/rag-pipeline.Jenkinsfile`](../../ci/jenkins/rag-pipeline.Jenkinsfile).

</details>

[Section index](README.md) · [Main index](../README.md)
