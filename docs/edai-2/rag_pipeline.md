# RAG Pipeline trên AWS

EDAI-2, dòng 9–10: pipeline text/chunk/embedding và governance.

Code / cấu hình: [DAG](../../dags/ml_rag_ingest.py), [pipeline](../../rag/pipeline.py).

Các kết quả dưới đây thuộc các lần chạy AWS, không xác nhận dịch vụ đang bật.

## Kết quả hoàn tất trên AWS

[DAG ml_rag_ingest](../../dags/ml_rag_ingest.py) chạy **6/6 task thành công**, gồm `publish`. Luồng: text → chunk → embedding → pgvector → validate → publish DataHub; raw/chunk/embedding ở S3, pgvector trên EKS, Airflow và DataHub trên EC2.

![Airflow AWS: ml_rag_ingest thành công, gồm publish](../evidence/aws/airflow-ml-rag-ingest-publish-20261031.png)

![DataHub AWS: lineage corpus → S3 raw → chunks → embedding → pgvector → validate](../evidence/aws/datahub-rag-lineage-20261031.png)

Hai ảnh chứng minh pipeline và lineage RAG đã chạy trên AWS. Bước publish đã sửa escape ASCII của value aspect và union `DataJobInfo.type`. Code xử lý và governance: [rag/pipeline.py](../../rag/pipeline.py); feature store: [Feast RAG](../../rag/feature_repo/feature_store.yaml).

## Trên AWS

DAG `ml_rag_ingest` chạy trên Airflow EC2, chunk + embedding ghi S3, online store là pgvector trên EKS (NodePort
30432). Lần đầu `load` hỏng: `type "vector" does not exist`. PVC `pgdata` vừa tạo lại nên database mới chưa bật
extension; image pgvector chỉ cài sẵn, không tự bật. Sửa: [`pgvector.yaml`](../../infra/k8s/rag/pgvector.yaml) thêm
ConfigMap initdb `CREATE EXTENSION IF NOT EXISTS vector` (chạy khi PVC mới), DB đang chạy thì bật tay một lần; test
`test_fresh_pgvector_volume_enables_vector_extension`.

![ml_rag_ingest trên AWS: upload, chunk, embed, load, validate xanh; publish skipped vì DataHub không chạy](../evidence/aws/airflow-ml-rag-ingest-20261030.png)

Tại lần chạy, `publish` bỏ qua vì chưa đặt Variable `platform_datahub_gms_url`; lần chạy phía trên đã hoàn tất publish.

### Feature API phục vụ RAG trên EKS

`deploy_feature_api.sh` với `FEAST_RAG_REGISTRY=s3://…/feature-store/rag-registry.db` bật `/chunks` và MCP tool
`search_policy_chunks`. Câu hỏi "What is Amazon's return window for apparel?" qua ingress HTTPS trả 200, chunk gần
nhất là mục "Return window" của `amazon-returns` (30 ngày).

![Swagger POST /chunks trên EKS: 200, top 2 chunk của amazon-returns](../evidence/aws/rag-chunks-swagger-20261030.png)

Hai lỗi gặp khi bật:

1. **Pod không nhận biến mới.** Script chỉ đổi Secret `feature-api-env`; pod template không đổi nên Kubernetes
   không thay pod, `/chunks` vẫn 503. Mở khoá bằng `kubectl rollout restart deployment/feature-api`.
2. **model-agent không gọi tool.** Qwen 1,5B trả "I'm sorry…" hoặc viết chữ "Call search_policy_chunks…".
   Đo thẳng llama.cpp trên EKS (temperature 0,1, 3 câu chính sách, cùng system prompt): mô tả tool tiếng Việt
   0/3 lần gọi tool, tiếng Anh 3/3. Docstring MCP tool chính là mô tả LLM đọc ⇒ đổi 4 docstring trong
   [`serving/app.py`](../../serving/app.py) sang tiếng Anh; `test_mcp_lists_tool` chặn mô tả không phải ASCII.
   Image `f97cba1-mcpen`, rồi restart `model-agent` + `data-agent` để nạp lại danh sách tool.

![Trước khi sửa: model-agent trả lời "không có thông tin", không gọi tool](../evidence/aws/rag-model-agent-no-tool-20261030.png)

![Sau khi sửa: model-agent gọi search_policy_chunks với query "Amazon return window for apparel"](../evidence/aws/rag-model-agent-tool-call-20261030.png)

![Kết quả tool (3 chunk) và câu trả lời: Amazon 30 ngày tính từ ngày giao dự kiến](../evidence/aws/rag-model-agent-answer-20261030.png)

Câu trả lời thêm ý Shopee 15 ngày (chunk thứ 3) dù không được hỏi: model nhỏ tóm tắt cả chunk lạc đề.

<details>
<summary>Tham khảo: thiết kế, hướng dẫn phát triển và ghi nhận trước triển khai AWS</summary>

Các ghi nhận local giữ nguyên môi trường, không dùng thay bằng chứng cloud.

<!-- Mục đích: pipeline RAG text → chunk → embedding → Feast (pgvector), -->
## RAG pipeline — LLM Platform

Pipeline kéo dữ liệu text → chia chunk → lưu embedding của chunk **vào feature store qua Feast**;
Governance pipeline RAG (lineage + validation trên DataHub).

```
rag/corpus/*.md (chính sách trả hàng 5 Channel + playbook Seller, giả lập)
   │ upload
   ▼
s3://platform/rag/raw/*.md ──chunk──► s3://platform/rag/chunks_text.parquet
                                     │ embed (fastembed BAAI/bge-small-en-v1.5, 384 chiều, chuẩn hoá)
                                     ▼
                         s3://platform/rag/chunks.parquet   ← offline source của Feast `platform_rag`
                                     │ load: feast apply + write_to_online_store
                                     ▼
              Postgres + pgvector (kind ns rag)  bảng platform_rag_policy_chunks   ← online store
                                     │ validate → receipt → publish (DataHub)
                                     ▼
       Feature API: POST /chunks, MCP tool search_policy_chunks → retrieve_online_documents_v2 (cosine)
```

| Phần | File |
|---|---|
| Tài liệu nguồn (giả lập, có ghi chú ở đầu mỗi file) | [`rag/corpus/`](../../rag/corpus/seller-return-playbook.md) |
| Cắt chunk + embedding | [`rag/chunking.py`](../../rag/chunking.py) |
| Các bước pipeline + validate + lineage DataHub | [`rag/pipeline.py`](../../rag/pipeline.py) |
| Feast thứ hai (online store Postgres/pgvector) | [`rag/feature_repo/features.py`](../../rag/feature_repo/features.py), [`feature_store.yaml`](../../rag/feature_repo/feature_store.yaml) |
| DAG Airflow | [`dags/ml_rag_ingest.py`](../../dags/ml_rag_ingest.py) |
| pgvector trong kind | [`infra/k8s/rag/pgvector.yaml`](../../infra/k8s/rag/pgvector.yaml) |
| Test (chunk, lineage) | [`rag/tests/`](../../rag/tests/test_chunking.py) |

## Vì sao Feast thứ hai

Feast của feature SKU dùng Redis làm online store; Redis không tìm theo vector. Feast hỗ trợ tìm vector với
online store Postgres + pgvector (`vector_enabled: true`, field `Array(Float32)` có `vector_index=True`,
`vector_search_metric="cosine"`). Tách project `platform_rag` (registry `s3://platform/feature-store/rag-registry.db`)
để không đụng Feast SKU đang chạy.

## Chunk

Mỗi mục `## ` của tài liệu là một chunk, đầu chunk ghi "Tiêu đề tài liệu — Tiêu đề mục:" để chunk tự đủ nghĩa
khi đứng một mình. Mục dài hơn 700 ký tự cắt theo câu, gối 100 ký tự. `chunk_id` = băm nội dung ⇒ chạy lại
ghi đè đúng dòng cũ, không nhân bản (kiểm: chạy `load` 2 lần, online vẫn 29 dòng).

## Validate (mỗi check = số vi phạm, 0 là đạt)

| Check | Ý nghĩa |
|---|---|
| `online_chunk_count_mismatch` | số entity trong bảng pgvector ≠ số chunk Parquet |
| `duplicate_chunk_ids` | trùng `chunk_id` |
| `empty_text` | chunk rỗng |
| `wrong_dimension` | vector khác 384 chiều |
| `not_unit_norm` | vector chưa chuẩn hoá (cosine sai) |
| `probe_wrong_top_doc` | 3 câu hỏi mẫu không ra đúng tài liệu ở vị trí đầu |

Receipt ghi vào `out/receipts/rag/<run_id>/validate.json`; bước `publish` biến mỗi check thành assertion
DataHub, kèm dataset (S3 raw, Parquet, bảng pgvector), job theo task DAG và cạnh lineage.

## Kết quả local

Chạy từng bước trong container Python 3.10 (cùng phiên bản với Airflow) nối MinIO + pgvector thật:
29 chunk từ 6 tài liệu, **6/6 check = 0**, 3 câu hỏi mẫu ra đúng tài liệu
([receipt](../evidence/rag-validate-local-20261021.json)).

Tìm thử "Shopee buyer says the parcel was damaged, what can I do?" → 3 chunk của `shopee-returns`
(Common reasons 0,242 · Dispute flow 0,298 · Return window 0,311).

Lỗi đã gặp và sửa:
- Feast 0.66 không nhận khoá `vector_len` trong `feature_store.yaml` (độ dài khai ở Field).
- Feast mặc định `sslmode=require`; pgvector local không bật TLS ⇒ `sslmode: disable` (Cloud đổi lại).
- `retrieve_online_documents_v2` của Postgres chọn đúng top_k nhưng trả về không theo thứ tự ⇒
  `search()` tự sắp theo `distance`.

## Chạy qua Airflow

Image Airflow build lại: feast-venv thêm `feast[postgres]` + `fastembed==0.8.1` (cài bằng uv; pip mất >10 phút giải phụ
thuộc), Python giữ 3.10 cho Spark. `scripts/setup/setup_airflow.sh` nạp Connection `platform_rag_pg` và Variable `platform_rag_root`.
Run `rag_local_202610211739`:

| Task | Trạng thái | Thời gian |
|---|---|---|
| upload | success | 2 s |
| chunk | success | 2 s |
| embed | success | 13 s (model nằm trong volume Airflow sau lần đầu) |
| load | success | 26 s (`feast apply` + `write_to_online_store`) |
| validate | success | 11 s — 6/6 check = 0, 3 câu hỏi mẫu ra đúng tài liệu |
| publish | skipped | chưa đặt Variable `platform_datahub_gms_url` (lần chạy local này chưa dựng DataHub; bước publish đã chạy trên AWS ở mục trên) |

Receipt: [`evidence/rag-validate-airflow-20261021.json`](../evidence/rag-validate-airflow-20261021.json). CI/CD:
[`ci/jenkins/rag-pipeline.Jenkinsfile`](../../ci/jenkins/rag-pipeline.Jenkinsfile).

</details>

[Mục lục phần này](../README.md) · [Mục lục chung](../../README.md)
