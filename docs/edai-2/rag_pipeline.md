# RAG Pipeline trên AWS

EDAI-2, dòng 9–10: pipeline xử lý văn bản thành chunk (đoạn nhỏ) và embedding (vector số), cùng phần governance (quản trị dữ liệu).

RAG (Retrieval-Augmented Generation) là cách cho LLM trả lời dựa trên tài liệu: trước khi trả lời, hệ thống tìm các đoạn tài liệu gần nghĩa nhất với câu hỏi rồi đưa cho LLM đọc.

Code / cấu hình: [DAG](../../dags/ml_rag_ingest.py), [pipeline](../../rag/pipeline.py).

Các kết quả dưới đây là của những lần chạy trên AWS; tài liệu không khẳng định dịch vụ hiện vẫn đang bật.

## Kết quả hoàn tất trên AWS

**Kết luận:** [DAG ml_rag_ingest](../../dags/ml_rag_ingest.py) chạy **thành công cả 6/6 task**, kể cả bước cuối `publish`.

Các bước theo thứ tự: lấy văn bản gốc, cắt thành chunk, tính embedding, nạp vào pgvector (Postgres có hỗ trợ tìm theo vector), kiểm tra (validate), rồi publish kết quả lên DataHub.
Văn bản gốc, chunk và embedding lưu trên S3; pgvector chạy trên EKS; Airflow và DataHub chạy trên EC2.

![Airflow AWS: ml_rag_ingest thành công, gồm publish](../evidence/aws/airflow-ml-rag-ingest-publish-20261031.png)

![DataHub AWS: lineage corpus → S3 raw → chunks → embedding → pgvector → validate](../evidence/aws/datahub-rag-lineage-20261031.png)

Hai ảnh trên chứng minh pipeline đã chạy trên AWS và lineage (sơ đồ dữ liệu đi từ đâu tới đâu) đã lên DataHub.
Để bước publish chạy được, đã sửa hai lỗi: cách escape ký tự ASCII trong value của aspect, và kiểu union của `DataJobInfo.type`.
Code xử lý và governance: [rag/pipeline.py](../../rag/pipeline.py); feature store: [Feast RAG](../../rag/feature_repo/feature_store.yaml).

## Trên AWS

DAG `ml_rag_ingest` chạy trên Airflow ở EC2. Chunk và embedding được ghi ra S3; online store (nơi tra cứu lúc phục vụ) là pgvector trên EKS (NodePort 30432).

Lần chạy đầu, task `load` lỗi `type "vector" does not exist`. Nguyên nhân: PVC `pgdata` vừa được tạo lại nên database còn mới, chưa bật
extension `vector`. Image pgvector chỉ cài sẵn extension, không tự bật.

Cách sửa:

- [`pgvector.yaml`](../../infra/k8s/rag/pgvector.yaml) thêm ConfigMap initdb chạy `CREATE EXTENSION IF NOT EXISTS vector`. Lệnh này chỉ tự chạy khi PVC mới được tạo.
- Với database đang chạy, bật extension bằng tay một lần.
- Test `test_fresh_pgvector_volume_enables_vector_extension` giữ cho lỗi này không quay lại.

![ml_rag_ingest trên AWS: upload, chunk, embed, load, validate xanh; publish skipped vì DataHub không chạy](../evidence/aws/airflow-ml-rag-ingest-20261030.png)

Ở lần chạy trong ảnh này, `publish` bị bỏ qua vì chưa đặt Variable `platform_datahub_gms_url`. Lần chạy ở mục trên đã hoàn tất cả bước publish.

### Feature API phục vụ RAG trên EKS

Chạy `deploy_feature_api.sh` với `FEAST_RAG_REGISTRY=s3://…/feature-store/rag-registry.db` sẽ bật endpoint `/chunks` và MCP tool
`search_policy_chunks`. Gửi câu hỏi "What is Amazon's return window for apparel?" qua ingress HTTPS thì nhận HTTP 200; chunk gần nghĩa
nhất là mục "Return window" của tài liệu `amazon-returns` (30 ngày).

![Swagger POST /chunks trên EKS: 200, top 2 chunk của amazon-returns](../evidence/aws/rag-chunks-swagger-20261030.png)

Hai lỗi gặp khi bật:

1. **Pod không nhận biến mới.** Script chỉ sửa Secret `feature-api-env`. Pod template không đổi nên Kubernetes
   không tạo pod mới, và `/chunks` vẫn trả 503. Cách gỡ: chạy `kubectl rollout restart deployment/feature-api` để tạo lại pod.
2. **model-agent không gọi tool.** Qwen 1,5B trả "I'm sorry…" hoặc chỉ viết ra chữ "Call search_policy_chunks…" thay vì gọi tool thật.
   Đã thử gọi thẳng llama.cpp trên EKS (temperature 0,1, 3 câu hỏi về chính sách, cùng system prompt):
   khi mô tả tool viết tiếng Việt, model gọi tool 0/3 lần; viết tiếng Anh thì 3/3 lần.
   Docstring của MCP tool chính là phần mô tả mà LLM đọc, nên đã đổi 4 docstring trong
   [`serving/app.py`](../../serving/app.py) sang tiếng Anh. Test `test_mcp_lists_tool` báo lỗi nếu mô tả có ký tự ngoài ASCII.
   Build image `f97cba1-mcpen`, rồi restart `model-agent` và `data-agent` để chúng nạp lại danh sách tool.

![Trước khi sửa: model-agent trả lời "không có thông tin", không gọi tool](../evidence/aws/rag-model-agent-no-tool-20261030.png)

![Sau khi sửa: model-agent gọi search_policy_chunks với query "Amazon return window for apparel"](../evidence/aws/rag-model-agent-tool-call-20261030.png)

![Kết quả tool (3 chunk) và câu trả lời: Amazon 30 ngày tính từ ngày giao dự kiến](../evidence/aws/rag-model-agent-answer-20261030.png)

Câu trả lời có thêm ý "Shopee 15 ngày" (lấy từ chunk thứ 3) dù câu hỏi không nhắc tới: model nhỏ tóm tắt luôn cả chunk lạc đề.

<details>
<summary>Tham khảo: thiết kế, hướng dẫn phát triển và ghi nhận trước triển khai AWS</summary>

Các ghi nhận local giữ nguyên môi trường, không dùng thay bằng chứng cloud.

<!-- Mục đích: pipeline RAG text → chunk → embedding → Feast (pgvector), -->
## RAG pipeline — LLM Platform

Pipeline lấy dữ liệu văn bản, chia thành chunk, rồi lưu embedding của từng chunk **vào feature store qua Feast**.
Phần governance của pipeline gồm lineage và kết quả kiểm tra (validation) trên DataHub.

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

Feast của feature SKU dùng Redis làm online store, mà Redis không tìm theo vector được. Feast có hỗ trợ tìm vector khi
online store là Postgres có pgvector (`vector_enabled: true`, field `Array(Float32)` có `vector_index=True`,
`vector_search_metric="cosine"`). Vì vậy tạo một project Feast riêng là `platform_rag` (registry `s3://platform/feature-store/rag-registry.db`)
để không ảnh hưởng tới Feast SKU đang chạy.

## Chunk

Mỗi mục `## ` của tài liệu là một chunk. Đầu chunk ghi "Tiêu đề tài liệu — Tiêu đề mục:" để chunk vẫn đủ nghĩa
khi đứng một mình. Mục dài hơn 700 ký tự được cắt theo câu, hai đoạn liền nhau gối lên nhau 100 ký tự.
`chunk_id` là mã băm của nội dung, nên khi chạy lại thì ghi đè đúng dòng cũ chứ không tạo bản trùng.
Đã kiểm: chạy `load` 2 lần, bảng online vẫn chỉ có 29 dòng.

## Validate (mỗi check = số vi phạm, 0 là đạt)

| Check | Ý nghĩa |
|---|---|
| `online_chunk_count_mismatch` | số entity trong bảng pgvector ≠ số chunk Parquet |
| `duplicate_chunk_ids` | trùng `chunk_id` |
| `empty_text` | chunk rỗng |
| `wrong_dimension` | vector khác 384 chiều |
| `not_unit_norm` | vector chưa chuẩn hoá (cosine sai) |
| `probe_wrong_top_doc` | 3 câu hỏi mẫu không ra đúng tài liệu ở vị trí đầu |

Kết quả kiểm tra (receipt) được ghi vào `out/receipts/rag/<run_id>/validate.json`. Bước `publish` biến mỗi check thành một assertion
trên DataHub, đồng thời đăng các dataset (S3 raw, Parquet, bảng pgvector), các job tương ứng với từng task DAG, và các cạnh lineage nối chúng.

## Kết quả local

Chạy từng bước trong container Python 3.10 (cùng phiên bản với Airflow), nối với MinIO và pgvector thật.
Kết quả: 29 chunk từ 6 tài liệu, **cả 6 check đều bằng 0 (không vi phạm)**, 3 câu hỏi mẫu đều ra đúng tài liệu
([receipt](../evidence/rag-validate-local-20261021.json)).

Tìm thử câu "Shopee buyer says the parcel was damaged, what can I do?" trả về 3 chunk, đều của `shopee-returns`.
Số kèm theo là khoảng cách cosine, càng nhỏ càng gần: Common reasons 0,242 · Dispute flow 0,298 · Return window 0,311.

Lỗi đã gặp và sửa:
- Feast 0.66 không nhận khoá `vector_len` trong `feature_store.yaml` (độ dài khai ở Field).
- Feast mặc định dùng `sslmode=require`, nhưng pgvector local không bật TLS. Vì vậy đặt `sslmode: disable` (trên Cloud thì đổi lại).
- `retrieve_online_documents_v2` của Postgres chọn đúng top_k kết quả nhưng trả về không theo thứ tự. Vì vậy
  `search()` tự sắp xếp lại theo `distance`.

## Chạy qua Airflow

Image Airflow được build lại: feast-venv thêm `feast[postgres]` và `fastembed==0.8.1` (cài bằng uv, vì pip mất hơn 10 phút chỉ để giải phụ
thuộc). Python giữ bản 3.10 để tương thích Spark. `scripts/setup/setup_airflow.sh` nạp Connection `platform_rag_pg` và Variable `platform_rag_root`.
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

[Mục lục phần này](README.md) · [Mục lục chung](../README.md)
