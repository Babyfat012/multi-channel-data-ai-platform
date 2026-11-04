# Notebook phân tích

Năm notebook đã chạy sẵn (có output). Bốn notebook đầu chỉ đọc file đã commit trong repo; notebook 05 cần thêm tập train 32 MB (xem bên dưới). Không notebook nào cần kết nối cụm AWS hay thư mục `out/`.

| Notebook | Trả lời câu hỏi | Nguồn dữ liệu |
|---|---|---|
| [01_model_training_evidence](01_model_training_evidence.ipynb) | Model `return-risk` hơn baseline bao nhiêu, dữ liệu nào sinh ra nó | [`mlflow-model-lineage.json`](../docs/evidence/aws/mlflow-model-lineage.json) |
| [02_drift_analysis](02_drift_analysis.ipynb) | Detector drift báo đúng không, hai lần chạy có trùng không | [`drift-summary-eks.json`](../docs/evidence/aws/drift-summary-eks.json), [`mle-drift-20261031.json`](../docs/evidence/aws/mle-drift-20261031.json) |
| [03_llm_benchmark_and_ab](03_llm_benchmark_and_ab.ipynb) | Số luồng llama.cpp, mức tải bão hòa, đánh đổi giữa model 1,5B và 0,5B | [`bench-llm-eks-t*`](../docs/evidence/aws/), [`llmd-benchmark/`](../docs/evidence/aws/llmd-benchmark/), [`ab-llm-prometheus-20261031.json`](../docs/evidence/aws/ab-llm-prometheus-20261031.json), [`agent-eval-*`](../docs/evidence/aws/) |
| [04_label_eda](04_label_eda.ipynb) | Nhãn mất cân bằng đến đâu, nhãn trễ bao lâu, vì sao phải chia theo đơn | [`data/labels_small.parquet`](data/labels_small.parquet) (mẫu 7.343 dòng của generator `small`) |
| [05_retrain_and_bootstrap](05_retrain_and_bootstrap.ipynb) | Huấn luyện lại bằng đúng `ml/train_lib.py` có tái lập không, model hơn baseline có ý nghĩa thống kê không (bootstrap, 5 seed) | [`data/test_set.parquet`](data/test_set.parquet) (đã commit) và `data/train_set.parquet` (không commit) |

Chạy lại và ghi đè output (từ gốc repo):

```bash
uv run --no-project --with jupyter --with matplotlib --with pandas --with pyarrow --with numpy \
  jupyter nbconvert --to notebook --execute --inplace notebooks/0*.ipynb
```

Notebook 05 cần tập train của run MLflow (32 MB, không commit; SHA-256 được kiểm so với tag của run):

```bash
aws s3 cp s3://platform-665116832975-lakehouse/mlflow/1/adba2b9f197e4e60a141acaf19682a61/artifacts/data/train/train_set notebooks/data/train_set.parquet
uv run --no-project --with jupyter --with matplotlib --with pandas --with pyarrow --with numpy --with lightgbm --with scikit-learn \
  jupyter nbconvert --to notebook --execute --inplace notebooks/05_retrain_and_bootstrap.ipynb
```

Giới hạn cần biết: các notebook 01–04 phân tích kết quả **đã thu trước**, không gọi lại LLM; chỉ notebook 05 huấn luyện lại model. Dữ liệu là mô phỏng,
mỗi kết quả là một lần chạy (không có khoảng tin cậy), và nhận xét trong từng notebook nêu rõ chỗ mẫu quá nhỏ để kết luận.
