# Analysis notebooks

Five notebooks, already executed (with outputs). The first four only read files committed to the repo; notebook 05 also needs the 32 MB train set (see below). No notebook needs a connection to the AWS cluster or the `out/` directory.

| Notebook | Question it answers | Data source |
|---|---|---|
| [01_model_training_evidence](01_model_training_evidence.ipynb) | How much the `return-risk` model beats the baseline, and which data produced it | [`mlflow-model-lineage.json`](../docs/evidence/aws/mlflow-model-lineage.json) |
| [02_drift_analysis](02_drift_analysis.ipynb) | Whether the drift detector reports correctly, and whether the two runs agree | [`drift-summary-eks.json`](../docs/evidence/aws/drift-summary-eks.json), [`mle-drift-20261031.json`](../docs/evidence/aws/mle-drift-20261031.json) |
| [03_llm_benchmark_and_ab](03_llm_benchmark_and_ab.ipynb) | llama.cpp thread count, the saturation load level, and the trade-off between the 1.5B and 0.5B models | [`bench-llm-eks-t*`](../docs/evidence/aws/), [`llmd-benchmark/`](../docs/evidence/aws/llmd-benchmark/), [`ab-llm-prometheus-20261031.json`](../docs/evidence/aws/ab-llm-prometheus-20261031.json), [`agent-eval-*`](../docs/evidence/aws/) |
| [04_label_eda](04_label_eda.ipynb) | How imbalanced the label is, how long the label delay is, and why the split must be by order | [`data/labels_small.parquet`](data/labels_small.parquet) (7,343-row sample from the `small` generator run) |
| [05_retrain_and_bootstrap](05_retrain_and_bootstrap.ipynb) | Whether retraining with exactly `ml/train_lib.py` reproduces the result, and whether the model's lead over the baseline is statistically significant (bootstrap, 5 seeds) | [`data/test_set.parquet`](data/test_set.parquet) (committed) and `data/train_set.parquet` (not committed) |

Re-run and overwrite the outputs (from the repo root):

```bash
uv run --no-project --with jupyter --with matplotlib --with pandas --with pyarrow --with numpy \
  jupyter nbconvert --to notebook --execute --inplace notebooks/0*.ipynb
```

Notebook 05 needs the MLflow run's train set (32 MB, not committed; its SHA-256 is checked against the run's tag):

```bash
aws s3 cp s3://platform-665116832975-lakehouse/mlflow/1/adba2b9f197e4e60a141acaf19682a61/artifacts/data/train/train_set notebooks/data/train_set.parquet
uv run --no-project --with jupyter --with matplotlib --with pandas --with pyarrow --with numpy --with lightgbm --with scikit-learn \
  jupyter nbconvert --to notebook --execute --inplace notebooks/05_retrain_and_bootstrap.ipynb
```

Limits to keep in mind: notebooks 01–04 analyze results **collected beforehand** and do not call the LLM again; only notebook 05 retrains the model. The data is simulated,
each result is a single run (no confidence interval), and the commentary in each notebook points out where the sample is too small to draw conclusions.
