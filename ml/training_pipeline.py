"""Kubeflow Pipelines: load (Feast offline) → split → train → evaluate → register (MLflow).

Every step runs in the `platform-train` image (docker/Dockerfile.train), calling functions from `ml/train_lib.py`.
Compile + submit: `scripts/run/run_training_pipeline.sh`.
"""

import os

from kfp import dsl, kubernetes

IMAGE = f"{os.environ.get('REGISTRY', 'localhost:5001')}/platform-train:{os.environ.get('TAG', '0.1.0')}"
# Target chosen at compile time. aws: real S3 via the kubeflow/pipeline-runner Pod Identity (no keys),
# MLflow on EC2 via private IP. kind: MinIO, credentials from a Secret created at submit time (not in the YAML).
TARGET = os.environ.get("PIPELINE_TARGET", "kind")
SECRET = "platform-minio" if TARGET == "kind" else None
if TARGET == "aws":
    FEAST_ROOT = f"s3://{os.environ['LAKEHOUSE_BUCKET']}/feature-store"
    TRACKING_URI = os.environ["MLFLOW_TRACKING_URI"]
    S3_ENV = {"AWS_REGION": os.environ.get("AWS_REGION", "ap-southeast-2")}
else:
    FEAST_ROOT = "s3://platform/feature-store"
    TRACKING_URI = "http://mlflow:5000"
    S3_ENV = {
        "FEAST_S3_ENDPOINT_URL": "http://minio:9000",
        "AWS_ENDPOINT_URL_S3": "http://minio:9000",
    }
FEAST_ENV = {
    "FEAST_REGISTRY": f"{FEAST_ROOT}/registry.db",
    "FEAST_SKU_DAILY_PATH": f"{FEAST_ROOT}/sku_daily",
    "FEAST_SKU_RETURN_24H_PATH": f"{FEAST_ROOT}/sku_return_24h/",
    "FEAST_REDIS": "redis.feature-store.svc.cluster.local:6379",
    **S3_ENV,
}


@dsl.component(base_image=IMAGE)
def load_features(entity_path: str, features: dsl.Output[dsl.Dataset]):
    """Read the labeled lines, then PIT-join SKU features from Feast's OFFLINE store."""
    import pandas as pd
    import s3fs
    from feast import FeatureStore

    from ml.train_lib import FEAST_FEATURES

    fs = s3fs.S3FileSystem()
    entity = pd.read_parquet(entity_path.removeprefix("s3://"), filesystem=fs)
    store = FeatureStore("/opt/feature_repo")
    # Feast's file offline store joins each line with EVERY daily snapshot of the SKU before PIT filtering:
    # 360k lines × ~180 days ≈ 65M intermediate rows ⇒ OOM. Call in batches, same result as a single call.
    # Limitation: fixed 20k batches; for larger data use the Spark offline store.
    chunks = [entity.iloc[i : i + 20_000] for i in range(0, len(entity), 20_000)]
    frame = pd.concat(
        store.get_historical_features(entity_df=chunk, features=FEAST_FEATURES).to_df()
        for chunk in chunks
    )
    frame.to_parquet(features.path)
    print(f"{len(frame)} rows, columns {list(frame.columns)}")


@dsl.component(base_image=IMAGE)
def split(
    features: dsl.Input[dsl.Dataset],
    test_days: int,
    train_set: dsl.Output[dsl.Dataset],
    test_set: dsl.Output[dsl.Dataset],
):
    """Split by order date: the last `test_days` days become test."""
    import pandas as pd

    from ml.train_lib import temporal_split

    train, test = temporal_split(pd.read_parquet(features.path), test_days)
    train.to_parquet(train_set.path)
    test.to_parquet(test_set.path)
    print(
        f"train {len(train)} rows, test {len(test)} rows, test prevalence {test['label'].mean():.3f}"
    )


@dsl.component(base_image=IMAGE)
def train(train_set: dsl.Input[dsl.Dataset], model: dsl.Output[dsl.Model]):
    """Train LightGBM with a fixed seed, save the model as booster text."""
    import pandas as pd

    from ml.train_lib import add_order_features, train_model

    fitted = train_model(add_order_features(pd.read_parquet(train_set.path)))
    fitted.booster_.save_model(model.path)


@dsl.component(base_image=IMAGE)
def evaluate(
    train_set: dsl.Input[dsl.Dataset],
    test_set: dsl.Input[dsl.Dataset],
    model: dsl.Input[dsl.Model],
    metrics: dsl.Output[dsl.Metrics],
) -> str:
    """PR-AUC of model vs baseline on the same test; return metrics as JSON for the register step."""
    import json

    import lightgbm as lgb
    import pandas as pd

    from ml.train_lib import FEATURES, add_order_features, baseline_scores
    from ml.train_lib import evaluate as score

    train, test = (
        pd.read_parquet(train_set.path),
        add_order_features(pd.read_parquet(test_set.path)),
    )
    booster = lgb.Booster(model_file=model.path)
    result = score(test, booster.predict(test[FEATURES]), baseline_scores(train, test))
    for key, value in result.items():
        metrics.log_metric(key, float(value))
    print(result)
    return json.dumps(result)


@dsl.component(base_image=IMAGE)
def register(
    model: dsl.Input[dsl.Model],
    train_set: dsl.Input[dsl.Dataset],
    test_set: dsl.Input[dsl.Dataset],
    result: str,
    entity_path: str,
    tracking_uri: str,
    model_name: str,
):
    """Log the MLflow run (params, metrics, model, data), register the version; always tag `latest`,
    alias + tag `production` only when it beats the baseline."""
    import hashlib
    import json

    import lightgbm as lgb
    import mlflow
    import pandas as pd
    from mlflow import MlflowClient

    from ml.train_lib import FEATURES

    def sha256(path):
        """Compute the file's SHA-256 hash to record the artifact version."""
        with open(path, "rb") as f:
            return hashlib.sha256(f.read()).hexdigest()

    metrics = json.loads(result)
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment("return-risk")
    train = pd.read_parquet(train_set.path)
    with mlflow.start_run() as run:
        mlflow.log_params({"features": ",".join(FEATURES), "algo": "lightgbm", "seed": 7})
        mlflow.log_metrics({k: float(v) for k, v in metrics.items()})
        mlflow.set_tags(
            {
                "data.entity_path": entity_path,
                "data.dataset_version": ",".join(sorted(train["dataset_version"].unique())),
                "data.generation_id": ",".join(sorted(train["generation_id"].unique())),
                "data.train_sha256": sha256(train_set.path),
                "data.test_sha256": sha256(test_set.path),
            }
        )
        mlflow.log_input(
            mlflow.data.from_pandas(train, source=entity_path, name="train"), context="training"
        )
        mlflow.log_artifact(train_set.path, "data/train")
        mlflow.log_artifact(test_set.path, "data/test")
        info = mlflow.lightgbm.log_model(
            lgb.Booster(model_file=model.path), name="model", registered_model_name=model_name
        )
    client = MlflowClient()
    version = info.registered_model_version
    # The "latest" alias is reserved by MLflow (models:/<name>/latest already points to the newest version) ⇒ use a tag.
    for older in client.search_model_versions(f"name='{model_name}'"):
        if older.version != version and older.tags.get("latest") == "true":
            client.set_model_version_tag(model_name, older.version, "latest", "false")
    client.set_model_version_tag(model_name, version, "latest", "true")
    client.set_model_version_tag(
        model_name, version, "production", str(metrics["beats_baseline"]).lower()
    )
    if metrics["beats_baseline"]:
        client.set_registered_model_alias(model_name, "production", version)
    print(
        f"run {run.info.run_id} → {model_name} v{version}, production={metrics['beats_baseline']}"
    )


@dsl.pipeline(name="return-risk-training", description="Feast offline → LightGBM → MLflow")
def return_risk_training(
    entity_path: str = f"{FEAST_ROOT}/training_lines",
    test_days: int = 10,
    tracking_uri: str = TRACKING_URI,
    model_name: str = "return-risk",
):
    """Five steps; each step takes exactly the artifacts of the previous one."""
    loaded = load_features(entity_path=entity_path)
    parts = split(features=loaded.outputs["features"], test_days=test_days)
    trained = train(train_set=parts.outputs["train_set"])
    scored = evaluate(
        train_set=parts.outputs["train_set"],
        test_set=parts.outputs["test_set"],
        model=trained.outputs["model"],
    )
    reg = register(
        model=trained.outputs["model"],
        train_set=parts.outputs["train_set"],
        test_set=parts.outputs["test_set"],
        result=scored.outputs["Output"],
        entity_path=entity_path,
        tracking_uri=tracking_uri,
        model_name=model_name,
    )
    # Declare RAM so a shortage fails clearly at the pod instead of starving the whole node: load holds entity + PIT features in pandas.
    loaded.set_memory_request("2G").set_memory_limit("4G")
    for task in (parts, trained, scored, reg):
        task.set_memory_request("1G").set_memory_limit("3G")
    for task in (loaded, parts, trained, scored, reg):
        task.set_caching_options(False)
        kubernetes.set_image_pull_policy(task, "IfNotPresent")
        for name, value in FEAST_ENV.items():
            task.set_env_variable(name, value)
        if SECRET:
            kubernetes.use_secret_as_env(
                task,
                secret_name=SECRET,
                secret_key_to_env={
                    "AWS_ACCESS_KEY_ID": "AWS_ACCESS_KEY_ID",
                    "AWS_SECRET_ACCESS_KEY": "AWS_SECRET_ACCESS_KEY",
                },
            )
