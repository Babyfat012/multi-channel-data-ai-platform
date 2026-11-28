"""Kubeflow Pipelines for return-risk, both scheduled as KFP recurring runs (`scripts/ml/run_training_pipeline.py`):

- `return-risk-training` (weekly): load (Feast offline) → split → train → evaluate → register challenger (MLflow).
- `return-risk-monitor` (daily): load → check_drift vs the production model's train set → the same training
  steps only if drift and no challenger awaits approval (`ml/drift.py` decide).

Neither touches the `production` alias: a human approves the challenger in the return-risk Jenkins pipeline
(`scripts/ml/promote_return_risk.py`), which then deploys it.

Steps run in `platform-train` (infra/docker/Dockerfile.train, logic in `ml/train_lib.py`), except check_drift in
`platform-drift` (infra/docker/Dockerfile.drift): Evidently 0.7 needs pandas 2 and is kept out of the Feast image.
"""

import os

from kfp import dsl, kubernetes

IMAGE = f"{os.environ.get('REGISTRY', 'localhost:5001')}/platform-train:{os.environ.get('TAG', '0.1.0')}"
DRIFT_IMAGE = f"{os.environ.get('REGISTRY', 'localhost:5001')}/platform-drift:{os.environ.get('TAG', '0.1.0')}"
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
    store = FeatureStore("/opt/feature_store")
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
    tracking_uri: str,
    model_name: str,
    metrics: dsl.Output[dsl.Metrics],
) -> str:
    """PR-AUC of model vs baseline vs the current production model (champion), all on THIS test set;
    return metrics as JSON for the register step."""
    import json

    import lightgbm as lgb
    import mlflow
    import pandas as pd
    from mlflow import MlflowClient

    from ml.train_lib import FEATURES, add_order_features, baseline_scores, champion_scores
    from ml.train_lib import evaluate as score

    train, test = (
        pd.read_parquet(train_set.path),
        add_order_features(pd.read_parquet(test_set.path)),
    )
    booster = lgb.Booster(model_file=model.path)
    # Champion = the version holding alias `production` (none on the very first run). Load it by VERSION, not by
    # alias, so a promotion between the lookup and the load cannot mix two versions.
    mlflow.set_tracking_uri(tracking_uri)
    found = MlflowClient().search_registered_models(filter_string=f"name='{model_name}'")
    champion_version = found[0].aliases.get("production") if found else None
    champion = None
    if champion_version is not None:
        champion_booster = mlflow.lightgbm.load_model(f"models:/{model_name}/{champion_version}")
        champion = champion_scores(champion_booster, test)
    result = score(test, booster.predict(test[FEATURES]), baseline_scores(train, test), champion)
    if champion is not None:
        result["champion_version"] = int(champion_version)
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
    alias `challenger` only when it beats the baseline. `production` is set only by a human approval."""
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
    for gate in ("beats_baseline", "beats_champion"):
        if gate in metrics:
            client.set_model_version_tag(model_name, version, gate, str(metrics[gate]).lower())
    # Below the baseline ⇒ not even a candidate; an older challenger (if any) keeps the alias.
    if metrics["beats_baseline"]:
        client.set_registered_model_alias(model_name, "challenger", version)
    print(
        f"run {run.info.run_id} → {model_name} v{version}, challenger={metrics['beats_baseline']}"
    )


@dsl.component(base_image=DRIFT_IMAGE)
def check_drift(
    features: dsl.Input[dsl.Dataset],
    tracking_uri: str,
    model_name: str,
    min_rows: int,
    report: dsl.Output[dsl.HTML],
    metrics: dsl.Output[dsl.Metrics],
) -> str:
    """Compare lines the production model never saw with its train set; return "train" or "skip"."""
    import json
    from pathlib import Path

    import mlflow
    import pandas as pd
    from mlflow import MlflowClient

    from ml.drift import check, decide, unseen
    from ml.train_lib import CATEGORICAL, NUMERIC, add_order_features

    def logged(run_id, part):
        """The train/test set the register step logged with the run (MLflow proxies the S3 read)."""
        folder = Path(
            mlflow.artifacts.download_artifacts(run_id=run_id, artifact_path=f"data/{part}")
        )
        return pd.concat(pd.read_parquet(f) for f in sorted(folder.iterdir()))

    mlflow.set_tracking_uri(tracking_uri)
    client = MlflowClient()
    found = client.search_registered_models(filter_string=f"name='{model_name}'")
    aliases = {k: str(v) for k, v in found[0].aliases.items()} if found else {}
    summary, rows = {}, 0
    if "production" not in aliases:
        decision, reason = (
            "skip",
            "no production model to compare with (the weekly training covers it)",
        )
    else:
        run_id = client.get_model_version(model_name, aliases["production"]).run_id
        reference, tested = logged(run_id, "train"), logged(run_id, "test")
        current = unseen(pd.read_parquet(features.path), reference, tested)
        rows = len(current)
        if rows >= min_rows:
            summary = check(
                add_order_features(reference),
                add_order_features(current),
                Path(report.path),
                categorical=[*CATEGORICAL, "label"],
                numeric=NUMERIC,
            )
        decision, reason = decide(
            summary.get("alert", False), rows, min_rows, aliases.get("challenger")
        )
    if not Path(report.path).exists():
        Path(report.path).write_text(f"<p>No drift report: {reason}</p>")
    metrics.log_metric("current_rows", float(rows))
    metrics.log_metric("drifted_columns", float(len(summary.get("drifted", []))))
    # Empty columns are a data break, not drift: visible here, never a reason to train.
    metrics.log_metric("empty_columns", float(len(summary.get("empty", []))))
    metrics.log_metric("train", float(decision == "train"))
    print(
        json.dumps(
            {
                "decision": decision,
                "reason": reason,
                "production": aliases.get("production"),
                **summary,
            }
        )
    )
    return decision


def train_and_register(features, entity_path, test_days, tracking_uri, model_name) -> list:
    """split → train → evaluate → register on already-loaded features; shared by both pipelines."""
    parts = split(features=features, test_days=test_days)
    trained = train(train_set=parts.outputs["train_set"])
    scored = evaluate(
        train_set=parts.outputs["train_set"],
        test_set=parts.outputs["test_set"],
        model=trained.outputs["model"],
        tracking_uri=tracking_uri,
        model_name=model_name,
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
    tasks = [parts, trained, scored, reg]
    for task in tasks:
        task.set_memory_request("1G").set_memory_limit("3G")
    return tasks


def configure(tasks) -> None:
    """Same runtime for every step: no cache, local image first, Feast/S3 env, MinIO keys on kind."""
    for task in tasks:
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


@dsl.pipeline(
    name="return-risk-training", description="Feast offline → LightGBM → MLflow challenger"
)
def return_risk_training(
    entity_path: str = f"{FEAST_ROOT}/training_lines",
    test_days: int = 10,
    tracking_uri: str = TRACKING_URI,
    model_name: str = "return-risk",
):
    """Five steps; each step takes exactly the artifacts of the previous one."""
    loaded = load_features(entity_path=entity_path)
    # Declare RAM so a shortage fails clearly at the pod instead of starving the whole node: load holds entity + PIT features in pandas.
    loaded.set_memory_request("2G").set_memory_limit("4G")
    steps = train_and_register(
        loaded.outputs["features"], entity_path, test_days, tracking_uri, model_name
    )
    configure([loaded, *steps])


@dsl.pipeline(
    name="return-risk-monitor",
    description="Feast offline → drift vs the production model's train set → train if drift",
)
def return_risk_monitor(
    entity_path: str = f"{FEAST_ROOT}/training_lines",
    test_days: int = 10,
    tracking_uri: str = TRACKING_URI,
    model_name: str = "return-risk",
    min_rows: int = 1000,
):
    """Load once; the drift check and (if it says so) the training both use these features."""
    loaded = load_features(entity_path=entity_path)
    loaded.set_memory_request("2G").set_memory_limit("4G")
    drift = check_drift(
        features=loaded.outputs["features"],
        tracking_uri=tracking_uri,
        model_name=model_name,
        min_rows=min_rows,
    )
    drift.set_memory_request("1G").set_memory_limit("3G")
    with dsl.If(drift.outputs["Output"] == "train", name="drift-detected"):
        steps = train_and_register(
            loaded.outputs["features"], entity_path, test_days, tracking_uri, model_name
        )
    configure([loaded, drift, *steps])
