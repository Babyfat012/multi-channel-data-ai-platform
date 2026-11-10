"""Check the SHA-256 matching logic of the MLflow evidence script (fake server)."""

import hashlib
import json

from scripts.evidence import mlflow_run_data_proof as proof

TRAIN, TEST = b"train-bytes", b"test-bytes"


def fake_server(tag_test):
    """Build a fake request function: one run with hash tags and two artifacts."""
    sha = lambda b: hashlib.sha256(b).hexdigest()  # noqa: E731
    tags = [
        {"key": "data.train_sha256", "value": sha(TRAIN)},
        {"key": "data.test_sha256", "value": tag_test},
        {"key": "mlflow.runName", "value": "r1"},
    ]
    files = {"data/train": "data/train/train.parquet", "data/test": "data/test/test.parquet"}
    blobs = {files["data/train"]: TRAIN, files["data/test"]: TEST}

    def request(base, path, params=None, body=None):
        if path.endswith("model-versions/get"):
            return json.dumps(
                {"model_version": {"run_id": "abc", "source": "models:/m-1"}}
            ).encode()
        if path.endswith("runs/get"):
            run = {
                "info": {"run_id": "abc", "start_time": 1},
                "data": {
                    "tags": tags,
                    "params": [{"key": "seed", "value": "7"}],
                    "metrics": [{"key": "pr_auc", "value": 0.3}],
                },
            }
            return json.dumps({"run": run}).encode()
        if path.endswith("get-by-name"):
            return json.dumps({"experiment": {"experiment_id": "1"}}).encode()
        if path.endswith("runs/search"):
            run = {"info": {"run_id": "abc"}, "data": {"tags": tags}}
            return json.dumps({"runs": [run]}).encode()
        if path.endswith("artifacts/list"):
            return json.dumps({"files": [{"path": files[params["path"]], "file_size": 1}]}).encode()
        return blobs[params["path"]]

    return request


def test_hash_tags_match_artifacts(monkeypatch, capsys):
    """Tags match the artifacts ⇒ exit code 0 and sha256_match true for both sets."""
    monkeypatch.setattr(proof, "request", fake_server(hashlib.sha256(TEST).hexdigest()))
    assert proof.main(["--run-name", "r1"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["train"]["sha256_match"] and out["test"]["sha256_match"]
    assert "data.train_sha256" in out["tags"]


def test_hash_mismatch_fails(monkeypatch, capsys):
    """Test tag differs from the artifact ⇒ exit code 1."""
    monkeypatch.setattr(proof, "request", fake_server("0" * 64))
    assert proof.main(["--run-name", "r1"]) == 1
    assert json.loads(capsys.readouterr().out)["test"]["sha256_match"] is False


def test_model_version_traces_back_to_run_and_data(monkeypatch, capsys):
    """--model/--version goes from the registry to the training run that produced it: params, metrics, matching data hashes."""
    monkeypatch.setattr(proof, "request", fake_server(hashlib.sha256(TEST).hexdigest()))
    assert proof.main(["--model", "return-risk", "--version", "1"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["model"] == {
        "name": "return-risk",
        "version": "1",
        "source": "models:/m-1",
        "run_id": "abc",
    }
    assert out["params"] == {"seed": "7"} and out["metrics"] == {"pr_auc": 0.3}
    assert out["run_name"] == "r1" and out["train"]["sha256_match"]
