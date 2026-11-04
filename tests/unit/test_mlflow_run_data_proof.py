"""Kiểm tra logic so khớp SHA-256 của script bằng chứng MLflow (server giả)."""

import hashlib
import json

from scripts.evidence import mlflow_run_data_proof as proof

TRAIN, TEST = b"train-bytes", b"test-bytes"


def fake_server(tag_test):
    """Dựng hàm request giả: một run có tag hash và hai artifact."""
    sha = lambda b: hashlib.sha256(b).hexdigest()  # noqa: E731
    tags = [
        {"key": "data.train_sha256", "value": sha(TRAIN)},
        {"key": "data.test_sha256", "value": tag_test},
        {"key": "mlflow.runName", "value": "r1"},
    ]
    files = {"data/train": "data/train/train.parquet", "data/test": "data/test/test.parquet"}
    blobs = {files["data/train"]: TRAIN, files["data/test"]: TEST}

    def request(base, path, params=None, body=None):
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
    """Tag khớp artifact ⇒ mã 0 và sha256_match true cho cả hai tập."""
    monkeypatch.setattr(proof, "request", fake_server(hashlib.sha256(TEST).hexdigest()))
    assert proof.main(["--run-name", "r1"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["train"]["sha256_match"] and out["test"]["sha256_match"]
    assert "data.train_sha256" in out["tags"]


def test_hash_mismatch_fails(monkeypatch, capsys):
    """Tag test lệch artifact ⇒ mã 1."""
    monkeypatch.setattr(proof, "request", fake_server("0" * 64))
    assert proof.main(["--run-name", "r1"]) == 1
    assert json.loads(capsys.readouterr().out)["test"]["sha256_match"] is False
