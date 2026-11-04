"""Chứng minh một run MLflow lưu version dữ liệu: tag SHA-256 khớp artifact data/train và data/test.

Chạy (MLflow EC2 qua tunnel hoặc trên EC2):
  python3 scripts/evidence/mlflow_run_data_proof.py --run-name mysterious-flea-253 \
    > docs/evidence/aws/mlflow-run-data-proof-<ngày>.json
  # Truy ngược từ model đang phục vụ: model version → run đã train ra nó → dữ liệu (hash) và tham số/metric
  python3 scripts/evidence/mlflow_run_data_proof.py --model return-risk --version 1
Chỉ dùng thư viện chuẩn: gọi REST của MLflow server (server proxy artifact). Thoát 1 nếu thiếu hoặc lệch.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import urllib.parse
import urllib.request


def request(base: str, path: str, params: dict | None = None, body: dict | None = None) -> bytes:
    """GET (hoặc POST JSON nếu có body) tới MLflow server, trả bytes."""
    url = f"{base}{path}" + (f"?{urllib.parse.urlencode(params)}" if params else "")
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data, {"Content-Type": "application/json"} if data else {})
    with urllib.request.urlopen(req, timeout=60) as response:
        return response.read()


def run_of_model(base: str, name: str, version: str) -> tuple[dict, dict]:
    """Từ model registry (name, version) đi tới run đã train ra nó; trả (model_version, run)."""
    got = json.loads(
        request(base, "/api/2.0/mlflow/model-versions/get", {"name": name, "version": version})
    )["model_version"]
    run = json.loads(request(base, "/api/2.0/mlflow/runs/get", {"run_id": got["run_id"]}))["run"]
    return got, run


def find_run(base: str, experiment: str, run_name: str) -> dict:
    """Tìm run theo tên trong experiment; lỗi rõ nếu không có."""
    got = json.loads(
        request(base, "/api/2.0/mlflow/experiments/get-by-name", {"experiment_name": experiment})
    )
    runs = json.loads(
        request(
            base,
            "/api/2.0/mlflow/runs/search",
            body={
                "experiment_ids": [got["experiment"]["experiment_id"]],
                "filter": f"tags.mlflow.runName = '{run_name}'",
                "max_results": 1,
            },
        )
    ).get("runs")
    if not runs:
        raise SystemExit(f"Không thấy run {run_name!r} trong experiment {experiment!r}")
    return runs[0]


def check_split(base: str, run_id: str, tags: dict, split: str) -> dict:
    """Liệt kê artifact data/<split>, tính SHA-256 từng file và so với tag data.<split>_sha256."""
    want = tags.get(f"data.{split}_sha256")
    files = json.loads(
        request(base, "/api/2.0/mlflow/artifacts/list", {"run_id": run_id, "path": f"data/{split}"})
    ).get("files", [])
    found = [
        {
            "path": f["path"],
            "bytes": f.get("file_size"),
            "sha256": hashlib.sha256(
                request(base, "/get-artifact", {"run_uuid": run_id, "path": f["path"]})
            ).hexdigest(),
        }
        for f in files
        if not f.get("is_dir")
    ]
    return {
        "tag": want,
        "artifacts": found,
        "sha256_match": bool(want) and bool(found) and all(a["sha256"] == want for a in found),
    }


def main(argv=None) -> int:
    """In JSON bằng chứng; mã 1 nếu tag/artifact thiếu hoặc hash lệch."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--run-name", help="tên run MLflow (hoặc dùng --model/--version)")
    parser.add_argument("--model", help="tên model trong registry, ví dụ return-risk")
    parser.add_argument("--version", help="phiên bản model, đi cùng --model")
    parser.add_argument("--experiment", default="return-risk")
    parser.add_argument("--url", default="http://127.0.0.1:5050")
    args = parser.parse_args(argv)
    lineage = {}
    if args.model:
        if not args.version:
            parser.error("--model cần --version")
        version, run = run_of_model(args.url, args.model, args.version)
        lineage = {
            "model": {
                "name": args.model,
                "version": args.version,
                "source": version.get("source"),
                "run_id": version["run_id"],
            }
        }
    elif args.run_name:
        run = find_run(args.url, args.experiment, args.run_name)
    else:
        parser.error("cần --run-name hoặc --model --version")
    run_id = run["info"]["run_id"]
    tags = {t["key"]: t["value"] for t in run["data"].get("tags", [])}
    result = {
        **lineage,
        "run_name": tags.get("mlflow.runName"),
        "run_id": run_id,
        "started": run["info"].get("start_time"),
        "params": {p["key"]: p["value"] for p in run["data"].get("params", [])},
        "metrics": {m["key"]: m["value"] for m in run["data"].get("metrics", [])},
        "tags": {k: v for k, v in sorted(tags.items()) if k.startswith("data.")},
        "train": check_split(args.url, run_id, tags, "train"),
        "test": check_split(args.url, run_id, tags, "test"),
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["train"]["sha256_match"] and result["test"]["sha256_match"] else 1


if __name__ == "__main__":
    sys.exit(main())
