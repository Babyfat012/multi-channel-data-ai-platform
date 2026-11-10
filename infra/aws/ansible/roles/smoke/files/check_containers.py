"""Check Docker Compose ps JSON (an array or one object per line) without missing any service."""

import json
import sys


def main():
    """Fail if a service is missing or unhealthy, or if MinIO is running."""
    raw = sys.stdin.read().strip()
    rows = (
        json.loads(raw)
        if raw.startswith("[")
        else [json.loads(line) for line in raw.splitlines() if line.strip()]
    )
    running = {
        "postgres",
        "lakekeeper",
        "spark-master",
        "spark-worker-1",
        "spark-worker-2",
        "airflow",
    }
    by_name = {r["Service"]: r for r in rows}
    for name in running:
        assert name in by_name, f"Missing service {name}"
        row = by_name[name]
        assert row["State"] == "running", row
        assert row.get("Health", "") in ("", "healthy"), row
    for row in rows:
        if row["Service"].endswith(("-init", "-migrate")):
            assert row["State"] == "exited" and row["ExitCode"] == 0, row
        if row["Service"] in ("minio", "minio-init"):
            assert row["State"] != "running", row
    print("Compose containers: PASS")


if __name__ == "__main__":
    main()
