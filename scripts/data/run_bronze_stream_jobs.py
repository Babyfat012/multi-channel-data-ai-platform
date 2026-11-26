"""Deploy the always-on Flink job that streams Kafka into lakehouse Bronze (flink_jobs/lakehouse/bronze_streams.sql).

A running job of the same name is stopped WITH a savepoint and the new one resumes from it, so a redeploy neither
re-reads nor skips a Kafka offset (Kafka offsets and Iceberg commits are both in the checkpoint). FRESH=1 skips the
savepoint, e.g. after a change to the SQL that the old state cannot be mapped to.

Runs on the machine with the Compose stack (local or EC2). COMPOSE overrides the compose command
(Jenkins: scripts/shared/ci_compose.sh with the profiles); PLATFORM_STORAGE=s3 uses the EC2 IAM role.

    python3 -m scripts.data.run_bronze_stream_jobs
"""

from __future__ import annotations

import json
import os
import re
import shlex
import time
import urllib.request
from datetime import datetime, timezone

from scripts.common import ROOT, env, load_env, run

SQL = ROOT / "flink_jobs/lakehouse/bronze_streams.sql"
JOB_NAME = "lakehouse-bronze-streams"
TOPICS = ("amazon.order-notifications", "tiktok_shop.order-webhooks")
SAVEPOINTS = "file:///opt/flink/output/savepoints"


def s3_options(environ) -> str:
    """Iceberg S3FileIO options: MinIO endpoint + path style locally, only the region on AWS (IAM role)."""
    if environ.get("PLATFORM_STORAGE", "minio") == "s3":
        pairs = {"client.region": environ["AWS_REGION"]}
    else:
        pairs = {
            "s3.endpoint": environ.get("MINIO_INTERNAL_ENDPOINT", "http://minio:9000"),
            "s3.path-style-access": "true",
            "client.region": "local-01",
        }
    return "".join(f",\n  '{key}' = '{value}'" for key, value in pairs.items())


def render(sql: str, values: dict[str, str]) -> str:
    """Fill every @@NAME@@ placeholder; a placeholder left over is a bug, not a default."""
    for name, value in values.items():
        sql = sql.replace(f"@@{name}@@", value)
    left = sorted(set(re.findall(r"@@[A-Z_]+@@", sql)))
    if left:
        raise ValueError(f"Unfilled placeholders: {left}")
    return sql


def topic_id(describe_output: str) -> str:
    """TopicId from `kafka-topics.sh --describe` (the topic incarnation: a recreated topic restarts its offsets)."""
    match = re.search(r"TopicId:\s*(\S+)", describe_output)
    if not match:
        raise ValueError("kafka-topics.sh --describe printed no TopicId")
    return match.group(1)


def kafka(compose: list[str], *args: str) -> str:
    """Run a Kafka CLI inside the kafka container."""
    return run([*compose, "exec", "-T", "kafka", *args], capture=True)


def source_epoch(compose: list[str], cdc_topic: str) -> str:
    """`<cluster id>/<topic id>`, the same CDC source identity the gate check compares against."""
    tool = "/opt/kafka/bin/"
    for topic in (*TOPICS, cdc_topic):  # the job must not start on a topic that does not exist yet
        kafka(
            compose,
            f"{tool}kafka-topics.sh",
            "--bootstrap-server",
            "localhost:9092",
            "--create",
            "--if-not-exists",
            "--topic",
            topic,
            "--partitions",
            "3",
            "--replication-factor",
            "1",
        )
    cluster = kafka(
        compose, f"{tool}kafka-cluster.sh", "cluster-id", "--bootstrap-server", "localhost:9092"
    )
    cluster_id = cluster.strip().rsplit(" ", 1)[-1]
    describe = kafka(
        compose,
        f"{tool}kafka-topics.sh",
        "--bootstrap-server",
        "localhost:9092",
        "--describe",
        "--topic",
        cdc_topic,
    )
    return f"{cluster_id}/{topic_id(describe)}"


def rest(base: str, path: str, body: dict | None = None) -> dict:
    """Call the Flink REST API (GET, or POST with a JSON body)."""
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(
        f"{base}{path}",
        data=data,
        method="POST" if data else "GET",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def stop_with_savepoint(base: str, job_name: str = JOB_NAME) -> str | None:
    """Stop the RUNNING job of this name with a savepoint; return its location (None if nothing runs)."""
    running = [
        j
        for j in rest(base, "/jobs/overview")["jobs"]
        if j["name"] == job_name and j["state"] == "RUNNING"
    ]
    if not running:
        return None
    if len(running) > 1:
        raise RuntimeError(
            f"{len(running)} jobs named {job_name} are RUNNING: fix by hand before redeploying"
        )
    job = running[0]["jid"]
    trigger = rest(base, f"/jobs/{job}/stop", {"targetDirectory": SAVEPOINTS, "drain": False})[
        "request-id"
    ]
    for _ in range(60):
        status = rest(base, f"/jobs/{job}/savepoints/{trigger}")
        if status["status"]["id"] == "COMPLETED":
            if "failure-cause" in status["operation"]:
                raise RuntimeError(f"savepoint failed: {status['operation']['failure-cause']}")
            print(f"stopped {job} with savepoint {status['operation']['location']}")
            return status["operation"]["location"]
        time.sleep(5)
    raise RuntimeError("savepoint did not complete in 5 minutes; the job keeps running")


def main() -> None:
    """Stop the old job with a savepoint, then submit the rendered SQL through sql-client in the jobmanager."""
    os.chdir(ROOT)
    load_env(required=False, override=False)
    compose = shlex.split(
        os.environ.get("COMPOSE", "docker compose --profile stream --profile flink")
    )
    cdc_topic = os.environ.get("KAFKA_TOPIC", "platform_cdc.walmart.orders")
    epoch = source_epoch(compose, cdc_topic)
    base = f"http://localhost:{os.environ.get('FLINK_UI_PORT', '8082')}"
    savepoint = None if os.environ.get("FRESH") == "1" else stop_with_savepoint(base)
    sql = render(
        SQL.read_text(),
        {
            "SAVEPOINT": f"SET 'execution.savepoint.path' = '{savepoint}';" if savepoint else "",
            "KAFKA": "kafka:9092",
            "CDC_TOPIC": cdc_topic,
            "CDC_TABLE": os.environ.get("CDC_TABLE", "raw_walmart_cdc_v2"),
            "CATALOG_URI": os.environ.get(
                "LAKEKEEPER_CATALOG_URI", "http://lakekeeper:8181/catalog"
            ),
            "WAREHOUSE": env("WAREHOUSE_NAME", "platform"),
            "S3_OPTIONS": s3_options(os.environ),
            "SOURCE_EPOCH": epoch,
            "RUN_ID": "flink__" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
        },
    )
    run(
        [
            *compose,
            "exec",
            "-T",
            "flink-jobmanager",
            "bash",
            "-c",
            'f=$(mktemp); cat > "$f"; ./bin/sql-client.sh -f "$f"; rm -f "$f"',
        ],
        stdin=sql,
    )


if __name__ == "__main__":
    main()
