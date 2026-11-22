"""Submit the TWO PyFlink stream feature jobs to the Flink ML cluster (2.2): ml-sku-return-24h-online and -offline.

Same file flink_jobs/ml/sku_return_feature.py with a different --sink. A RUNNING job with the same name is cancelled
first, so redeploys never duplicate jobs. COMPOSE overrides the compose command (EC2 presets the AWS overlay).

    python3 -m scripts.ml.run_stream_feature_jobs
"""

import os
import shlex

from scripts.common import ROOT, run
from scripts.ml.run_sku_return_feature import cancel_running

TOPIC = "order_snapshots.return_events"


def main() -> None:
    """Create the topic with infinite retention, then (re)submit both sinks."""
    os.chdir(ROOT)
    compose = shlex.split(
        os.environ.get("COMPOSE") or "docker compose --profile stream --profile flink-ml"
    )
    kafka = [*compose, "exec", "-T", "kafka"]
    bootstrap = ["--bootstrap-server", "localhost:9092"]
    # Records carry (past) event time: without retention.ms=-1 Kafka deletes them immediately by timestamp.
    run(
        [
            *kafka,
            "/opt/kafka/bin/kafka-topics.sh",
            *bootstrap,
            "--create",
            "--if-not-exists",
            "--topic",
            TOPIC,
            "--partitions",
            "3",
            "--replication-factor",
            "1",
            "--config",
            "retention.ms=-1",
        ]
    )
    run(
        [
            *kafka,
            "/opt/kafka/bin/kafka-configs.sh",
            *bootstrap,
            "--alter",
            "--entity-type",
            "topics",
            "--entity-name",
            TOPIC,
            "--add-config",
            "retention.ms=-1",
        ]
    )
    rest = f"http://localhost:{os.environ.get('FLINK_ML_UI_PORT', '8086')}"
    for sink in ("online", "offline"):
        cancel_running(rest, f"ml-sku-return-24h-{sink}")
        run(
            [
                *compose,
                "exec",
                "-T",
                "flink-ml-jobmanager",
                "./bin/flink",
                "run",
                "-d",
                "-py",
                "/opt/flink/jobs/ml/sku_return_feature.py",
                "--sink",
                sink,
            ]
        )


if __name__ == "__main__":
    main()
