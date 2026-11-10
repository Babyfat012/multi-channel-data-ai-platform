"""Generate seed-deterministic insert/update/delete and save the oracle after each commit."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from spark_jobs.lib.data_contracts.registry import Registry
from spark_jobs.lib.data_contracts.schema import validate_record
from spark_jobs.lib.order_lifecycle_cdc import payload_sha256, write_json


def mutation_plan(sample, seed):
    """Copy a sample Order under its own key; c/u payloads must pass the existing contract."""
    key = "CDC-V2-" + hashlib.sha256(str(seed).encode()).hexdigest()[:16]
    payload = json.loads(sample["raw_payload"])
    payload["purchaseOrderId"] = key
    if "customerOrderId" in payload:
        payload["customerOrderId"] = key
    payload["orderLines"]["orderLine"][0]["orderLineStatuses"] = {
        "orderLineStatus": [{"status": "Created"}]
    }
    changed = deepcopy(payload)
    changed["orderLines"]["orderLine"][0]["orderLineStatuses"]["orderLineStatus"][0]["status"] = (
        "Shipped"
    )
    schema = Registry().entries.get("sim-native-v2/walmart/1")["schema"]
    plan = []
    for order, (op, value) in enumerate((("c", payload), ("u", changed), ("d", None)), 1):
        raw = (
            None
            if value is None
            else json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        )
        if value is not None:
            errors = validate_record(
                {
                    "fetched_at": sample["fetched_at"],
                    "purchase_order_id": key,
                    "raw_payload": value,
                },
                schema,
            )
            if errors:
                raise ValueError(f"Payload does not match the contract: {errors}")
        plan.append(
            {
                "op": op,
                "dataset_version": sample["dataset_version"],
                "purchase_order_id": key,
                "raw_payload": raw,
                "after_sha256": payload_sha256(raw),
                "commit_order": order,
            }
        )
    return plan


def mutate(settings, output, seed):
    """Only modify probe keys; an incomplete oracle is rejected by the validator."""
    import urllib.request

    import psycopg

    from scripts.setup.setup_order_lifecycle_debezium import CONNECTOR_NAME, SLOT

    with urllib.request.urlopen(
        settings.get("DEBEZIUM_URL", "http://127.0.0.1:8084")
        + f"/connectors/{CONNECTOR_NAME}/status",
        timeout=10,
    ) as response:
        status = json.load(response)
    if (
        status["connector"]["state"] != "RUNNING"
        or not status["tasks"]
        or any(t["state"] != "RUNNING" for t in status["tasks"])
    ):
        raise RuntimeError("Connector/task not RUNNING yet")
    if output.exists():
        raise FileExistsError(f"Not overwriting oracle {output}; use a new output")
    with psycopg.connect(
        host=settings.get("POSTGRES_HOST", "127.0.0.1"),
        port=int(settings.get("POSTGRES_PORT", "5432")),
        dbname=settings["POSTGRES_DB"],
        user=settings["POSTGRES_USER"],
        password=settings["POSTGRES_PASSWORD"],
    ) as conn:
        if not conn.execute(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name=%s AND active", (SLOT,)
        ).fetchone():
            raise RuntimeError("v2 slot not active yet")
        row = conn.execute(
            "SELECT dataset_version,purchase_order_id,fetched_at,raw_payload "
            "FROM walmart_lifecycle.orders ORDER BY dataset_version,purchase_order_id LIMIT 1"
        ).fetchone()
        if row is None:
            raise ValueError("Load the v2 fixture with the loader first")
        sample = dict(
            zip(("dataset_version", "purchase_order_id", "fetched_at", "raw_payload"), row)
        )
        sample["fetched_at"] = row[2].isoformat()
        plan = mutation_plan(sample, seed)
        key = (plan[0]["dataset_version"], plan[0]["purchase_order_id"])
        if conn.execute(
            "SELECT 1 FROM walmart_lifecycle.orders WHERE dataset_version=%s AND purchase_order_id=%s",
            key,
        ).fetchone():
            raise ValueError("Probe key already exists")
        conn.commit()
        oracle = {"format_version": 1, "seed": seed, "complete": False, "events": []}
        write_json(output, oracle)
        for event in plan:
            if event["op"] == "c":
                cursor = conn.execute(
                    "INSERT INTO walmart_lifecycle.orders VALUES (%s,%s,%s,%s)",
                    (*key, sample["fetched_at"], event["raw_payload"]),
                )
            elif event["op"] == "u":
                cursor = conn.execute(
                    "UPDATE walmart_lifecycle.orders SET raw_payload=%s "
                    "WHERE dataset_version=%s AND purchase_order_id=%s",
                    (event["raw_payload"], *key),
                )
            else:
                cursor = conn.execute(
                    "DELETE FROM walmart_lifecycle.orders WHERE dataset_version=%s AND purchase_order_id=%s",
                    key,
                )
            if cursor.rowcount != 1:
                raise RuntimeError("Mutation must touch exactly one row")
            conn.commit()
            oracle["events"].append(
                {**event, "committed_at": datetime.now(timezone.utc).isoformat()}
            )
            write_json(output, oracle)
        oracle["complete"] = True
        write_json(output, oracle)


def main():
    """Run the mutation sequence once the connector is ready."""
    from scripts.setup.setup_order_snapshots_debezium import load_settings

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--seed", type=int, default=11)
    parser.add_argument("--output", type=Path, default=Path("order_lifecycle_cdc_expected.json"))
    args = parser.parse_args()
    mutate(load_settings(args.env_file), args.output, args.seed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
