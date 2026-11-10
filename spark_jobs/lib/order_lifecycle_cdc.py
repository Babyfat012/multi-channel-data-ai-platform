"""Pure functions reconciling CDC v2 against the oracle, plus shared Spark/receipt helpers."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections import Counter, defaultdict
from pathlib import Path

TOPIC = "platform_cdc_order_lifecycle.walmart_lifecycle.orders"
TARGET = "lakehouse.bronze.lifecycle_raw_walmart_cdc_events"


def payload_sha256(payload):
    """Hash the exact UTF-8 bytes of raw_payload TEXT; a delete has a null after, so it hashes null."""
    return None if payload is None else hashlib.sha256(payload.encode("utf-8")).hexdigest()


def write_json(path, document):
    """Write JSON atomically on the POSIX control directory shared by the two tasks."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=".cdc-v2-")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(document, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def compare_events(expected, actual, epoch, boundary=None):
    """Count missing/extra, hash mismatches, same-key reordering and mixed epochs, without hiding bad rows."""
    checks = dict.fromkeys(
        (
            "missing",
            "extra",
            "payload_mismatch",
            "order_mismatch",
            "epoch_mismatch",
            "invalid",
            "duplicate_offsets",
            "outside_boundary",
        ),
        0,
    )
    want, got = defaultdict(list), defaultdict(list)
    sequences_want, sequences_got = defaultdict(list), defaultdict(list)
    offsets = set()
    for event in sorted(expected, key=lambda e: e["commit_order"]):
        key = (event["dataset_version"], event["purchase_order_id"])
        want[(*key, event["op"])].append(event)
        sequences_want[key].append((event["op"], event["after_sha256"]))
    for event in actual:
        key = (event.get("dataset_version"), event.get("purchase_order_id"))
        got[(*key, event.get("op"))].append(event)
        sequences_got[key].append(event)
        checks["epoch_mismatch"] += event.get("source_epoch") != epoch
        invalid = (
            event.get("op") not in ("c", "u", "d")
            or not all(key)
            or event.get("source_lsn") is None
            or event.get("source_ts") is None
            or type(event.get("kafka_partition")) is not int
            or type(event.get("kafka_offset")) is not int
        )
        if event.get("op") in ("c", "u"):
            invalid |= event.get("payload_after") is None
        elif event.get("op") == "d":
            invalid |= event.get("payload_after") is not None
        checks["invalid"] += bool(invalid)
        offset = (
            event.get("source_epoch"),
            event.get("topic"),
            event.get("kafka_partition"),
            event.get("kafka_offset"),
        )
        checks["duplicate_offsets"] += offset in offsets
        offsets.add(offset)
        if boundary:
            p, o = str(event.get("kafka_partition")), event.get("kafka_offset")
            inside = (
                event.get("topic") == boundary["topic"]
                and p in boundary["starting_offsets"]
                and type(o) is int
                and boundary["starting_offsets"][p] <= o < boundary["ending_offsets"][p]
            )
            checks["outside_boundary"] += not inside
    for key in want.keys() | got.keys():
        ws, gs = want[key], got[key]
        checks["missing"] += max(0, len(ws) - len(gs))
        checks["extra"] += max(0, len(gs) - len(ws))
        gs = sorted(gs, key=lambda e: (e.get("kafka_partition") or 0, e.get("kafka_offset") or 0))
        for w, g in zip(ws, gs):
            digest = payload_sha256(g.get("payload_after"))
            checks["payload_mismatch"] += (
                digest != w["after_sha256"] or g.get("record_sha256") != digest
            )
    for key, sequence in sequences_want.items():
        rows = sequences_got[key]
        # Compare fingerprints to catch two swapped same-key UPDATEs, without treating payload errors as ordering.
        observed = [
            (e.get("op"), payload_sha256(e.get("payload_after")))
            for e in sorted(rows, key=lambda e: e.get("kafka_offset") or 0)
        ]
        if Counter(observed) == Counter(sequence):
            partitions = {e.get("kafka_partition") for e in rows}
            checks["order_mismatch"] += len(partitions) != 1 or observed != sequence
    return checks


def build_session(name, local_warehouse=None):
    """Use the real catalog by default, or a separate local Hadoop one for the harness without Lakekeeper."""
    if local_warehouse is None:
        from spark_jobs.lib.iceberg_session import build_spark

        return build_spark(name, shuffle_partitions=2)
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName(name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", local_warehouse)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
