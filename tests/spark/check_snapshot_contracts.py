"""Pipeline A contract check (spark_jobs/lib/snapshot_contracts.py) on a real local Spark, no Iceberg, no network.

Documents are generated from the sim-native-v1 schema files (minimal valid = required fields only), then broken
on purpose. Run with the plain Spark image:

docker run --rm --network none --hostname localhost --tmpfs /tmp:rw,exec -e HOME=/tmp -e SPARK_HOME=/opt/spark \
  -v "$PWD":/w:ro -w /w -e PYTHONPATH=/w:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
  --entrypoint python3 apache/spark:3.5.3-java17 tests/spark/check_snapshot_contracts.py
"""

from __future__ import annotations

import copy
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from spark_jobs.lib.data_contracts.registry import Registry  # noqa: E402
from spark_jobs.lib.snapshot_contracts import (  # noqa: E402
    PREFIX,
    contract_aggregates,
    failure,
    type_errors,
    v1_registry,
    with_contract,
)

VALUES = {"string": "x", "integer": 1, "number": 1.5, "boolean": True}


def minimal(schema):
    """Smallest document the schema accepts: required keys only, one item per array."""
    kind = schema.get("type")
    kind = kind[0] if isinstance(kind, list) else kind
    if kind == "object":
        return {k: minimal(schema["properties"][k]) for k in schema.get("required", [])}
    if kind == "array":
        return [minimal(schema.get("items", {}))]
    return VALUES[kind]


def without(doc, *path):
    """Copy of doc with the key at `path` removed (ints index arrays)."""
    doc = copy.deepcopy(doc)
    node = doc
    for step in path[:-1]:
        node = node[step]
    del node[path[-1]]
    return doc


def check(spark, registry, channel, payloads, column="payload"):
    """(failing rows, examples, type errors) exactly as the validate jobs compute them."""
    scoped = v1_registry(registry, channel)
    frame = spark.createDataFrame([(p,) for p in payloads], f"{column} string")
    checked = with_contract(frame, channel, column, scoped)
    row = checked.agg(F.count("*").alias("rows"), *contract_aggregates()).first()
    return (
        int(row["contract_rows"] or 0),
        list(row["contract_examples"]),
        type_errors(checked, scoped),
    )


def main() -> int:
    """Assert clean, missing-path, unknown-shape, wrong-type and malformed cases."""
    registry = Registry(ROOT / "contracts")
    docs = {
        sid[len(PREFIX) :]: minimal(e["schema"])
        for sid, e in registry.entries.items()
        if sid.startswith(PREFIX)
    }
    assert sorted(docs) == [
        "amazon/1",
        "amazon/2",
        "shopee/1",
        "shopify/1",
        "tiktok_shop/1",
        "walmart/1",
    ], docs
    spark = (
        SparkSession.builder.master("local[1]").appName("check-snapshot-contracts").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        # 1. Every minimal document passes (Walmart's payload column is raw_payload in Bronze).
        for name, doc in docs.items():
            channel = name.split("/")[0]
            column = "raw_payload" if channel == "walmart" else "payload"
            assert check(spark, registry, channel, [json.dumps(doc)] * 3, column) == (0, [], []), (
                name
            )

        # 2. Amazon: both shapes fine; a missing SKU and a row with neither shape are counted and named.
        a1, a2 = docs["amazon/1"], docs["amazon/2"]
        rows, examples, typed = check(
            spark,
            registry,
            "amazon",
            [
                json.dumps(a1),
                json.dumps(a2),
                json.dumps(without(a1, "OrderItems", 0, "SellerSKU")),
                json.dumps({"something": "else"}),
            ],
        )
        assert rows == 2, rows
        assert any(
            "contract_violation sim-native-v1/amazon/1 $.OrderItems[0].SellerSKU" in e
            for e in examples
        ), examples
        assert any(e.startswith("unregistered_schema <no shape>") for e in examples), examples

        # 3. A wrong type passes the Spark presence check but not the sampled type check.
        shopee = copy.deepcopy(docs["shopee/1"])
        shopee["response"]["order_list"][0]["item_list"][0]["model_quantity_purchased"] = "1"
        rows, _, typed = check(spark, registry, "shopee", [json.dumps(shopee)])
        assert rows == 0 and any("model_quantity_purchased: type string" in e for e in typed), typed

        # 4. Broken JSON: no required path can be read ⇒ violation; the sample reports it as not JSON.
        rows, examples, typed = check(spark, registry, "shopify", ['{"order": {"id": 1'])
        assert rows == 1 and "sim-native-v1/shopify/1 $: not JSON" in typed, (examples, typed)

        # 5. contract_reasons tags each row: passes (None), missing required paths, unregistered shape.
        from spark_jobs.lib.snapshot_contracts import contract_reasons

        frame = spark.createDataFrame(
            [
                (json.dumps(a1), PREFIX + "amazon/1"),
                (json.dumps({}), PREFIX + "amazon/1"),
                ("{}", "nope"),
            ],
            "raw_json string, contract_schema_id string",
        )
        reasons = contract_reasons(frame, v1_registry(registry, "amazon")).select("reason")
        assert sorted(str(r["reason"]) for r in reasons.collect()) == [
            "None",
            "contract_violation",
            "unregistered_schema",
        ]

        # 6. The message the task fails with; None when everything holds.
        assert failure(0, [], []) is None
        # Type errors alone (every row has its required paths) must still fail the task.
        assert "1 type errors" in failure(0, [], ["sim-native-v1/shopee/1 $.x: type string"])
        assert "2 rows break" in failure(2, ["contract_violation x $.a"], [])
    finally:
        spark.stop()
    print("snapshot contracts: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
