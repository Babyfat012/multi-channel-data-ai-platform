"""Pipeline A contracts (contracts/schemas/sim-native-v1): what they require, and that they actually bite.

Standard library only; the Spark side runs in tests/spark/check_snapshot_contracts.py.
"""

import copy
import re
from pathlib import Path

import pytest

from spark_jobs.lib.data_contracts.registry import SUPPORTED_PARSERS, Registry
from spark_jobs.lib.data_contracts.schema import required_json_paths, validate_record

ROOT = Path(__file__).resolve().parents[4]
REGISTRY = Registry(ROOT / "contracts")
V1 = {sid: e for sid, e in REGISTRY.entries.items() if sid.startswith("sim-native-v1/")}
VALUES = {"string": "x", "integer": 1, "number": 1.5, "boolean": True}


def minimal(schema):
    """Smallest accepted document: required keys only, one item per array."""
    kind = schema.get("type")
    kind = kind[0] if isinstance(kind, list) else kind
    if kind == "object":
        return {k: minimal(schema["properties"][k]) for k in schema.get("required", [])}
    if kind == "array":
        return [minimal(schema.get("items", {}))]
    return VALUES[kind]


def drop(doc, path):
    """Copy of doc without the key at a required_json_paths path like $.a[0].b."""
    doc = copy.deepcopy(doc)
    steps = [int(s) if s.isdigit() else s for s in re.findall(r"\w+", path.removeprefix("$"))]
    node = doc
    for step in steps[:-1]:
        node = node[step]
    del node[steps[-1]]
    return doc


def test_six_shapes_registered_with_their_own_parsers():
    """One contract per Silver parser; v1 parser names do not collide with v2's."""
    assert sorted(V1) == [
        f"sim-native-v1/{n}"
        for n in ("amazon/1", "amazon/2", "shopee/1", "shopify/1", "tiktok_shop/1", "walmart/1")
    ]
    assert all(
        e["parser"].startswith("snapshots/") and e["parser"] in SUPPORTED_PARSERS
        for e in V1.values()
    )
    assert REGISTRY.check_registry() == []


@pytest.mark.parametrize("sid", sorted(V1))
def test_every_required_field_is_enforced(sid):
    """The minimal document passes; removing ANY required key fails, so no requirement is decorative."""
    schema = V1[sid]["schema"]
    doc = minimal(schema)
    assert validate_record(doc, schema) == []
    for path in required_json_paths(schema):
        assert validate_record(drop(doc, path), schema), f"{sid}: {path} not enforced"


@pytest.mark.parametrize(
    ("sid", "money_or_qty", "line"),
    [
        ("sim-native-v1/amazon/1", ["PromotionDiscount", "Amount"], "OrderItems"),
        ("sim-native-v1/amazon/2", ["promotion", "amount"], "orderItems"),
    ],
)
def test_amazon_net_amount_inputs_are_required(sid, money_or_qty, line):
    """Silver's net = price − promotion: a missing promotion would make net NULL silently, so it is required."""
    paths = required_json_paths(V1[sid]["schema"])
    assert f"$.{line}[0].{'.'.join(money_or_qty)}" in paths


def test_only_what_silver_needs_is_required():
    """Fields Silver never reads (seen in every sample row) stay optional: data may change them freely."""
    amazon = required_json_paths(V1["sim-native-v1/amazon/1"]["schema"])
    assert "$.OrderItems[0].SellerSKU" in amazon and "$.IsBusinessOrder" not in amazon
    shopee = required_json_paths(V1["sim-native-v1/shopee/1"]["schema"])
    assert not any("buyer_user_id" in p or "recipient_address" in p for p in shopee)


def test_types_are_checked_for_required_and_optional_fields():
    """A string quantity breaks the contract; an optional field may be absent but not of the wrong type."""
    schema = V1["sim-native-v1/shopee/1"]["schema"]
    doc = minimal(schema)
    bad = copy.deepcopy(doc)
    bad["response"]["order_list"][0]["item_list"][0]["model_quantity_purchased"] = "1"
    assert validate_record(bad, schema)
    typed_optional = copy.deepcopy(doc)
    typed_optional["response"]["order_list"][0]["region"] = 84
    assert validate_record(typed_optional, schema)


def test_dp1_validate_jobs_check_contracts_in_their_existing_scan():
    """No extra pass over Bronze: the contract aggregates sit in the same agg as the row counts."""
    for name in ("validate_order_snapshots.py", "validate_file_order_snapshots.py"):
        text = (ROOT / "spark_jobs/bronze" / name).read_text()
        agg = text.split("checked.agg(")[1].split(").first()")[0]
        assert 'F.count("*").alias("rows")' in agg and "*contract_aggregates()" in agg
        assert "raise ValueError(message)" in text
