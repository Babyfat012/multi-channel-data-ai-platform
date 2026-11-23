"""Data contracts for pipeline A (dp1 Bronze → dp2 Silver): subjects `sim-native-v1/*` in contracts/registry.json.

The dp1 validate tasks check Bronze before dp2 Silver reads it (diagram: "Check contracts"):

- every row, inside the aggregation validate already runs (no extra pass): the shape is registered and every
  required path is present (Spark `get_json_object`; arrays: first item);
- types and every array item: Python `validate_record` on the first SAMPLE_ROWS rows of each shape.

Any violation fails the task. dp2 Silver re-reads all of Bronze, so there is no quarantine to send rows to
without changing every Silver job. Required = what Silver needs for a correct row; see the schema files.
"""

from __future__ import annotations

import json
import re
from types import SimpleNamespace
from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from spark_jobs.lib.data_contracts.registry import SUPPORTED_PARSERS, Registry
from spark_jobs.lib.data_contracts.schema import required_json_paths, validate_record

if TYPE_CHECKING:  # hints only: unit tests import the validate jobs with a minimal fake pyspark
    from pyspark.sql import Column, DataFrame

PREFIX = "sim-native-v1/"
# ponytail: types are checked on the first rows of each shape only; a wrong type that appears later is missed
# (Silver's cast then gives NULL). Every row would need a Python UDF over the whole table.
SAMPLE_ROWS = 1000
EXAMPLES = 5


def v1_registry(registry: Registry | None = None, channel: str | None = None) -> SimpleNamespace:
    """Only the v1 entries (optionally one Channel's): the Spark expression has one branch per entry."""
    registry = registry or Registry()
    scope = PREFIX + (f"{channel}/" if channel else "")
    return SimpleNamespace(
        entries={sid: e for sid, e in registry.entries.items() if sid.startswith(scope)}
    )


def schema_id(channel: str, payload: Column) -> Column:
    """Which contract a row claims. Amazon has two shapes, told apart by the root identity key exactly as
    Silver's split_by_form does; a row with neither gets NULL ⇒ `unregistered_schema`, not silently dropped."""
    if channel == "amazon":
        return F.when(
            F.get_json_object(payload, "$.AmazonOrderId").isNotNull(), F.lit(PREFIX + "amazon/1")
        ).when(F.get_json_object(payload, "$.orderId").isNotNull(), F.lit(PREFIX + "amazon/2"))
    return F.lit(f"{PREFIX}{channel}/1")


def contract_reasons(bronze: DataFrame, registry) -> DataFrame:
    """Every row plus `reason` (null = passes) and `detail` (missing required paths), lazily: callers can
    aggregate it in the scan they already do (dp1 validate) instead of a second pass over the table."""
    parser = F.lit(None).cast("string")
    registered = F.lit(False)
    missing = F.array().cast("array<string>")
    for sid, entry in registry.entries.items():
        matches = F.col("contract_schema_id") == sid
        registered = registered | F.coalesce(matches, F.lit(False))
        parser = F.when(matches, F.lit(entry["parser"]).cast("string")).otherwise(parser)
        paths = []
        for path in required_json_paths(entry["schema"]):
            absent = F.get_json_object("raw_json", path).isNull()
            # JSON Schema items does not require non-empty arrays; skip checks under empty arrays.
            for match in re.finditer(r"\[0\]", path):
                prefix = path[: match.start()]
                absent = absent & ~F.coalesce(
                    F.get_json_object("raw_json", prefix) == "[]", F.lit(False)
                )
            paths.append(F.when(absent, F.lit(path)))
        required = F.filter(F.array(*paths).cast("array<string>"), lambda x: x.isNotNull())
        missing = F.when(matches, required).otherwise(missing)
    return (
        bronze.withColumn("contract_parser", parser)
        .withColumn("detail", F.concat_ws(", ", missing))
        .withColumn(
            "reason",
            F.when(~registered, "unregistered_schema")
            .when(
                ~F.coalesce(F.col("contract_parser").isin(*SUPPORTED_PARSERS), F.lit(False)),
                "no_parser",
            )
            .when(F.length("detail") > 0, "contract_violation"),
        )
    )


def with_contract(bronze: DataFrame, channel: str, payload: str, registry) -> DataFrame:
    """Bronze rows plus raw_json, contract_schema_id, reason (NULL = passes) and detail."""
    frame = bronze.withColumn("raw_json", F.col(payload)).withColumn(
        "contract_schema_id", schema_id(channel, F.col(payload))
    )
    return contract_reasons(frame, registry)


def contract_aggregates() -> list[Column]:
    """To add to validate's existing `agg`: failing rows and a few distinct reasons (bounded by distinct
    reason/shape/missing-path combinations, not by row count)."""
    failing = F.col("reason").isNotNull()
    example = F.concat_ws(
        " ", "reason", F.coalesce("contract_schema_id", F.lit("<no shape>")), "detail"
    )
    return [
        F.sum(F.when(failing, 1).otherwise(0)).alias("contract_rows"),
        F.slice(F.array_sort(F.collect_set(F.when(failing, example))), 1, EXAMPLES).alias(
            "contract_examples"
        ),
    ]


def type_errors(checked: DataFrame, registry, sample_rows: int = SAMPLE_ROWS) -> list[str]:
    """validate_record (types, every array item) on the first rows of each of this Channel's shapes."""
    errors = []
    for sid, entry in registry.entries.items():
        rows = (
            checked.filter(F.col("contract_schema_id") == sid)
            .select("raw_json")
            .limit(sample_rows)
            .collect()
        )
        for row in rows:
            try:
                document = json.loads(row["raw_json"])
            except (TypeError, ValueError):
                errors.append(f"{sid} $: not JSON")
                continue
            errors.extend(f"{sid} {e}" for e in validate_record(document, entry["schema"]))
    return errors


def failure(contract_rows: int, examples: list[str], typed: list[str]) -> str | None:
    """The error message to fail with, or None when the contract holds."""
    if not contract_rows and not typed:
        return None
    lines = [f"Contract check failed: {contract_rows:,} rows break a required path or shape"]
    lines += [f"  {e}" for e in examples or []]
    if typed:
        lines.append(f"{len(typed):,} type errors in the sampled rows, e.g.:")
        lines += [f"  {e}" for e in typed[:EXAMPLES]]
    return "\n".join(lines)
