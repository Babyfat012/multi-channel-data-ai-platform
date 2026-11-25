"""One line per check result, printed by a Spark validator and picked up by Airflow for DataHub.

The validator prints `DATAHUB_ASSERTION {json}` before it raises (a failed check must still be reported);
PlatformSparkSubmitOperator (dags/platform_datahub.py) reads these lines from spark-submit output and reports
each one as an assertion result of that Airflow run. No pyspark here, so pytest checks it directly.
"""

from __future__ import annotations

import json

MARKER = "DATAHUB_ASSERTION "


def emit(
    dataset: str,
    passed: bool,
    metrics: dict[str, int | float | str | bool],
    logic: str,
    name: str | None = None,
    urn: str | None = None,
) -> None:
    """Print one result line.

    `name` tells apart several checks of one task (Gold: 4); the assertion urn is then
    `urn:li:assertion:platform-<dag>-<task>-<name>`. `urn` fixes the assertion instead, for assertions owned by
    a Data Contract (schema contract, published from CI).
    """
    record = {
        "dataset": dataset,
        "passed": bool(passed),
        "metrics": metrics,
        "logic": logic,
        "name": name,
        "urn": urn,
    }
    print(MARKER + json.dumps(record, sort_keys=True, default=str), flush=True)


def parse(line: str) -> dict | None:
    """The record of a result line, or None for any other output line."""
    _, marker, payload = line.strip().partition(MARKER)
    if not marker:
        return None
    return json.loads(payload)


def contract_schema_urn(channel: str) -> str:
    """Schema assertion of the Bronze Data Contract of a Channel (scripts/data/publish_datahub_contracts.py)."""
    return f"urn:li:assertion:platform-contract-schema-{channel}"
