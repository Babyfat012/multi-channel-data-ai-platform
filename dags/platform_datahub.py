"""Report the check results of one Airflow task run to DataHub, as assertion results.

PlatformSparkSubmitOperator collects the `DATAHUB_ASSERTION` lines its Spark validator printed
(spark_jobs/lib/assertion_result.py) and calls `report` when spark-submit ends, PASS or FAIL. Same GraphQL
mutations the DataHub SDK uses (upsertCustomAssertion, reportAssertionResult), sent with the stdlib so the
Airflow image needs no extra package. DataHub is where results are looked at, not the gate: the Spark validator
already passed or failed the task, so an unset DATAHUB_GMS_URL or an unreachable DataHub only logs a warning.
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.request
from collections.abc import Callable

log = logging.getLogger(__name__)

UPSERT = """
mutation upsertCustomAssertion($assertionUrn: String, $entityUrn: String!, $type: String!,
                               $description: String!, $platformName: String, $logic: String) {
  upsertCustomAssertion(urn: $assertionUrn, input: {entityUrn: $entityUrn, type: $type,
    description: $description, platform: {name: $platformName}, logic: $logic}) { urn }
}"""
REPORT = """
mutation reportAssertionResult($assertionUrn: String!, $timestampMillis: Long!, $type: AssertionResultType!,
                               $properties: [StringMapEntryInput!], $externalUrl: String) {
  reportAssertionResult(urn: $assertionUrn, result: {timestampMillis: $timestampMillis, type: $type,
    properties: $properties, externalUrl: $externalUrl})
}"""

Post = Callable[[str, dict], dict]


def dataset_urn(name: str) -> str:
    """Same platforms as scripts/data/publish_order_snapshots_datahub.py: warehouse.* is EC2 PostgreSQL."""
    platform = "postgres" if name.startswith("warehouse.") else "iceberg"
    return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{name},PROD)"


def assertion_urn(dag_id: str, task_id: str, record: dict) -> str:
    """Fixed urn from the record (contract-owned), else one per task (+ check name when a task has several)."""
    if record.get("urn"):
        return record["urn"]
    suffix = f"-{record['name']}" if record.get("name") else ""
    return f"urn:li:assertion:platform-{dag_id}-{task_id}{suffix}"


def graphql_post(server: str, token: str | None) -> Post:
    """POST to GMS /api/graphql; GraphQL errors come back with HTTP 200, so they are raised here."""

    def post(query: str, variables: dict) -> dict:
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        request = urllib.request.Request(
            f"{server.rstrip('/')}/api/graphql",
            data=json.dumps({"query": query, "variables": variables}).encode(),
            headers=headers,
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=15) as response:
            body = json.load(response)
        if body.get("errors"):
            raise RuntimeError(f"DataHub GraphQL error: {body['errors']}")
        return body["data"]

    return post


def report(
    records: list[dict], dag_id: str, task_id: str, run_id: str, log_url: str | None, post: Post
) -> None:
    """Define each custom assertion (idempotent), then record this run's result with run_id and metrics."""
    now = int(time.time() * 1000)
    for record in records:
        urn = assertion_urn(dag_id, task_id, record)
        if not record.get("urn"):
            check = f"{dag_id}.{task_id}" + (f" ({record['name']})" if record.get("name") else "")
            post(
                UPSERT,
                {
                    "assertionUrn": urn,
                    "entityUrn": dataset_urn(record["dataset"]),
                    "type": "Airflow Spark Validate",
                    "description": f"{check} checks {record['dataset']}",
                    "platformName": "PLATFORM Airflow",
                    "logic": record["logic"],
                },
            )
        post(
            REPORT,
            {
                "assertionUrn": urn,
                "timestampMillis": now,
                "type": "SUCCESS" if record["passed"] else "FAILURE",
                "properties": [
                    {"key": "run_id", "value": run_id},
                    *({"key": k, "value": str(v)} for k, v in sorted(record["metrics"].items())),
                ],
                "externalUrl": log_url,
            },
        )


def report_task(records: list[dict], context) -> None:
    """Operator entry point: never raises, the task result stays the Spark validator's."""
    if not records:
        return
    server = os.environ.get("DATAHUB_GMS_URL")
    if not server:
        log.warning("DATAHUB_GMS_URL not set: %d check results not sent to DataHub", len(records))
        return
    ti = context["ti"]
    try:
        report(
            records,
            ti.dag_id,
            ti.task_id,
            context["run_id"],
            getattr(ti, "log_url", None),
            graphql_post(server, os.environ.get("DATAHUB_GMS_TOKEN")),
        )
        log.info("Sent %d check results to DataHub", len(records))
    except Exception:  # noqa: BLE001 — DataHub down must not change the task result
        log.warning("Could not send check results to DataHub", exc_info=True)
