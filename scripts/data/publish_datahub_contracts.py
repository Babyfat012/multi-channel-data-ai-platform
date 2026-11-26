"""Publish contracts/registry.json to DataHub as one Data Contract per Bronze dataset (diagram: CI → DataHub).

Run by the Jenkins `ci` job when contracts/** changes (or by hand after DataHub comes up). Per Channel:

- a schema assertion (DATA_SCHEMA) whose expected schema is the payload JSON Schema; a Channel with several active
  versions (Amazon v0 + 2026) gets `anyOf` of them, which is what the Bronze validator checks (each row matches
  one version). The urn is fixed (`contract_schema_urn`), so the Bronze validate task reports each run's result
  against it: DataHub OSS stores contracts but does not evaluate them (that is DataHub Cloud).
- the contract: that schema assertion + the Bronze row/provenance check (urn of task dp1_bronze.validate_<channel>)
  as data quality.

    uv run --with acryl-datahub==1.7.0.14 python -m scripts.data.publish_datahub_contracts [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import os

from spark_jobs.lib.assertion_result import contract_schema_urn
from spark_jobs.lib.data_contracts.registry import Registry

PREFIX = "sim-native-v1/"


def bronze_dataset_urn(channel: str) -> str:
    """The Bronze table dp1 writes and validates for a Channel."""
    return f"urn:li:dataset:(urn:li:dataPlatform:iceberg,lakehouse.bronze.raw_{channel}_orders_dp1,PROD)"


def contracts(registry: Registry) -> list[dict]:
    """One contract definition per sim-native-v1 Channel, from the active versions in the registry."""
    out = []
    for name, subject in sorted(registry.document["subjects"].items()):
        if not name.startswith(PREFIX):
            continue
        channel = name.removeprefix(PREFIX)
        schemas = [
            registry.entries[v["schema_id"]]["schema"]
            for v in subject["versions"]
            if v.get("status", "active") == "active"
        ]
        if not schemas:
            continue
        out.append(
            {
                "channel": channel,
                "dataset": bronze_dataset_urn(channel),
                "contract": f"urn:li:dataContract:platform-bronze-{channel}",
                "schema_assertion": contract_schema_urn(channel),
                "json_schema": schemas[0] if len(schemas) == 1 else {"anyOf": schemas},
                "quality_assertions": [f"urn:li:assertion:platform-dp1_bronze-validate_{channel}"],
            }
        )
    return out


def mcps(contract: dict) -> list:
    """DataHub change proposals for one contract (needs acryl-datahub, imported here only)."""
    from datahub.api.entities.datacontract.schema_assertion import SchemaAssertion
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import (
        DataContractPropertiesClass,
        DataContractStateClass,
        DataContractStatusClass,
        DataQualityContractClass,
        SchemaContractClass,
        StatusClass,
    )

    schema = SchemaAssertion.model_validate(
        {"type": "json-schema", "json-schema": contract["json_schema"]}
    )
    return [
        *schema.generate_mcp(contract["schema_assertion"], contract["dataset"]),
        *MetadataChangeProposalWrapper.construct_many(
            entityUrn=contract["contract"],
            aspects=[
                DataContractPropertiesClass(
                    entity=contract["dataset"],
                    schema=[SchemaContractClass(assertion=contract["schema_assertion"])],
                    dataQuality=[
                        DataQualityContractClass(assertion=urn)
                        for urn in contract["quality_assertions"]
                    ],
                ),
                StatusClass(removed=False),
                DataContractStatusClass(state=DataContractStateClass.ACTIVE),
            ],
        ),
    ]


def main() -> None:
    """Emit every contract; --dry-run prints the proposals instead (no DataHub needed)."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--server", default=os.getenv("DATAHUB_GMS_URL", "http://localhost:8089"))
    parser.add_argument("--contracts-dir", default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    items = contracts(Registry(args.contracts_dir))
    proposals = [m for c in items for m in mcps(c)]
    if args.dry_run:
        for m in proposals:
            print(json.dumps(m.to_obj(), sort_keys=True))
        return
    from datahub.emitter.rest_emitter import DatahubRestEmitter

    emitter = DatahubRestEmitter(args.server, token=os.getenv("DATAHUB_GMS_TOKEN"))
    emitter.test_connection()
    for m in proposals:
        emitter.emit_mcp(m)
    print(f"DataHub: {len(items)} Bronze Data Contracts ({', '.join(c['channel'] for c in items)})")


if __name__ == "__main__":
    main()
