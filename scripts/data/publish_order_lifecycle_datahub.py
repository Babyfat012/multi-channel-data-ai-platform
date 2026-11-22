"""Build v2 DataHub MCPs from real tables, contracts and receipts; prints offline by default.

Only --publish sends over the network. --dry-run outputs exactly the MCP list used for a real send,
without importing the SDK, running Spark, or turning quarantine counts into failing checks.
"""

from __future__ import annotations

import argparse
import ast
import contextlib
import hashlib
import json
import math
import os
import re
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DAG_ID = "dp_order_lifecycle"
FLOW = f"urn:li:dataFlow:(airflow,{DAG_ID},prod)"
TASKS = ("check_contracts", "ingest_bronze", "build_silver", "build_gold", "validate_gold")


def constants(filename):
    """Read constants via AST so pyspark need not be imported and no job is executed."""
    tree = ast.parse((ROOT / "spark_jobs" / filename).read_text())
    values = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name):
            with contextlib.suppress(ValueError, TypeError):
                values[node.targets[0].id] = ast.literal_eval(node.value)
    return values


def inventory():
    """Get default table names and table tuples from the job code that actually writes them."""
    bronze = constants("bronze/ingest_order_lifecycle.py")
    silver = constants("silver/build_order_lifecycle.py")
    gold = constants("lib/order_lifecycle_gold.py")
    tree = ast.parse((ROOT / "spark_jobs/bronze/ingest_order_lifecycle.py").read_text())
    default = next(
        ast.literal_eval(k.value)
        for n in ast.walk(tree)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and n.func.attr == "add_argument"
        and any(isinstance(a, ast.Constant) and a.value == "--table" for a in n.args)
        for k in n.keywords
        if k.arg == "default"
    )
    bronze_name = f"lakehouse.{bronze['NAMESPACE']}.{default}"
    silver_names = {
        n: f"lakehouse.{silver['NAMESPACE']}."
        + ("lifecycle_quarantine_records" if n == "quarantine_records" else f"lifecycle_stg_{n}")
        for n in (*silver["TABLES"], "build_info")
    }
    gold_names = {
        n: f"lakehouse.gold.lifecycle_{n.removesuffix('_v2')}" for n in gold["GOLD_TABLES"]
    }
    # Gold reads ONLY Silver (observation history + coverage validity live in Silver), no longer Bronze.
    gold_inputs = [silver_names[n] for n in gold["SILVER_TABLES"]]
    return bronze_name, silver_names, gold_names, gold_inputs


def dataset_urn(name, platform="iceberg"):
    """Build a URN from platform and the v2 namespace, never the historical v1 tables."""
    return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{name},PROD)"


def mcp(entity_type, urn, aspect_name, value):
    """Wrap a MetadataChangeProposal in DataHub's exact REST JSON shape."""
    return {
        "entityType": entity_type,
        "entityUrn": urn,
        "changeType": "UPSERT",
        "aspectName": aspect_name,
        "aspect": {
            "value": json.dumps(value, ensure_ascii=False, sort_keys=True),
            "contentType": "application/json",
        },
    }


def schema_fields(schema, prefix="", optional=False):
    """Flatten a JSON Schema or Spark StructType, keeping the original paths and nullability."""
    fields = []
    spark = schema.get("type") == "struct"
    entries = (
        ((f["name"], f["type"], f.get("nullable", True)) for f in schema.get("fields", []))
        if spark
        else (
            (key, value, key not in schema.get("required", []))
            for key, value in schema.get("properties", {}).items()
        )
    )
    for name, child, nullable in entries:
        child = {"type": child} if isinstance(child, str) else child
        path = f"{prefix}.{name}" if prefix else name
        kind = child.get("type", "string")
        if isinstance(kind, dict):
            child, kind = kind, kind.get("type", "string")
        kinds = kind if isinstance(kind, list) else [kind]
        primary = next((t for t in kinds if t != "null"), "null")
        avro = {
            "object": "Record",
            "struct": "Record",
            "array": "Array",
            "boolean": "Boolean",
            "integer": "Number",
            "number": "Number",
            "long": "Number",
            "int": "Number",
            "double": "Number",
            "float": "Number",
            "date": "Date",
            "timestamp": "Time",
            "null": "Null",
            "binary": "Bytes",
        }.get(primary, "String")
        fields.append(
            {
                "fieldPath": path,
                "type": {"type": {f"com.linkedin.schema.{avro}Type": {}}},
                "nativeDataType": json.dumps(kind),
                "nullable": optional or nullable or "null" in kinds,
            }
        )
        if primary in ("object", "struct"):
            fields.extend(schema_fields(child, path, optional or nullable))
        if primary == "array":
            item = child.get("items", child.get("elementType", {}))
            if isinstance(item, dict):
                fields.extend(schema_fields(item, path + "[]", optional or nullable))
    return fields


def source_inventory(contracts_dir, source_root, walmart_jdbc):
    """Read the contract schema for each feed; use the manifest, if present, for the real directory."""
    registry = json.loads((contracts_dir / "registry.json").read_text())
    source_uri = str(source_root).rstrip("/")
    remote = "://" in source_uri
    source_root = Path(source_root)
    manifest_path = source_root / "manifest.json"
    manifest = (
        json.loads(manifest_path.read_text()) if not remote and manifest_path.is_file() else None
    )
    feeds = {}
    for subject, item in registry["subjects"].items():
        group, feed = subject.split("/", 1)
        if group == "sim-native-v2":
            feed += ".orders"
        selected = item["versions"]
        if manifest and group == "sim-native-v2":
            channel = feed.removesuffix(".orders")
            schema_id = manifest["native_sources"][channel]["schema_id"]
            selected = [v for v in selected if v["schema_id"] == schema_id]
        schemas = [json.loads((contracts_dir / v["schema_path"]).read_text()) for v in selected]
        # Several registered versions: union the paths, keeping each version's native type.
        fields = {}
        for schema in schemas:
            for field in schema_fields(schema):
                key = field["fieldPath"]
                if key in fields and fields[key]["nativeDataType"] != field["nativeDataType"]:
                    fields[key]["nativeDataType"] += " | " + field["nativeDataType"]
                else:
                    fields[key] = field
                fields[key]["description"] = "Contract: " + ", ".join(
                    v["schema_id"] for v in selected
                )
        paths = (
            [f"{source_uri}/dataset_version=*/producer=*/feed={feed}"]
            if remote
            else [str(source_root / "dataset_version=*" / "producer=*" / f"feed={feed}")]
        )
        if manifest:
            paths = sorted(
                {
                    str(source_root / str(Path(f["path"]).parent.parent))
                    for f in manifest["files"]
                    if f["role"] == "source" and f["feed"] == feed
                }
            )
        for path in paths:
            feeds[path] = {
                "platform": "file",
                "feed": feed,
                "fields": list(fields.values()),
                "schemas": schemas,
                "active": not (walmart_jdbc and feed == "walmart.orders"),
            }
        if feed == "walmart.orders":
            # Always register the v2 table; only wire it as input when the DAG enables JDBC.
            feeds[walmart_jdbc or "walmart_lifecycle.orders"] = {
                "platform": "postgres",
                "feed": feed,
                "fields": list(fields.values()),
                "schemas": schemas,
                "active": bool(walmart_jdbc),
            }
    return feeds


def load_receipts(args):
    """Read the given receipts, refusing missing files or a missing original run_id."""
    paths = {}
    if args.receipt_dir:
        if not args.receipt_dir.is_dir():
            raise ValueError(f"Receipt directory does not exist: {args.receipt_dir}")
        paths.update(
            {
                t: args.receipt_dir / f"{t}.json"
                for t in TASKS
                if (args.receipt_dir / f"{t}.json").is_file()
            }
        )
    for value in args.receipt:
        task, filename = value.split("=", 1)
        if task not in TASKS:
            raise ValueError(f"Receipt task does not belong to the DAG: {task}")
        paths[task] = Path(filename)
    if not paths:
        raise ValueError(
            "Need at least one completed receipt via --receipt-dir or --receipt TASK=PATH"
        )
    receipts = {}
    schemas = {}
    for task, path in paths.items():
        doc = json.loads(path.read_text())
        if not isinstance(doc.get("run_id"), str) or not doc["run_id"]:
            raise ValueError(f"Receipt is missing the original run_id: {path}")
        receipts[task] = (doc, path)
        if doc.get("schema_file"):
            schema_path = Path(doc["schema_file"])
            if not schema_path.is_absolute():
                schema_path = path.parent / schema_path
            schemas.update(json.loads(schema_path.read_text())["tables"])
    return receipts, schemas


def build_mcps(args):
    """Build all datasets, jobs, lineage edges, and one assertion per check."""
    receipts, schemas = load_receipts(args)
    bronze, silver, gold, gold_inputs = inventory()
    sources = source_inventory(args.contracts_dir, args.source_root, args.walmart_jdbc)
    datasets = {dataset_urn(name, meta["platform"]): (name, meta) for name, meta in sources.items()}
    for name in (bronze, *silver.values(), *gold.values()):
        datasets[dataset_urn(name)] = (
            name,
            {
                "platform": "iceberg",
                "fields": schema_fields(schemas.get(name, {})),
                "schemas": [schemas[name]] if name in schemas else [],
            },
        )
    result = []
    for urn, (name, meta) in datasets.items():
        description = f"PLATFORM v2 {meta['platform']}: {name}"
        if not meta["fields"]:
            description += (
                "; no schema fields yet (the job has not emitted a JSON schema in its receipt)."
            )
        result.append(
            mcp(
                "dataset",
                urn,
                "datasetProperties",
                {
                    "name": name,
                    "description": description,
                    "customProperties": {
                        "pipeline": DAG_ID,
                        "schema_source": "receipt"
                        if name in schemas
                        else "contracts"
                        if meta["fields"]
                        else "unavailable",
                    },
                },
            )
        )
        if meta["fields"]:
            raw = json.dumps(meta["schemas"], sort_keys=True)
            result.append(
                mcp(
                    "dataset",
                    urn,
                    "schemaMetadata",
                    {
                        "schemaName": name,
                        "platform": "urn:li:dataPlatform:" + meta["platform"],
                        "version": 0,
                        "hash": hashlib.sha256(raw.encode()).hexdigest(),
                        "platformSchema": {"com.linkedin.schema.OtherSchema": {"rawSchema": raw}},
                        "fields": meta["fields"],
                    },
                )
            )
    source_inputs = [
        dataset_urn(name, meta["platform"]) for name, meta in sources.items() if meta["active"]
    ]
    jobs = {
        "check_contracts": ([], []),
        "ingest_bronze": (source_inputs, [dataset_urn(bronze)]),
        "build_silver": ([dataset_urn(bronze)], [dataset_urn(n) for n in silver.values()]),
        "build_gold": (
            [dataset_urn(n) for n in gold_inputs],
            [dataset_urn(n) for n in gold.values()],
        ),
        "validate_gold": ([dataset_urn(n) for n in [*gold_inputs, *gold.values()]], []),
    }
    result.append(
        mcp(
            "dataFlow",
            FLOW,
            "dataFlowInfo",
            {"name": DAG_ID, "description": "v2 source DAG, manual trigger"},
        )
    )
    upstreams = {}
    previous = None
    for task, (inputs, outputs) in jobs.items():
        urn = f"urn:li:dataJob:({FLOW},{task})"
        result.append(
            mcp("dataJob", urn, "dataJobInfo", {"name": task, "type": "COMMAND", "flowUrn": FLOW})
        )
        result.append(
            mcp(
                "dataJob",
                urn,
                "dataJobInputOutput",
                {
                    "inputDatasets": inputs,
                    "outputDatasets": outputs,
                    "inputDatajobs": [previous] if previous else [],
                },
            )
        )
        previous = urn
        for target in outputs:
            upstreams.setdefault(target, set()).update(inputs)
    for target, inputs in upstreams.items():
        result.append(
            mcp(
                "dataset",
                target,
                "upstreamLineage",
                {
                    "upstreams": [
                        {"dataset": source, "type": "TRANSFORMED"} for source in sorted(inputs)
                    ],
                },
            )
        )
    for task, (doc, path) in receipts.items():
        checks = doc.get("checks", {})
        if not isinstance(checks, dict):
            raise ValueError(f"checks is not an object: {path}")
        timestamp = (
            int(
                datetime.fromisoformat(doc["completed_at"].replace("Z", "+00:00")).timestamp()
                * 1000
            )
            if doc.get("completed_at")
            else int(path.stat().st_mtime * 1000)
        )
        assertee = dataset_urn(
            silver["line_lifecycle"]
            if task == "build_silver"
            else bronze
            if task == "ingest_bronze"
            else gold["fact_order_lines"]
        )
        for check, value in sorted(checks.items()):
            if (
                not isinstance(value, (int, float))
                or isinstance(value, bool)
                or not math.isfinite(value)
            ):
                raise ValueError(
                    f"Check needs a finite number per the v2 job: {path}: {check}={value!r}"
                )
            check_id = re.sub(r"[^A-Za-z0-9_-]", "_", check)
            if check_id != check:
                check_id += "-" + hashlib.sha256(check.encode()).hexdigest()[:12]
            urn = f"urn:li:assertion:platform-{DAG_ID}-{task}-{check_id}-v2"
            result.append(
                mcp(
                    "assertion",
                    urn,
                    "assertionInfo",
                    {
                        "type": "CUSTOM",
                        "description": f"{task}: {check} = 0; a nonzero violation count is FAIL",
                        "customAssertion": {
                            "type": "PLATFORM v2 reconcile/validate",
                            "entity": assertee,
                            "logic": f"{check} == 0",
                        },
                        "customProperties": {"task_id": task, "receipt": str(path)},
                    },
                )
            )
            result.append(
                mcp(
                    "assertion",
                    urn,
                    "assertionRunEvent",
                    {
                        "timestampMillis": timestamp,
                        "runId": doc["run_id"],
                        "asserteeUrn": assertee,
                        "status": "COMPLETE",
                        "assertionUrn": urn,
                        "result": {
                            "type": "SUCCESS" if value == 0 else "FAILURE",
                            "nativeResults": {
                                "run_id": doc["run_id"],
                                "check": check,
                                "value": str(value),
                                "timestamp_source": "completed_at"
                                if doc.get("completed_at")
                                else "receipt_mtime",
                                "receipt_status": str(doc.get("status", "not_recorded")),
                            },
                        },
                    },
                )
            )
    return result


def main():
    """Print MCPs by default; only send to GMS when the operator explicitly picks --publish."""
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run", action="store_true", help="Print all MCPs as JSON, no network calls"
    )
    mode.add_argument("--publish", action="store_true", help="Send these exact MCPs to GMS")
    parser.add_argument("--receipt-dir", type=Path)
    parser.add_argument("--receipt", action="append", default=[], metavar="TASK=PATH")
    parser.add_argument("--contracts-dir", type=Path, default=ROOT / "contracts")
    parser.add_argument("--source-root", default="/data/order_lifecycle")
    parser.add_argument("--walmart-jdbc", default=None)
    parser.add_argument("--server", default=os.getenv("DATAHUB_GMS_URL", "http://localhost:8089"))
    args = parser.parse_args()
    if args.walmart_jdbc and not re.fullmatch(
        r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*", args.walmart_jdbc
    ):
        parser.error("--walmart-jdbc requires SCHEMA.TABLE")
    proposals = build_mcps(args)
    if not args.publish:
        print(json.dumps(proposals, ensure_ascii=False, indent=2, sort_keys=True))
        return
    # The SDK is loaded only when sending; the emitter gets exactly the MCPs printed in dry-run.
    from datahub.emitter.rest_emitter import DatahubRestEmitter
    from datahub.metadata.schema_classes import MetadataChangeProposalClass

    emitter = DatahubRestEmitter(gms_server=args.server, token=os.getenv("DATAHUB_GMS_TOKEN"))
    for proposal in proposals:
        emitter.emit_mcp(MetadataChangeProposalClass.from_obj(proposal))
    emitter.flush()
    print(f"Sent {len(proposals)} v2 MCPs")


if __name__ == "__main__":
    main()
