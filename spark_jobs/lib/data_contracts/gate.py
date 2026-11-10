"""Pure-Python fixture gate: checks every row and every array element."""

import json
from collections import Counter
from pathlib import Path

from spark_jobs.lib.data_contracts.registry import SUPPORTED_PARSERS
from spark_jobs.lib.data_contracts.schema import validate_record


def gate_root(root, registry):
    """Follow the manifest, preferring the label in the row; quarantine does not stop other feeds."""
    root = Path(root)
    manifest = json.loads((root / "manifest.json").read_text())
    files = []
    reasons, schemas = Counter(), Counter()
    for source in manifest["files"]:
        if source["role"] != "source":
            continue
        groups = {}
        for line in (root / source["path"]).read_text().splitlines():
            row = json.loads(line)
            sid = row.get("schema_id") or manifest["native_sources"].get(
                source["feed"].removesuffix(".orders"), {}
            ).get("schema_id")
            entry = registry.entries.get(sid)
            detail = []
            if entry is None:
                reason = "unregistered_schema"
            elif entry["parser"] not in SUPPORTED_PARSERS:
                reason = "no_parser"
            else:
                detail = validate_record(row, entry["schema"])
                reason = "contract_violation" if detail else None
            group = groups.setdefault(
                sid,
                {
                    "file": source["path"],
                    "feed": source["feed"],
                    "schema_id": sid,
                    "rows": 0,
                    "accepted": 0,
                    "by_reason": {},
                    "details": [],
                },
            )
            group["rows"] += 1
            if reason:
                reasons[reason] += 1
                schemas[sid] += 1
                group["by_reason"][reason] = group["by_reason"].get(reason, 0) + 1
                if detail:
                    group["details"].append(detail)
            else:
                group["accepted"] += 1
        files.extend(groups.values())
    return {
        "files": files,
        "accepted": sum(f["accepted"] for f in files),
        "quarantine": {
            "total": sum(reasons.values()),
            "by_reason": dict(reasons),
            "by_schema_id": dict(schemas),
        },
    }
