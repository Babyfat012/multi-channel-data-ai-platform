"""Write the actual schema and completion time for receipts, with no Spark dependency."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path


def completed_at_utc() -> str:
    """Return the current time as ISO-8601 UTC, ending in Z."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _validate_type(schema, location):
    """Reject empty schemas or fields missing a name/type before replacing the old file."""
    if isinstance(schema, str) and schema:
        return
    if not isinstance(schema, dict) or not schema.get("type"):
        raise ValueError(f"Schema missing type: {location}")
    kind = schema["type"]
    if kind == "struct":
        fields = schema.get("fields")
        if not isinstance(fields, list) or not fields:
            raise ValueError(f"Schema struct needs non-empty fields: {location}")
        for field in fields:
            if (
                not isinstance(field, dict)
                or not isinstance(field.get("name"), str)
                or not field["name"]
            ):
                raise ValueError(f"Schema field missing name: {location}")
            _validate_type(field.get("type"), f"{location}.{field['name']}")
    elif kind == "array":
        _validate_type(schema.get("elementType"), f"{location}[]")
    elif kind == "map":
        _validate_type(schema.get("keyType"), f"{location}.key")
        _validate_type(schema.get("valueType"), f"{location}.value")
    elif not isinstance(kind, str):
        raise ValueError(f"Invalid schema type: {location}")


def write_receipt_schemas(tables: dict, receipt_path: Path | str) -> str:
    """Write schemas.json next to the receipt with an atomic rename; keeps schemas of jobs in the same run.

    Jobs in one receipt directory run sequentially. Each job passes exactly the tables
    it just wrote as {full name: df.schema.jsonValue()}, not taken from the contract.
    """
    if not isinstance(tables, dict) or not tables:
        raise ValueError("Schema tables needs a non-empty object")
    for name, schema in tables.items():
        if not isinstance(name, str) or not name:
            raise ValueError("Table schema missing full name")
        if not isinstance(schema, dict) or schema.get("type") != "struct":
            raise ValueError(f"Table schema needs type struct: {name}")
        _validate_type(schema, name)

    destination = Path(receipt_path).parent / "schemas.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    previous = (
        json.loads(destination.read_text(encoding="utf-8"))["tables"]
        if destination.exists()
        else {}
    )
    document = {"tables": {**previous, **tables}}
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=destination.parent,
            prefix=".schemas-",
            suffix=".tmp",
            delete=False,
        ) as stream:
            temporary = Path(stream.name)
            json.dump(document, stream, ensure_ascii=False, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, destination)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return destination.name
