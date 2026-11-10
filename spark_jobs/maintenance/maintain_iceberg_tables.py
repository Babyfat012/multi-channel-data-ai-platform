"""Iceberg maintenance: read metadata, plan/execute, check data and save a JSON receipt.

Fingerprint = (count(*), sum(cast(xxhash64(all columns in schema order) AS decimal(38,0)))).
The sum does not depend on row/file order and does not overflow bigint; it is a probabilistic checksum,
not a mathematical proof that two multisets are equal. Metadata counts are not used.
The module only imports pyspark when building the session, so the CLI/help loads without Spark.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from spark_jobs.lib.maintenance_policy import (
    count_probe_files,
    ensure_refs_survive_expiration,
    execute_actions,
    load_config,
    plan_maintenance,
    validated_config,
)

ROOT = Path(__file__).resolve().parents[2]


def identifier(value):
    """Quote each identifier part; requires an explicit catalog.namespace.table."""
    parts = value.split(".")
    if len(parts) < 3 or any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", part) for part in parts):
        raise ValueError(f"Invalid table name: {value}")
    return ".".join(f"`{part}`" for part in parts)


def column_name(value):
    """Quote a Spark column name, including names containing backticks."""
    return "`" + value.replace("`", "``") + "`"


def literal(value):
    """Build a Spark SQL literal; reject strings with backslashes to avoid ambiguous escaping."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, datetime):
        return "TIMESTAMP '" + value.strftime("%Y-%m-%d %H:%M:%S.%f") + "'"
    if isinstance(value, date):
        return "DATE '" + value.isoformat() + "'"
    value = str(value)
    if "\\" in value:
        raise ValueError("SQL literal does not support backslashes")
    return "'" + value.replace("'", "\\'") + "'"


def rows(spark, query):
    """Collect metadata/procedure results into JSON-serializable dicts."""
    return [row.asDict(recursive=True) for row in spark.sql(query).collect()]


def snapshot_id(spark, table):
    """Read the current snapshot from the main ref, not guessed from timestamps."""
    current = rows(spark, f"SELECT snapshot_id FROM {identifier(table)}.refs WHERE name = 'main'")
    return current[0]["snapshot_id"] if current else None


def read_metadata(spark, table):
    """Capture files/snapshots/refs/partitions/properties for the pure-Python policy."""
    name = identifier(table)
    files = rows(spark, f"SELECT * FROM {name}.files WHERE content = 0")
    return {
        "files": files,
        "snapshots": rows(spark, f"SELECT * FROM {name}.snapshots"),
        "refs": rows(spark, f"SELECT * FROM {name}.refs"),
        "partitions": rows(spark, f"SELECT * FROM {name}.partitions"),
        "properties": {
            row["key"]: row["value"] for row in rows(spark, f"SHOW TBLPROPERTIES {name}")
        },
        "current_snapshot_id": snapshot_id(spark, table),
    }


def fingerprint(spark, table, version=None):
    """Count logical rows and the decimal sum of xxhash64 over all columns, using the same schema."""
    relation = identifier(table)
    if version is not None:
        relation += " VERSION AS OF " + literal(version)
    frame = spark.sql(f"SELECT * FROM {relation}")
    columns = ", ".join(column_name(column) for column in frame.columns)
    result = frame.selectExpr(
        "count(*) AS rows",
        f"coalesce(sum(cast(xxhash64({columns}) AS decimal(38,0))), 0) AS fingerprint",
    ).first()
    return {
        "rows": result["rows"],
        "fingerprint": str(result["fingerprint"]),
        "columns": frame.columns,
    }


def probe_where(probe):
    """Turn a probe's AND of closed ranges into a Spark SQL expression."""
    clauses = []
    for column, (low, high) in probe["ranges"].items():
        if low is not None:
            clauses.append(f"{column_name(column)} >= {literal(low)}")
        if high is not None:
            clauses.append(f"{column_name(column)} <= {literal(high)}")
    return " AND ".join(clauses) or "true"


def measure(spark, table, config, metadata=None):
    """Measure checksum, files/bytes/snapshots and probes: 1 warmup + 3 runs, n=3."""
    metadata = metadata or read_metadata(spark, table)
    result = {
        **fingerprint(spark, table),
        "data_files": len(metadata["files"]),
        "total_bytes": sum(file["file_size_in_bytes"] for file in metadata["files"]),
        "snapshots": len(metadata["snapshots"]),
        "probes": {},
    }
    if config["zorder_columns"]:
        for probe in config["probe_predicates"]:
            query = f"SELECT count(*) FROM {identifier(table)} WHERE {probe_where(probe)}"
            spark.sql(query).collect()
            samples = []
            for _ in range(3):
                started = time.perf_counter()
                spark.sql(query).collect()
                samples.append(time.perf_counter() - started)
            result["probes"][probe["name"]] = {
                "candidate_data_files": count_probe_files(metadata["files"], probe["ranges"]),
                "seconds": samples,
                "n": 3,
                "warmup": 1,
            }
    return result


def assert_same(before, after):
    """Fail when the row count, checksum or column order/schema changes."""
    for key in ("rows", "fingerprint", "columns"):
        if before[key] != after[key]:
            raise ValueError(f"Data check failed: {key}: {before[key]} != {after[key]}")


def partition_where(spark, table, partitions):
    """Translate identity/time partitions into source-column predicates; fail closed on other specs.

    Partition transform names are never used directly as column names. Tables with an evolved spec
    other than the current one, or bucket/truncate, are rejected before any procedure.
    """
    jtable = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        spark._jsparkSession, table
    )
    spec = jtable.spec()
    if jtable.specs().size() != 1:
        raise ValueError(
            "Table has partition spec evolution; needs a dedicated mapping before compaction"
        )
    fields = list(spec.fields())
    clauses = []
    for partition in partitions:
        if not partition:
            return "true"
        predicates = []
        if set(partition) != {field.name() for field in fields}:
            raise ValueError("Partition spec evolution needs a dedicated mapping; stopping safely")
        for field in fields:
            value = partition[field.name()]
            source_name = str(jtable.schema().findColumnName(field.sourceId()))
            if "." in source_name:
                raise ValueError("Partitioning on a nested column needs a dedicated mapping")
            column = column_name(source_name)
            transform = str(field.transform())
            if value is None:
                predicates.append(f"{column} IS NULL")
            elif transform == "identity":
                predicates.append(f"{column} = {literal(value)}")
            elif transform in ("year", "month", "day", "hour"):
                epoch = datetime(1970, 1, 1)
                if transform == "year":
                    start, end = datetime(1970 + value, 1, 1), datetime(1971 + value, 1, 1)
                elif transform == "month":
                    year, month = 1970 + value // 12, value % 12 + 1
                    start = datetime(year, month, 1)
                    end = datetime(year + (month == 12), month % 12 + 1, 1)
                else:
                    delta = timedelta(days=1) if transform == "day" else timedelta(hours=1)
                    start, end = epoch + value * delta, epoch + (value + 1) * delta
                predicates.append(f"({column} >= {literal(start)} AND {column} < {literal(end)})")
            else:
                raise ValueError(f"Unsupported partition transform: {transform}")
        clauses.append("(" + " AND ".join(predicates) + ")")
    return " OR ".join(clauses)


def action_sql(spark, table, action):
    """Build Iceberg 1.11 procedures; quote where as a string parameter."""
    name = identifier(table)
    catalog, short = table.split(".", 1)
    system = f"`{catalog}`.system"
    kind = action["kind"]
    if kind in ("compact", "zorder"):
        options = {"target-file-size-bytes": str(action["target_file_size_bytes"])}
        if kind == "compact":
            options["min-input-files"] = str(action["min_input_files"])
            extra = ", where => " + literal(partition_where(spark, table, action["partitions"]))
            strategy = "binpack"
        else:
            # rewrite-all is required: after binpack files may already be large enough but not yet z-ordered.
            options["rewrite-all"] = "true"
            extra = ", sort_order => " + literal("zorder(" + ",".join(action["columns"]) + ")")
            strategy = "sort"
        mapping = ", ".join(f"{literal(key)}, {literal(value)}" for key, value in options.items())
        return (
            f"CALL {system}.rewrite_data_files(table => {literal(short)}, strategy => {literal(strategy)}"
            f"{extra}, options => map({mapping}))"
        )
    if kind == "expire_snapshots":
        return (
            f"CALL {system}.expire_snapshots(table => {literal(short)}, "
            f"older_than => {literal(datetime.fromisoformat(action['older_than']))}, "
            f"retain_last => {action['retain_last']})"
        )
    if kind == "remove_orphans":
        # prefix_listing: list through the table's FileIO (S3FileIO/HadoopFileIO), not through the Hadoop
        # FileSystem; Hadoop has no FileSystem for the s3:// scheme that Lakekeeper on AWS returns.
        return (
            f"CALL {system}.remove_orphan_files(table => {literal(short)}, "
            f"older_than => {literal(datetime.fromisoformat(action['older_than']))}, "
            f"dry_run => {literal(action['dry_run'])}, prefix_listing => true)"
        )
    raise ValueError(f"Unknown action: {kind} on {name}")


def code_hashes():
    """Attach code/config sha256 to the receipt to trace which version ran."""
    paths = [Path(__file__), ROOT / "spark_jobs/lib/maintenance_policy.py"]
    return {
        str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths
    }


def write_receipt(path, receipt):
    """Write JSON atomically in the same directory, including failure receipts."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(receipt, ensure_ascii=False, indent=2, default=str) + "\n")
    temp.replace(path)


def check_tags(spark, table, tags):
    """Read back each generation tag with VERSION AS OF and compare against the earlier fingerprint."""
    for tag, expected in tags.items():
        assert_same(expected, fingerprint(spark, table, tag))


def run_table(
    spark, table, config, receipt, execute=False, transient_failures=0, fatal_failure=False
):
    """Run one table; the receipt is updated in place even when it fails midway."""
    config = validated_config(config)
    spark.conf.set("spark.sql.parser.escapedStringLiterals", "false")
    receipt.update(table=table, config=config, attempts=[], status="running")
    try:
        metadata = read_metadata(spark, table)
        plan = plan_maintenance(metadata, config)
        receipt["plan"] = plan
        if any(action["kind"] == "expire_snapshots" for action in plan["actions"]):
            ensure_refs_survive_expiration(metadata)
        # Preflight all SQL so an unsupported partition does not cause a half-done execution.
        statements = [action_sql(spark, table, action) for action in plan["actions"]]
        receipt["sql"] = statements
        if not execute:
            receipt["status"] = "planned"
            return receipt
        receipt["before"] = measure(spark, table, config, metadata)
        receipt["generation_before"] = {
            tag: fingerprint(spark, table, tag) for tag in plan["generation_tags"]
        }
        injected = 0
        first = plan["actions"][0] if plan["actions"] else None

        def perform(action):
            """Execute the procedure, inject before CALL and record the z-order marker after commit."""
            nonlocal injected
            if action is first:
                if fatal_failure:
                    raise ValueError("Injected fatal failure before procedure")
                if injected < transient_failures:
                    injected += 1
                    raise RuntimeError(
                        "CommitFailedException: injected transient failure before procedure"
                    )
            if action["kind"] == "expire_snapshots":
                # Re-check right before CALL because compact/zorder may run past the ref TTL.
                ensure_refs_survive_expiration(read_metadata(spark, table))
            result = rows(spark, action_sql(spark, table, action))
            if action["kind"] == "zorder":
                current = snapshot_id(spark, table)
                spark.sql(
                    f"ALTER TABLE {identifier(table)} SET TBLPROPERTIES "
                    f"('platform.maintenance.last_zorder_snapshot_id' = '{current}')"
                ).collect()
            if action["kind"] == "expire_snapshots":
                check_tags(spark, table, receipt["generation_before"])
            return result

        execute_actions(plan["actions"], perform, config, receipt["attempts"])
        receipt["after"] = measure(spark, table, config)
        assert_same(receipt["before"], receipt["after"])
        check_tags(spark, table, receipt["generation_before"])
        receipt["generation_after"] = {
            tag: fingerprint(spark, table, tag) for tag in plan["generation_tags"]
        }
        receipt["status"] = "success"
        return receipt
    except Exception as error:
        receipt.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise


def validate_receipt(spark, receipt):
    """A new session must read the same current data and the receipt's generations."""
    if receipt.get("status") != "success" or not receipt.get("tables"):
        raise ValueError("Receipt is not from a successful execute")
    for entry in receipt["tables"]:
        if entry.get("status") != "success":
            raise ValueError("Table maintenance did not succeed")
        assert_same(entry["before"], entry["after"])
        assert_same(entry["after"], fingerprint(spark, entry["table"]))
        check_tags(spark, entry["table"], entry["generation_before"])
    return {"status": "validated", "validated_at": datetime.now(timezone.utc).isoformat()}


def arguments():
    """CLI shared by plan/execute/validate and an offline local catalog."""
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--plan-only", action="store_true")
    mode.add_argument("--execute", action="store_true")
    mode.add_argument("--validate-receipt")
    parser.add_argument("--config", default=str(ROOT / "configs/iceberg_maintenance.json"))
    parser.add_argument(
        "--config-json", help="JSON from an Airflow Variable, same schema as --config"
    )
    parser.add_argument("--tables", help="JSON array or comma-separated list of tables")
    parser.add_argument("--receipt", default="/tmp/platform-maintenance-receipt.json")
    parser.add_argument("--catalog", default="lakehouse")
    parser.add_argument(
        "--local-warehouse", help="Use a local Hadoop catalog instead of build_spark REST"
    )
    parser.add_argument("--inject-transient-failures", type=int, default=0)
    parser.add_argument("--inject-fatal-failure", action="store_true")
    return parser.parse_args()


def build_session(args):
    """Production uses the existing helper; local Hadoop does not fetch packages over the network."""
    if args.local_warehouse:
        from pyspark.sql import SparkSession

        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", args.catalog):
            raise ValueError("Invalid catalog")
        return (
            SparkSession.builder.master("local[2]")
            .appName("platform-maintenance-local")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config(f"spark.sql.catalog.{args.catalog}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{args.catalog}.type", "hadoop")
            .config(f"spark.sql.catalog.{args.catalog}.warehouse", args.local_warehouse)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
    from spark_jobs.lib.iceberg_session import build_spark

    return build_spark("platform-iceberg-maintenance", shuffle_partitions=4)


def main():
    """Write a failure receipt and exit non-zero; validate does not overwrite the execute receipt."""
    args = arguments()
    receipt = {
        "status": "running",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "mode": "execute" if args.execute else "plan",
        "code_sha256": code_hashes(),
        "tables": [],
    }
    spark = None
    try:
        if args.inject_transient_failures < 0:
            raise ValueError("Injection failure count must be >=0")
        if args.validate_receipt:
            spark = build_session(args)
            original = json.loads(Path(args.validate_receipt).read_text())
            print(json.dumps(validate_receipt(spark, original)))
            return
        if args.config_json:
            payload = args.config_json
            data = json.loads(payload)
            if not isinstance(data.get("tables"), dict) or not data["tables"]:
                raise ValueError("Config needs non-empty tables")
            configs = {table: validated_config(config) for table, config in data["tables"].items()}
        else:
            payload = Path(args.config).read_text()
            configs = load_config(args.config)
        receipt["config_sha256"] = hashlib.sha256(payload.encode()).hexdigest()
        tables = (
            json.loads(args.tables)
            if args.tables and args.tables.lstrip().startswith("[")
            else args.tables.split(",")
            if args.tables
            else list(configs)
        )
        if not isinstance(tables, list) or not tables:
            raise ValueError("Table list must be non-empty")
        for table in tables:
            identifier(table)
            if table.split(".")[0] != args.catalog or table not in configs:
                raise ValueError(f"Table has no config or a different catalog: {table}")
        spark = build_session(args)
        for index, table in enumerate(tables):
            entry = {}
            receipt["tables"].append(entry)
            run_table(
                spark,
                table,
                configs[table],
                entry,
                args.execute,
                args.inject_transient_failures if index == 0 else 0,
                args.inject_fatal_failure if index == 0 else False,
            )
        receipt["status"] = "success" if args.execute else "planned"
    except Exception as error:
        receipt.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        if not args.validate_receipt:
            receipt["finished_at"] = datetime.now(timezone.utc).isoformat()
            write_receipt(args.receipt, receipt)
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
