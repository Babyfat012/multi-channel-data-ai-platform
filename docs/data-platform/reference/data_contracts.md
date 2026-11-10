# Data contracts — data contracts, schema registry and the Silver v2 quarantine gate

Package G puts the v2 source contracts into `contracts/`, automatically compares schema evolution and quarantines
unsupported data before parsing. The demo uses the small fixture from
[package B](source_order_lifecycle_fixtures.md); it is not evidence at 100 GB scale.

## Source problems and goals

Three gaps were recorded during the review:

1. Native order payloads do not carry a `schema_id`; the label lives in
   `manifest.native_sources[channel]`. Reading only the envelope loses the source label.
   Where the producer currently creates the label: `generator/generate_order_lifecycle_fixture.py:625`; where Bronze takes the
   fallback label after the fix: `spark_jobs/lib/order_lifecycle_lakehouse.py:71`.
2. Silver before package G guessed the Amazon schema from the payload shape and always parsed
   Shopee as v1; a field rename could silently produce nulls. After the fix, the parser selection point
   at `spark_jobs/lib/order_lifecycle_lakehouse.py:132` uses `contract_parser`, which the gate at
   `spark_jobs/lib/order_lifecycle_lakehouse.py:341` takes from the registry.
3. Schemas existed only in Spark code, with no independent data for a machine to compare two
   versions. The new registry is at `contracts/registry.json`, the comparison algorithm at
   `spark_jobs/lib/data_contracts/compat.py:1`. This risk already materialized in the v1 data:
   the description of the 81-day Amazon format change is at `docs/data-platform/reference/silver.md:463`.

The code references above describe the current locations after the fix; the three gaps are the state
before package G. The goal is to keep every source row in Bronze, move rows that fail the
contract to a quarantine table, and keep processing the Channels that still qualify.

## Design

| Component | Responsibility |
|---|---|
| `contracts/registry.json` | Subject, compatibility, version, schema, parser, status, native identity path |
| `contracts/schemas/` | A JSON Schema subset: type, properties, required, items; required is the intersection of the observed fields |
| `spark_jobs/lib/data_contracts/schema.py` | Infer, validate whole records in Python, build required JSON paths for Spark |
| `spark_jobs/lib/data_contracts/compat.py` | Diff and verdict by old/new reader direction |
| `spark_jobs/lib/data_contracts/registry.py` | Schema lookup, locating the contracts directory, metadata checks and parser-reuse rules |
| `spark_jobs/lib/data_contracts/gate.py`, `spark_jobs/lib/data_contracts/__main__.py` | Python gate and the infer/diff/check-registry/gate CLI, no Docker needed |
| `spark_jobs/lib/order_lifecycle_lakehouse.py` | Bronze labeling, the gate at the start of Silver using Spark expressions, parsing per the registry, reconciliation |
| Job `spark_jobs/silver/build_order_lifecycle.py` | Writes `silver.lifecycle_quarantine_records`, reconciles the tables read back from the catalog and emits a receipt |

The package lives in `spark_jobs/` so that both `import data_contracts` in the
container and `import spark_jobs.lib.data_contracts` from the repo work. Inside the package it uses
relative imports and only the standard library; there is no new service or dependency.

Bronze adds `contract_schema_id`: the label in the row takes priority, then the native feed label
in the manifest, including Walmart JDBC. Bronze does not filter on contract violations.
Silver looks up the schema, then splits accepted/quarantined **before** parsing; no
Python UDF is used. Amazon has two separate parsers. TikTok v2 adds an optional `buyer_message`,
registered with parser `tiktok_shop/1`, so the new version needs no code branch.
Shopee v2 renames `order_sn` and `model_sku` to `order_number` and `sku`; this version
is generated for the demo and is **not** in the official registry.

The registry contains the six initial native schemas, TikTok v2, nine operational feeds and two auxiliary
coverage/uncertainties feeds so the gate does not quarantine them by mistake. Amazon uses mode
`NONE` because its two exports already had separate parsers; the other subjects use `BACKWARD`.

The contracts lookup order shared by `Registry()` and the CLI commands that need the registry:
`--contracts-dir` → `PLATFORM_CONTRACTS_DIR` → `/opt/contracts` if the directory exists →
`contracts/` next to the repo root computed from the package location, independent of the working directory.
The job `spark_jobs/silver/build_order_lifecycle.py` passes the default `None` to use the same rule.

### Schema evolution rules

| Mode | Meaning | Breaking changes |
|---|---|---|
| `BACKWARD` | A new reader can read old data | Adding a required field, changing a type, optional to required |
| `FORWARD` | An old reader can read new data | Removing/renaming an old required field, changing a type, old required to optional |
| `FULL` | Both directions hold | The union of both directions' breaking changes |
| `NONE` | No compatibility check | Nothing is blocked on compatibility |

`migrations` only explains rename paths so the diff shows `field_renamed`;
it does not make a rename compatible. A compatible version may reuse a parser;
a breaking version must have a different parser or `null` (not yet supported). `check-registry`
returns a non-zero code if a breaking version reuses an earlier parser.

### Three quarantine reasons, in exactly this order

| Reason | Condition |
|---|---|
| `unregistered_schema` | Missing label or label not in the registry |
| `no_parser` | Schema exists but the parser is null or Silver has no branch for that parser yet |
| `contract_violation` | Parser exists but a required path is missing; `detail` records the path |

The Python gate also checks JSON types and every array element. The Spark gate checks the
required paths with `get_json_object(...).isNull()` and does not fully check types.
The quarantine table keeps `dataset_version, feed, producer, contract_schema_id, reason,
detail, source_file, record_sha256, raw_json`. The receipt has `quarantine.total`,
`by_reason`, `by_schema_id`; **quarantine counts do not fail the job**. A non-zero reconciliation invariant
still fails the job.

### Reconciliation and identifying quarantined records

`native_record_balance = native_records_total - parsed_native_records -
quarantined_native_records` must be 0 so that no row is silently lost.
`identity_path` is optional version metadata; if present it must be a string starting
with `$.`. Every registered native schema has its own path.
`_native_order_identity(frame, registry)` uses only the path for exactly that
`contract_schema_id` and does not guess the payload of an unknown schema.

If the order ID can be read, `quarantined_orders` counts **distinct order IDs** per Channel
and removes them from the expected OMS set. If the order ID is null, the whole scope
`(dataset_version, channel)` is excluded from native↔OMS reconciliation; the number of rows is
reported in `info.quarantined_native_records_without_order_identity`. This number is not
called a number of orders. Both DataFrames and the registry snapshot pass through the `silver` dict; job
21 keeps the snapshot when building the dict that rereads the tables, so the `reconcile` signature does not need to change.

The Shopee v2 case must report **3 quarantined records with unknown order ID**, not three orders.
The case where the producer keeps the Shopee v1 label but renames `order_sn` also cannot read the
order ID; the expectation is one record with unknown order ID, and the Shopee scope is excluded from
native↔OMS reconciliation. The other Channels must still reconcile cleanly. This is the **Spark test
expectation after the fix**, not a new run measurement.

## Remeasured Python results

Running `scripts/run/run_data_contracts_demo.sh`, without Docker:

| Fixture | Accepted | Quarantined | Reason |
|---|---:|---:|---|
| default | 550 | 0 | None |
| shopee=2 | 547 | 3 | `unregistered_schema` |
| tiktok_shop=2 | 550 | 0 | None |

`check-registry`: exit 0, no errors. Diff Shopee v1 → v2 under `BACKWARD`: exit 1,
listing two removed fields (`order_sn`, `model_sku`) and two added required fields
(`order_number`, `sku`) at the corresponding order/item paths.

The commands, verbatim stdout/stderr, exit codes, git HEAD and SHA-256 of the code/registry
are in the [Data contracts evidence](../../evidence/contracts-validation-20261018.json). The current script creates a timestamped receipt in `out/data-contracts-evidence/`; the historical evidence file is kept unchanged on reruns.

Targeted pytest `tests/unit/spark_jobs/lib/test_data_contracts.py tests/unit/generator/test_generate_order_lifecycle_fixture.py`: **23 passed**.
Whole repo at the time of the run: **214 passed, 2 failed**; both failures belong to
`tests/unit/spark_jobs/lib/test_maintenance_policy.py` from a package developed in parallel (missing config and a function
that did not exist yet at the time of the check). That package's files were not modified. The full output is in the
report (internal note, not in the repo).

## Spark — confirmed separately by the operator

The run before the `identity_path` fix was stopped by the operator because it was outdated (not used as evidence).

**After the fix (operator): `Ran 32 tests in 2749.990s — OK`** — the 27 existing Silver v2 cases + 5 contract cases, running
`tests/spark/check_order_lifecycle_silver.py` in `platform-airflow:2.10.5-spark3.5.3`, `--network none --read-only`,
`PYSPARK_SUBMIT_ARGS='--driver-memory 3g pyspark-shell'`. The long duration is because the machine was running many jobs in parallel (the old 27 cases
run alone took 258 s); a 1 GB driver once caused `UNABLE_TO_ACQUIRE_MEMORY` under the overlapping load, so it was raised to 3 GB.

The tests fixed for the operator to rerun are
`test_contract_unknown_shopee_keeps_other_channels`,
`test_contract_producer_lies_about_schema`,
`test_contract_registered_but_missing_parser`. The default and TikTok-compatible
tests and the 27 old cases are still in the harness; it is not claimed that they have passed
with this fix. Command for the operator:

```bash
docker run --rm --network none --read-only --tmpfs /tmp:rw,size=1g \
  --mount type=bind,src=$PWD,dst=/workspace,readonly --entrypoint python \
  platform-airflow:2.10.5-spark3.5.3 /workspace/tests/spark/check_order_lifecycle_silver.py
```

## Limitations and work left for package I

- Spark checks only the first array element `[0]`; do not infer from this gate that every item is
  valid. The Python validator checks every element; empty arrays and nullable fields
  are not treated as missing required fields just because there is no item/null.
- The registry is a file managed in Git; there is no registry service, discovery or
  automatic registration of new schemas yet. Inferring from a fixture reflects the observed sample and needs manual review.
- There is no dedicated DAG/DataHub for contracts/quarantine yet; left for package I. Existing DAGs
  are not changed in this package.
- When a record has an unknown order ID, excluding the whole Channel scope from native↔OMS reconciliation
  reduces the check's coverage; the receipt states the number of records so it is not misread as the
  Channel having been fully reconciled.
- Performance/large scale has not been measured, Spark has not been run after the fix, and no real Iceberg table
  has been written for the identity_path fix. The Python demo does not replace that evidence.
