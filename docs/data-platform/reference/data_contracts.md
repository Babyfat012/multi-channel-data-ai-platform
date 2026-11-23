# Data contracts — schema registry and the dp1 contract check

The registry lives in Git: [`contracts/registry.json`](../../../contracts/registry.json) plus one JSON Schema per shape.
CI runs `python -m spark_jobs.lib.data_contracts check-registry`; the same CLI has `infer` and `diff`.
The order-lifecycle (v2) contracts, quarantine gate and their evidence were removed with that pipeline
(archived in git tag `v2-archive`).

## Pipeline A (dp1 → dp2): `sim-native-v1`

Pipeline A checks contracts on Bronze (diagram: nightly lakehouse, first step "Check contracts"). The subjects are
`sim-native-v1/{amazon,walmart,shopee,shopify,tiktok_shop}` with parsers `snapshots/*`; the row check is
`contract_reasons` in [`snapshot_contracts.py`](../../../spark_jobs/lib/snapshot_contracts.py).

| Decision | Choice and why |
|---|---|
| What is required | Only what the Silver job needs for a correct row (keys, times, status, currency, SKU, quantity, price, discount). Fields Silver reads but tolerates as NULL are typed, not required. Fields Silver never reads are not in the contract, even if every sample row has them (`infer_schema` would wrongly require e.g. `IsBusinessOrder`). |
| Where | In the dp1 validate tasks, on Bronze, inside the aggregation they already run: no extra pass over the data. Bronze keeps the raw rows as evidence; dp2 Silver runs only after dp1 succeeds. |
| On a violation | The task fails with the count and up to 5 distinct reasons (`reason shape missing-path`). No quarantine: dp2 Silver re-reads all of Bronze. |
| Amazon's two shapes | `amazon/1` (PascalCase) and `amazon/2` (2026 camelCase), told apart by the root id as Silver's `split_by_form` does. A row with neither is `unregistered_schema`; before, Silver dropped it silently. |

Checks: every row for registered shape + required paths (Spark expressions, arrays: first item);
types and every array item with `validate_record` on the first 1,000 rows of each shape (a wrong type appearing only
later is missed; Silver's cast would then give NULL). All 12,900 local sample rows (`out/channels-small`) pass; tests:
[`test_snapshot_contracts_v1.py`](../../../tests/unit/spark_jobs/lib/test_snapshot_contracts_v1.py) (removing any
required key must fail) and [`check_snapshot_contracts.py`](../../../tests/spark/check_snapshot_contracts.py) on a real
local Spark 3.5.3. Not run on AWS yet.

## Schema evolution rules

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
