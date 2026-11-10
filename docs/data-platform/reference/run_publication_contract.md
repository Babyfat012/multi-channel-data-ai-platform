# Run, publication and serving contract — small-scale implementation design

This is a technical design within Q1–Q61, approved by the user;
Q60 uses current non-canceled sales, Q61 adds Seller feeds with
provenance. It does not change the target `return-or-refund-line-v2`, product permissions or
Cloud scope. This protocol is not implemented yet and no POC has passed. The
ID/hash examples below are illustrative values, not runtime artifacts.

This document makes concrete ADR-0007,
readiness, the [label contract](return_label_contract.md)
and IR02/06/10/17/18/19/20/22/23/25 in the
risk register. Fixing resume/CDC/validator alone is not
enough to close these IRs. Link checks or JSON parsing are not taken as pipeline proof.

## 1. Invariants and local backend choice

Each build reads one frozen input vector; each rewrite uses a new attempt
namespace. A candidate may only be read publicly after the required outputs have been
sealed and reconciled and the activation pointer has been switched in one control transaction.
An activation links a compatible **data generation + serving bundle**;
they have separate lifecycles. A model's training dataset is immutable and does not need the same
ID as the new data generation that serves the model. One turn uses exactly one context.

PostgreSQL 15, already in [Compose](../../../docker-compose.yml), is chosen as the control
store, with schema `platform_control` in the application's own DB; the Lakekeeper catalog DB
or the Airflow DB are not used for business tables. The small setup can use the same
PostgreSQL server but roles and DB must be separate. No DB/schema is created here.
The existing object store is used to hold immutable JSON/report/artifacts. No new
orchestration service is added. This is a local implementation choice for testing; it does not claim
that availability, RPO/RTO or Cloud scalability have been proven.

| Store | Identifiers that must be pinned | Write/read rules |
|---|---|---|
| Iceberg | Catalog/table UUID + metadata location + snapshot ID + schema digest | Read `snapshot-id`; never read current and merely attach the ID to the response |
| File/object | Bucket/key + object version if any + SHA-256 + bytes | Exact file set in the manifest; never re-glob on retry |
| PostgreSQL outputs | Generation-specific schema/table or partition + schema/key/content digest | The loader writes a separate candidate; once sealed, runtime has SELECT only; no JDBC overwrite of a table being served |
| Feast/Redis | Generation-specific project/namespace + registry artifact digest + materialization receipt | Each generation has its own online namespace; never overwrite a pinned project |
| Model/RAG | Image digest, weights/preprocessor/embedding/corpus/index digests | Endpoint startup proves the artifacts actually loaded; alias/tag is not identity |

Sample output namespaces: `out/<epoch>/<run>/<attempt>/...`,
`pub_<generation>_<attempt>.sales_daily`, Feast project `platform_<generation>_<attempt>`; the manifest points to the selected attempt,
and two retries never write to the same project.
The POC must check that the pinned Feast version really separates Redis keys by project and registry;
isolation is not inferred from the project name in the config. If the adapter does not
support it, choose another physical namespace and record it in the manifest before acceptance;
never add a generation column to the entity in a way that changes the feature's meaning. The offline adapter
must read the exact Iceberg snapshot. If it cannot, export immutable Parquet from
that snapshot, then use an exact file manifest and reconcile parity; Iceberg remains the
root lineage, and changing the read path must be stated clearly in the Feast proof.

## 2. Immutable manifest and frozen input vector

The JSON envelope must have `schema_version`, identity, UTC timestamps, clock,
contract/config/code digests and references. Use canonical UTF-8 JSON per the project's
pinned convention, then SHA-256; never hash arbitrary pretty-printed output. Every real digest
must state its algorithm and have 64 hex digits; `h_*` below are placeholders.
Never put credentials, URLs with tokens or sensitive payloads in the manifest.

```json
{
  "schema_version": "platform.run-input.v1",
  "run_id": "r_018",
  "dataset_version": "d_world_07",
  "world_digest": "h_world",
  "clock_id": "sim_world_07",
  "knowledge_cutoff_exclusive": "2026-07-01T00:00:00Z",
  "business_window": {"start": "2026-01-02T00:00:00Z", "end": "2026-07-01T00:00:00Z"},
  "view_mode": "as_known",
  "source_profile_digest": "h_sources",
  "output_profile_digest": "h_profile",
  "code_tree_digest": "h_code",
  "dependency_lock_digest": "h_lock",
  "config_digest": "h_config",
  "metric_contract_digest": "h_current_noncanceled",
  "label_contract": "return-or-refund-line-v2",
  "label_contract_digest": "h_label_v2",
  "inputs": [
    {
      "role": "amazon_orders_bronze",
      "kind": "iceberg",
      "catalog": "platform",
      "table": "bronze.amazon_orders",
      "table_uuid": "uuid_a",
      "snapshot_id": "81234001",
      "metadata_location": "s3://lake/metadata/00017.json",
      "schema_digest": "h_schema_a",
      "dataset_manifest_digest": "h_source_manifest",
      "coverage_receipt_digest": "h_orders_coverage"
    },
    {
      "role": "seller_delivery",
      "kind": "file_set",
      "manifest_uri": "s3://raw/d_world_07/delivery/manifest.json",
      "manifest_digest": "h_delivery_files",
      "producer": "simulated_seller_wms",
      "authority": "actual_delivery",
      "coverage_receipt_digest": "h_delivery_coverage"
    }
  ],
  "resolved_at_wall": "2026-10-18T08:00:00Z"
}
```

These are only two entries to illustrate the shape. The resolver must expand **every**
feed/Channel/dependency according to the source profile: Orders for the five Channels, mappings,
and the Listing/inventory/lifecycle/refund/coverage that are used. One entry is not enough for
all of Q61. A coverage receipt includes the Channel/account/pool scope, the covered event range,
observation cutoff, producer/authority, missing/conflicts/linkage,
completeness definition and upstream evidence. `unsupported` differs from `empty`.

The resolver runs once when the run is created; the build token is issued only after the manifest is written.
It may resolve snapshots that do not share a commit timestamp, but it must check the same
world/dataset, cutoff and source coverage. If Bronze contains observations after the cutoff,
the transform still filters `source_observed_at < cutoff`. An exact snapshot does not by itself
prove the known-time is correct. A mutable JDBC source must be exported under one
consistent snapshot into immutable staging before the manifest is closed; parallel
JDBC connections do not share a DB snapshot by default. A CDC entry needs the source
incarnation, topic ID/partition set and concrete start-inclusive/end-exclusive offsets
from the CDC protocol; never substitute event time or the keyword `latest`.

A task receives `--input-manifest <uri> --input-digest <sha256> --attempt-id ...`.
The DAG does not re-read Airflow Variables to resolve sources/config between tasks.
The validator receives that manifest plus the exact output receipts. When an upstream creates
new output for a downstream in the same run, add a new stage manifest with a parent digest
and the exact snapshot of the previous stage; never edit the original input manifest. A retry uses
the same manifest, a different attempt and a different namespace. A missing snapshot/file or a digest
mismatch fails with `INPUT_UNAVAILABLE`/`INPUT_CONFLICT`, with no fallback to latest.

The DAGs [DP2](../../../dags/dp2_silver.py), [DP3](../../../dags/dp3_features.py) and
[label bridge](../../../dags/dp3_return_labels.py) currently pass table names/run IDs;
they do not have this interface yet. Each DAG's `max_active_runs=1` does not create a multi-table
snapshot or a lock across DAGs. Iceberg supports reading a snapshot of each table separately,
but does not create cross-table snapshots by itself.
[Iceberg 1.7.2 source](https://iceberg.apache.org/docs/1.7.2/spark-queries/).

## 3. Output profile locked before the build

A profile has an explicit scope and capabilities and is hashed into the input manifest.
A candidate is not allowed to drop an output after seeing an error. Each scope has its own required
outputs; optional outputs never cause the main figures to be lost or turn missing into0.

```json
{
  "schema_version": "platform.output-profile.v1",
  "profile_id": "seller-demo-v1",
  "scope": {"channels": ["amazon", "walmart", "tiktok_shop", "shopify", "shopee"]},
  "required": ["sales_daily_pg", "listing_inventory_pg", "coverage_pg"],
  "capability_requirements": [
    {"capability": "return_label_v2", "scope_manifest_digest": "h_label_scope", "outputs": ["label_history_offline", "features_offline", "features_online"]},
    {"capability": "sku_risk_queue", "scope_manifest_digest": "h_risk_scope", "outputs": ["risk_queue_pg", "risk_model_compatibility"]}
  ],
  "optional": ["customer_repeat_supported_scope"],
  "validation_suite_digest": "h_expected_suite",
  "coverage_policy_digest": "h_coverage_policy"
}
```

Scope manifests list each namespace that has or lacks a capability, with the reason; they
are settled from G0, and never claim that labels/models cover five Channels if in practice only
part of them qualify. The Q61 Seller feed has its own authority/bridge and does not give every pool
the same coverage. If a required capability in a declared scope loses a
feed/receipt, the candidate fails and the previous good activation is kept. Scopes outside the
capability show `unsupported`/`unknown`; rows are never silently cut. A missing optional
output must have an `unavailable` receipt, and consumers must not infer0. Changing a requirement
or scope creates a new profile revision, with audit and verification under the corresponding contract.

Each output receipt must have role/status, input/stage digest, physical identity,
key/schema/content digest, logical row count, quantity/amount per currency,
coverage, freshness, validation report digest and seal evidence. Valid zero rows
must be proven by input/coverage; zero rows do not stand in for missing.
The content digest uses the pinned key order + canonical value encoding (including null,
decimal/time precision), shard digests + root; a count alone is not a sufficient oracle.
The validator compares logical rows/key/value at the exact snapshot against an independent fixture/source
oracle, and never uses physical file counts as the business row count.

## 4. Label revisions, training and the correct clock

Store history append-only by full line identity + target version; never overwrite
0 with1 and lose the ability to query earlier knowledge. Minimum row revision:

```json
{
  "line_key": ["d_world_07", "amazon", "shop_1", "order_10", "line_2"],
  "label_contract": "return-or-refund-line-v2",
  "revision_id": "lr_003",
  "revision_sequence": 3,
  "known_at": "2026-06-15T09:00:00Z",
  "state": "immature",
  "label": null,
  "maturity_end": "2026-07-01T12:00:00Z",
  "cohort_date": "2026-06-01",
  "reason": "delivery_correction",
  "source_refs_digest": "h_corrected_delivery_refs",
  "coverage_receipt_digest": "h_coverage_c3",
  "replaces_revision": "lr_002",
  "computed_at_wall": "2026-10-18T09:00:00Z"
}
```

`known_at` is the moment there is enough source/coverage/maturity evidence on the declared business
clock; it is never simply the delivery time or the replay run time.
If maturity has only passed in wall-clock time without a completeness proof,
no official label0 is created. For an exclusive cutoff C, choose the latest revision
with `known_at < C`; a tie on the same known_at must use the source revision/order defined by the
contract or report a conflict, never an arbitrary Spark ordering. The derived validity range
of a revision at t before a revision at t2 is **t < C <= t2**, keeping the label contract's
exclusive boundary. A derived `next_known_at` may be stored, without editing the
content of the old revision. Add state changes for maturity/coverage even when there is no
new positive event. Historical queries read a source snapshot that contains the necessary revisions,
and filter at the correct cutoff; never query the current table and reconstruct the old label.

Required fixture: C1 knows0, C2 a late event makes it1, C3 a delivery correction makes it
immature; the training dataset locked at C1 keeps its old checksum. An unmatched Refund keeps the affected
scope before eligibility; uncertainty is never lost through an inner join. Maturity,
positive/negative and boundaries still follow L01–L17; no new target is defined.

The training manifest must be separate from the publication manifest:

```json
{
  "schema_version": "platform.training-dataset.v1",
  "training_dataset_id": "td_fold_03",
  "world_digest": "h_world",
  "clock_id": "sim_world_07",
  "feature_definition_digest": "h_feature_v3",
  "feature_schema_digest": "h_ordered_units_defaults",
  "label_contract": "return-or-refund-line-v2",
  "label_contract_digest": "h_label_v2",
  "training_information_cutoff_exclusive": "2026-04-01T00:00:00Z",
  "sample_manifest_digest": "h_sample_keys_feature_cutoffs_label_revisions",
  "input_manifest_digests": ["h_training_inputs"],
  "eligibility_and_split_digest": "h_fold_03",
  "created_at_wall": "2026-10-18T09:30:00Z"
}
```

Each sample stores the prediction/order time, its own feature cutoff, the label revision
selected before the training information cutoff, eligibility/coverage and the fold ID. A future
outcome after the feature cutoff can be used as a label **if it was known before the training
cutoff**; the label does not have to be known when the Order was placed. All preprocessing,
feature selection, calibration, thresholds and tuning may only be fit on data
permitted at the cutoff; the future test set is only opened after those choices are locked.

Forward-at-order evaluation needs the same `clock_id` and time ordering:

```text
all training/tuning information observed < training_information_cutoff
training_information_cutoff <= model_ready_at <= sample_prediction_at
feature information observed < sample_feature_cutoff (not after prediction)
outcome_evaluation_cutoff > maturity + full coverage of the sample
```

A model trained up to D60 is not valid for predicting an Order on D10 even if delivered on D65; this case must
be excluded from the forward-at-order proof. Folds use prediction time, not
just delivery date. For a sample whose timestamp equals the cutoff, keep the strict source
boundary from the contract; it never becomes inclusive by itself. A temporal holdout must report the number of samples
excluded, immature/unknown, coverage scope, baseline and target version.

Separate two evidence modes: `actual_deployment` uses real availability/ready
in wall-clock time; `simulated_replay` uses the simulated clock and a declared simulated
ready time + training latency policy locked before the fold. Replay still records actual
artifact creation/training timestamps; never backdate them into evidence of a
past deployment. If model-ready has not been modeled on the simulated clock,
only call it a retrospective evaluation, not sufficient forward-at-order proof. Never
compare a wall timestamp of 2026-10 with a simulated date of 2026-04 as if on the same clock.

## 5. Data generation, serving bundle and activation

An immutable generation records the frozen input digest, output profile digest, output
receipts, cutoff/view/capability, validation report and wall timestamps.
`generation_id` is a UUID or a new opaque ID; string ordering of IDs is never used to measure
recency. An independent bundle pins the model/RAG/tool artifacts:

```json
{
  "schema_version": "platform.serving-bundle.v1",
  "bundle_id": "b_07",
  "model": {
    "image_digest": "h_image",
    "weights_digest": "h_weights",
    "preprocessor_digest": "h_preprocessor",
    "training_dataset_digest": "h_td_fold_03",
    "feature_definition_digest": "h_feature_v3",
    "feature_schema_digest": "h_ordered_units_defaults",
    "label_contract": "return-or-refund-line-v2",
    "label_contract_digest": "h_label_v2",
    "score_semantics_digest": "h_probability_or_uncalibrated_contract"
  },
  "agent": {
    "prompt_digest": "h_prompt",
    "tool_schema_digest": "h_tools",
    "permission_policy_digest": "h_permissions",
    "corpus_digest": "h_corpus",
    "embedding_digest": "h_embedding",
    "index_digest": "h_index"
  },
  "compatibility_policy_digest": "h_compatibility"
}
```

Compatibility is a checked contract comparison: ordered feature names, dtype,
units, categorical vocab/default/missing policy, definition digest, label
policy, output semantics and capability; not just the column count. A generation does not
need the same training dataset ID as the model. A new bundle can use an old generation
if compatible; new data does not force a retrain. The v1 model/old proof keeps target
v1; its metadata is never changed to v2 to share an endpoint name.

Endpoint startup checks the image actually running and the weights/preprocessor actually loaded, and returns the
observed bundle digest. The gateway only routes to endpoints attested for the correct bundle;
rolling pods with a different bundle get their own route. The A/B arm is chosen once per context,
not re-chosen between tool calls; each arm must be compatible with the same target/scope.
A rollback activation selects a verified generation/bundle pair, with a clear rollback audit;
never automatically route a mutable `production` alias. Inference results echo the observed
model/bundle digest, never copying the desired name from the gateway.

## 6. Control tables, fencing and atomic publish

Minimum logical tables, implemented in a separate migration during the POC:

| Table | Main keys/constraints |
|---|---|
| `environment` | singleton/environment ID, recovery epoch UUID, mode recovering/ready, policy digest |
| `publication_lane` | lane, epoch, next fence, lease owner/token/expiry; desired release sequence number issued by the scheduler |
| `run`/`attempt` | immutable run input digest; attempt UUID + epoch + fence + status + namespace |
| `artifact`/`artifact_reference` | identity + digest + namespace + ready/deleting/deleted; refs to parent/child artifacts |
| `generation`/`bundle` | IDs + content digests; insert-only content, lifecycle state kept separately |
| `activation` | append-only activation UUID, generation digest, bundle digest, intent sequence, publisher epoch/fence, reason |
| `active_pointer` | lane + epoch + pointer version + activation ID |
| `reader_lease` | lease ID + activation/dependency references + epoch + expiry + subject/scope hash |
| `publication_event` | idempotency key + candidate digest + outcome + pointer version; unique key |

Control writes go only through services/procedures; loaders/agents have no direct UPDATE permission
on pointer/epoch. Use the DB clock for leases; the process clock never decides
who still holds a lease. Fixed lock order: environment → lane/pointer → artifact
refs/lease, to avoid deadlocks. PostgreSQL row locks last until transaction end;
`SELECT ... FOR UPDATE` prevents other writers from changing the same row but does not by itself protect
Redis/Iceberg. [PostgreSQL 15 locking](https://www.postgresql.org/docs/15/explicit-locking.html).

Protocol:

1. The scheduler issues the intent sequence and input manifest; it acquires the lane lease in a
   transaction, incrementing the fence on every grant/reacquire. The worker receives `(epoch,fence,
   owner,expiry)`; renewal is conditional on the correct token and epoch and on not being expired.
2. The attempt writes its own namespace. An old worker must not share a new attempt's
   namespace. Before sealing: finish every producer transaction/job; revoke loader
   credentials, stop/close writing sessions, lock modification rights on the namespace. For PG,
   the service owner holds the schema and the loader is not the owner; revoke is not enough if an
   old transaction is still running. For Redis, generation-specific credentials/prefix,
   verify the revoke/disconnect actually happened; an application-level lease flag is not trusted as sufficient.
   If credential isolation/revocation or producer shutdown cannot be proven
   on the pinned backend, do not seal/publish, and do not substitute a sleep-and-wait.
3. Seal and validate the exact output identities/digests. Only a candidate that has completed
   producer shutdown + seal evidence becomes `VALIDATED`. A retry holding an old token
   cannot write/publish into an accepted namespace; a credential and
   storage enforcement POC is needed to prove this, not only app-side checks.
4. The publisher transaction locks environment/lane/pointer; it checks that the environment is ready,
   epoch/fence/owner/lease are still valid by the DB clock, required outputs/profile and the
   dependency closure are all sealed/validated/not-deleting, bundle compatibility,
   the expected pointer version and that the intent is still valid. No remote sink is called while
   holding a long DB transaction; remote receipts have already been checked and the namespace sealed.
5. Re-check expiry with `clock_timestamp()` right before the CAS in the transaction
   that holds the lane lock; never use the fixed timestamp from transaction start.
   Insert activation/event, switch the pointer with CAS `WHERE epoch=:e AND
   pointer_version=:expected`; increment the pointer version, commit in the same transaction.
   The CAS must change exactly one row. Wrong epoch/token/expiry/intention/CAS: roll back,
   never automatically retry with the latest pointer to override a newer publication.
6. Lost ACK: look up the unique publication idempotency key. The same committed candidate
   returns the old result; the same key with a different digest is a conflict. If the activation was replaced
   afterwards, return the historical outcome; never republish and pull the pointer back.

This pseudocode describes the service transaction, not a runnable migration:

```sql
BEGIN;
-- lock + check environment, lane lease, dependencies, compatibility first
SELECT * FROM platform_control.active_pointer WHERE lane = :lane FOR UPDATE;
-- application checks expected version, intent and exact candidate digest
INSERT INTO platform_control.activation (...) VALUES (...);
UPDATE platform_control.active_pointer
SET activation_id = :activation, pointer_version = pointer_version + 1
WHERE lane = :lane AND epoch = :epoch AND pointer_version = :expected;
-- assert affected_rows = 1; insert event with UNIQUE idempotency key
COMMIT;
```

CAS alone does not stop an old candidate from acquiring a new token and then publishing over newer
data. The scheduler keeps a separate desired intent sequence: a candidate may only be published
if it has not been superseded and its cutoff does not move backwards, except for an **explicit rollback intent** whose
dependencies/compatibility have been checked. Each release manifest records action/reason, without
editing old generations. Keep the publication intent and the active pointer version separate.

If a required sink fails, the old pointer stays unchanged and freshness is shown correctly; if
the store of the activation being read is actually lost, the request fails as unavailable, never reading another
latest store. A profile can return a partial capability published in advance,
but requirements are never lowered in failure handling. The atomic pointer only guarantees the
visibility boundary; it does not fix wrong input or wrong output.

## 7. Turns, pagination, cache and historical routes

The gateway/server creates the context after auth and does not accept a scope/cutoff the LLM
widens on its own. Resolve the pointer and create the reader lease/dependency refs in **the same
control-locking transaction**; if GC has already marked it deleting, the pin fails. Context:

```json
{
  "context_id": "ctx_1",
  "epoch": "recovery_uuid_9",
  "activation_id": "a_12",
  "generation_digest": "h_g12",
  "bundle_digest": "h_b07",
  "lease_id": "lease_1",
  "view_mode": "as_known",
  "business_window": ["2026-06-01T00:00:00Z", "2026-07-01T00:00:00Z"],
  "knowledge_cutoff_exclusive": "2026-07-01T00:00:00Z",
  "scope_hash": "h_authorized_account_storefront_sku_currency_filters",
  "metric_contract_digest": "h_current_noncanceled",
  "deadline_wall": "2026-10-18T10:01:00Z",
  "budget_id": "budget_ctx_1"
}
```

The coordinator passes a server-signed context handle to agents/tools/model/RAG;
the LLM only chooses tools/arguments within the permitted schema and scope. Tool results
echo context/generation/bundle/view/cutoff/coverage. A mismatch makes the whole branch
abstain/retry with the same context, never merging evidence from different versions. A new turn resolves
the new activation and revalidates old evidence; conversation history is not a source of current
figures. Comparing two generations is an explicit comparison with two pinned contexts
and a result that keeps each side separate, never summing them.

A signed/MAC'd cursor contains context, scope/query digest, sort keys + unique tie-break,
last key, expiry, auth scope digest. The next page and drilldown keep the same context;
an expired lease must return `CONTEXT_EXPIRED` and restart with a clear indication, never
falling back to current. Leases are not held indefinitely across every browser page; the server
renews within the configured total context lifetime, and an expired cursor does not revive a
lease that has been GC'd. Token expiry must not exceed the time the server actually holds the pin.

The cache key includes the canonical full query + auth scope + epoch + activation,
generation/bundle/metric/tool schema digests + view/knowledge cutoff/business
window/currency/filter + A/B arm. A cache hit still checks the current lease/context/authorization;
a cached answer never exceeds permissions that have just been revoked. When there is a data sensitivity
policy, redact before caching/logging. Never use a single SKU as the cache key.

The API separates business window from knowledge cutoff and view:

- `official`: reads the published generation per the materialized scope/window.
- `as_known`: reads historical source/history pinned enough to know only what was known before C.
- `restated`: the same business window but using corrections known at an explicitly stated cutoff C2,
  never calling that information known at cutoff C1.
- `live`: its own identity/freshness, never implicitly joined into the daily official answer.

Online Feast/Redis only serves the generation/cutoff actually materialized. A historical
request needs an offline route to an exact snapshot or a retained historical generation,
a bounded query and its own manifest; if not materialized or past retention, return
`HISTORY_UNAVAILABLE`/`UNSUPPORTED_VIEW`, never sending a version parameter down to the latest
online lookup. Every route must check the capability/profile against what the query requires.

## 8. Retention, GC and clean epoch recovery

Maintain a dependency graph for generations, model training datasets, bundles,
validation/evidence and reader leases. Long-lived refs used for models/evidence still
protected by the retention policy; reader refs have expiry/renewal. Never protect only the
active pointer and then delete training snapshots that an old model still needs to reproduce.

Two-step GC: in a transaction that locks artifact/control, confirm there is no longer any
active/retained dependency or valid lease and mark `DELETING`; from then on new
leases/refs are forbidden. Then delete each sink with idempotency/tombstone, record a receipt, then
`DELETED`. A crash between steps can be retried; a missing artifact never returns to READY.
A lease renewal racing GC must use the same lock/state check: a renewal after deleting fails,
and a request that is losing its lease must stop before using more data. Never automatically resolve
latest or accept a late backend result after the context has expired.

Iceberg expiry/orphan cleanup only runs from a GC allowlist whose dependency
closure has been computed; snapshot/tag refs appropriate to the catalog/runtime version must be kept. Orphan cleanup
must protect files of running writers and use the policy age threshold, never sweeping
uncommitted candidates as garbage. Redis expiry must not cut short a pinned
generation; if the adapter TTL can expire features before retention, it must
adjust/rebuild/validate or mark the namespace unavailable. Offline TTL, online
TTL and API staleness are three separate constraints.
[Iceberg maintenance source](https://iceberg.apache.org/docs/1.7.2/maintenance/).

Local recovery procedure before G4:

1. Close serving/write ingress and stop the scheduler/workers; revoke/rotate old writer
   and service credentials, terminate connections. Do not merely set ready=false
   in the DB copy about to be restored. Bootstrap outside the backup is driven by the operator/local
   script; old processes are not allowed to bootstrap an epoch themselves.
2. Restore the backup set/catalog/PG/artifacts. Generate a **new epoch UUID after restore**,
   different from the backup epoch; rotate context signing credentials so old tokens become
   invalid. Fence counters may be restored to lower values but the new epoch is still different.
3. Check the exact dependency closure of the activation intended to be served, including
   content/schema/coverage/compatibility and endpoint attestation. Redis can be
   rebuilt from the exact offline generation and then validated; an old marker is never treated as
   evidence that the materialization still exists. A rebuild that changes the physical namespace needs a
   new receipt/deployment activation referencing the old generation content digest;
   never edit an immutable receipt or reuse an ID name for different content.
4. New writers only receive new-epoch credentials/tokens. Reset old leases, check GC and
   orphan candidates; never revive reader tokens from the backup. When every required
   dependency is valid, switch the environment to ready in a control transaction and open
   ingress. If there is no valid activation, stay unavailable; a rollback to a version
   that can be kept must be an explicit recovery activation, never called the latest version.

Process liveness differs from readiness. Readiness needs the current epoch, a reachable
control store, verified required namespaces/receipts, an attested model bundle, scope
coverage and freshness per policy. A Redis failure only makes unavailable the capability
that depends on it, if the output profile allows partial serving; never return0.
RPO/RTO, backup cadence, TTL/lease/sweep intervals and retention durations must
be configured/benchmarked before G4; they are not considered met from this design alone.

## 9. Budgets and multi-hop execution

A request has a total work budget issued by the gateway, using an atomic server-side ledger
per context. Agents may not grant themselves new budget; every nested call/
retry fan-out must reserve from the same ledger before dispatch and reconcile actual usage
after the call; when past the deadline or out of quota, nothing more is dispatched. The budget is not just an
HTTP count: it limits total tool attempts, query time, rows scanned/returned,
bytes, model tokens, concurrent tasks and total deadline. SQL LIMIT only caps
rows returned, not rows scanned; reject unbounded window/tenant/filter before the
query, use indexed/materialized views, statement timeout and a query layout
benchmark. If the engine has no hard byte-scan quota, state clearly that the limit is
estimated + time/concurrency, never claiming hard byte enforcement.

Demo policy to start the POC, not a measured SLA/acceptance threshold:
`answer deadline 45s`, `tool attempts total 8` including retries, `concurrency 2`,
`SQL statement 5s`, `returned rows/tool 500`, `response bytes/tool 1 MiB`,
`metric window <=180 days`; token/corpus/query-scan caps are required config
that must be settled per model/engine version before the corresponding POC. These defaults never
replace a CPU benchmark or Cloud budget; the policy digest lives in the bundle/input.
Retry only transient errors while budget/deadline remain; invalid scope, compatibility,
expired context, missing history or unknown coverage are never retried against latest.

Roles separate publisher, source loader, serving query and model runtime. A read-only
agent only calls tool templates/allowlisted views per account; no raw SQL,
control-plane tables, arbitrary URLs, filesystem or secret endpoints. The serving
role owns no tables, has no CREATE/WRITE and cannot bypass the scope filter;
service auth verifies audience/subject/authorization at each boundary, never
passing arbitrary tokens through. Read-only credentials still need query bounds.

Cancellation propagates gateway → coordinator → child tool → DB/model handle;
the DB has a query ID so it can actually cancel, not just stop awaiting. Spark/historical jobs
have a job group/cancel and a runtime cap; queued tasks keep the shared deadline. A backend
that does not support cancel must isolate the worker with a hard timeout/process limit before
exposing the endpoint. A late response must not be emitted into an answer that has timed out. Lease cleanup
happens after cancel/finalize; the trace records attempt/query IDs and actual
work/token usage, redacting data/credentials. PostgreSQL has `statement_timeout`
and `lock_timeout`; the app must still attach them to the query/session in the correct scope.
[PostgreSQL 15 source](https://www.postgresql.org/docs/15/runtime-config-client.html).

## 10. POCs to pass and IR closing conditions

Each probe stores input/output digests, an independent expected fixture, environment/deps,
trace/actual work, cleanup state and result. None of the probes below has passed yet.

| IR | Required probe | Gate / closing condition |
|---|---|---|
| IR02 | Replace Bronze/edit Variables between tasks and retry; upstream snapshot expires | G1/G2: build/validate the exact manifest or fail clearly, never mixing inputs |
| IR06 | 0→1→immature at three cutoffs, same-time source conflict, maturity boundary | G2: correct historical selection, old training dataset digest unchanged |
| IR10 | Same business window with as-known/restated; request for history not yet materialized | G2/G3: correct result for the cutoff or unsupported, no latest fallback |
| IR17 | Publish between tools/pages, retry/cache with different scope/currency, expired pin | G3: one turn/context on the correct version; expiry ends clearly; no permission leak |
| IR18 | Wrong weights, reordered units/schema, mixed rolling pods, wrong target | G2/G3: blocked before inference; trace of the actual artifact; rollback to the correct bundle |
| IR19 | Restore PG/catalog/Redis at mismatched points in time, old writer comes back, old cached token | G4: real epoch/credential fence, dependencies verified before ready |
| IR20 | Required output missing; optional unavailable; five-Channel capability subset | G2/G3: profile not changed to pass, no unknown→0, scope reported correctly |
| IR22 | Rule/config/dependency/prompt changes between build/validate/serve; mutable alias | G0–G4: digest mismatch blocked or creates a new revision/activation |
| IR23 | Order D10, train info D60, delivery D65; replay vs actual timestamps | G2: that sample not counted in the forward-at-order proof; fold clocks clear |
| IR25 | Prompt injection/URL, huge scan, nested retry, concurrency/cancel | G3/G4: correct role/scope, shared budgets enforced, backend work actually stops/bounded |

The publication POC must also force two writers/CAS conflict/lost ACK, stale intent,
a worker that lost its lease but is still writing, a pin racing GC, Redis TTL and seal enforcement,
Iceberg reference retention, compatibility and parity on exactly the pinned
Feast/Spark/catalog versions. The table above maps onto the existing S/G items and does not claim
an additional number of implemented scenarios. A failed result must keep the issue open,
not close it because this document exists. In particular, new sources/schemas/capabilities,
physical backend sizing, dependency pins, hard query-scan/token caps and G4
retention/RPO/RTO still need evidence before full scale.
