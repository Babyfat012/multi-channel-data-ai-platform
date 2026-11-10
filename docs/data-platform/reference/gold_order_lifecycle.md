# Gold for source v2 — package E (small fixture)

> **Pipeline v2 (order lifecycle) — designed and tested locally; not the deployed pipeline.** The pipeline running on AWS is v1 (DP1 → DP4 + ML), see [Data Platform evidence](../evidence.md). Mentions of GCP below are an old plan that never ran; the current cloud target is [AWS](../../shared/aws_deploy.md).

The new Gold uses the `build_gold` namespace and **reads only Silver v2** (the first version also read Bronze). It uses no ground truth, probes or v1 labels. The single policy lives in
[`spark_jobs/lib/return_label.py`](../../../spark_jobs/lib/return_label.py), following
the [Return v2 contract](return_label_contract.md) and ADR-0010.
This is a small fixture, not yet evidence of a 100 GB run.

## Design

| Table | Grain | Contents |
|---|---|---|
| `fact_order_lines` | dataset × Order Line | Full Channel/account/Order/line identity, native seller_sku, created_at, ordered/cancelled/delivered quantities, headline and product refund success kept separate, currency; money as minor-unit bigint |
| `label_return` | dataset × Order Line × cutoff × generation | state, nullable label, cohort_date, maturity_end, qualifying_event_id, `positive_kind` (return_and_refund / refund_only / return_only when label = 1 — see [Return ≠ Refund](return_label_contract.md#return--refund--three-cases-and-how-the-label-handles-them-settled)), reasons, label_version, cutoff, generation_id, computed_at; evidence includes source_event_refs, decision_observed_at and an immutable input_json |
| `feat_sku_return_rate_90d` (table name unchanged; column renamed to `return_or_refund_rate_90d`) | dataset × Order Line | event_timestamp = created_at, created = computation time, labeled_count_90d, positive_count_90d, return_or_refund_rate_90d, label_version, generation_id |
| `label_segments` | dataset × Order Line × cutoff segment | Each segment `(start_exclusive, end_inclusive]` over which the line is in state `labeled`: label, cohort_date, maturity_end, decision_observed_at. The PIT feature reads this table |
| `label_inputs` | dataset × Order Line | Observed, normalized input; used for Python/Spark reconciliation |

Facts and features are partitioned by `days(created_at)`. Labels are appended per generation; rerunning the same
`generation_id` is not allowed; the current serving tables are replaced per run.
Old label snapshots stay unchanged together with their input_json and are not modified by new corrections.
Only a receipt with status passed is evidence of completion; writing several tables is not yet
a single transaction.

## Labels and evidence

The label rule is not rewritten as Spark expressions. Spark groups observations by
line, then an RDD calls `label_line(line, cutoff)`; pandas is not needed. The Python function returns
a `LabelResult` with policy ID `return-or-refund-line-v2`.

The window is per allocation, inclusive bounds, UTC, exclusive cutoff; terminal fulfillment,
maturity and coverage are all required for both label0/1. A correction takes effect only from
the time it is observed; a business cancellation/reversal keeps the historical event. See the contract for details
and the official definition; do not use the last delivery date in place of each allocation's own
window.

**Read only Silver, group evidence by key (refactor).** The first version also read
`bronze.lifecycle_raw_records`, sent **all** of the dataset's evidence to **each** line and recomputed the label for every pair
(historical line × order placement time) — correct on the fixture but quadratic, unable to handle a large source (OOM even on
the small scale source). The current version:

- Silver keeps history: `lifecycle_stg_observed_evidence` (every revision, dropping only exact duplicate retries, with keys `ev_line_id`,
  `ev_order_id`, `corrects_source_event_id`) and `lifecycle_stg_coverage_validity` (digest/count/sequence/partition/interval
  of each coverage revision, checked **once**).
- Gold groups evidence by key: by line, by Order (scope with no known line), corrections of records belonging to the line, and the few
  keyless records sent once to the workers; coverage is filtered by the line's interval before input_json. `spark_jobs.lib.return_label.visible_events` still filters exactly.
- The PIT feature uses `spark_jobs.lib.return_label.label_segments`: a label changes only at the observation times of the line itself or when it passes
  maturity ⇒ each line is scored O(number of records) times; per-SKU counts use a sweep over the half-open window `[a, b)`.
- Equivalence evidence: `tests/unit/spark_jobs/lib/test_return_label_segments.py` (compares `label_line` at every boundary ±1µs) and
  `tests/spark/check_order_lifecycle_gold_equivalence.py` (fact/label/feature **identical** to the old version — snapshot at
  `tests/spark/reference_order_lifecycle_gold.py`). The diff is 0 for fact/label/feature on the fixture and on the 60-day scale source, both before and after the rename (196 s). The old reference version runs out of memory on the 90-day source, so the comparison stops at 60 days; non-zero features are checked separately with `tests/spark/check_order_lifecycle_weekly_gold.py`.
- The job `spark_jobs/gold/build_order_lifecycle.py` runs only when `silver.lifecycle_stg_build_info.as_of` = cutoff (written by the job `spark_jobs/silver/build_order_lifecycle.py`) and there is no observed evidence
  ≥ cutoff; otherwise it stops before writing.

The validator replaces per-pair lineage (too large at real scale) with: `pit_segment_before_known` (a segment must not open before
the decision is known and maturity has passed) and `pit_feature_mismatch` — recomputes the feature for a sample of 50 lines with `label_line`
at the exact order placement time over every line of the same SKU, without the segment/sweep code.

## Keyless records and memory

Records not tied to a line/Order (keyless, mostly coverage) apply to every line.
Replicating them in Spark once caused OOM even on the small source: 3,631 lines ×
15 records × about 43 KB ≈ 2.3 GB; `source_sequence_ranges` alone can reach
313 KB/record. So this memory cost cannot be treated as linear in the number of lines only.

`keyless_evidence` collects this set together with related corrections to the driver once;
`_slim` drops the fields in `VERIFICATION_ONLY_FIELDS` (`source_sequence_ranges`),
then sends the slimmed set to the workers via a closure, avoiding a line × keyless join in Spark.
`MAX_KEYLESS_EVIDENCE = 10_000` caps the number of keyless records before collecting to the driver;
above the threshold it fails, because the scope may have been extracted wrongly. This is a record-count threshold,
not a byte limit or a limit on the total number of related corrections.

After the coverage fix per ADR-0013, `source_line` filters coverage by the delivery+30d window before `input_json`,
so the number of tail weeks does not make each line's input larger. The driver and workers still hold the
keyless set; it must be re-measured when the number of envelopes/payload size grows, even below the threshold.
Dropping `source_sequence_ranges` does not change labels: Silver `coverage_validity` already used
this field to check coverage once; `spark_jobs.lib.return_label.source_line` only reads it in the branch
without `coverage_status`, while Gold always passes the `coverage_status` map from
Silver. Pure Python test: `tests/unit/spark_jobs/lib/test_return_label_keyless.py` compares the full
`label_line` result when coverage has/does not have this field, with the same Silver status.

e2e numbers after the OOM fix (local, 6g, weekly coverage): small 45 days × 50 orders — 3,631 lines, `gold_build` 13.7 s, `gold_validate` 13.9 s, every check = 0, 357 `coverage_validity` records (15 before splitting into weeks). Separate Gold case 60 days × 4 orders × 3 SKUs: **131 generator-produced lines with `labeled_count_90d > 0`** (85 s), matching the pure Python count. Medium (48,874 lines) has not been rerun after the fix — to be run on GCP.

## Point-in-time

For each placement time T, the historical label is recomputed with exactly `label_line(..., T)` over
observations with `source_observed_at < T`. Therefore neither late events nor future corrections
leak into the history at T. Only lines of the same dataset/seller_sku are counted, excluding the target line,
with state labeled, maturity_end < T, decision_observed_at < T, and cohort_date in
`[UTC date(T) − 90, UTC date(T))`. If the count is 0, the rate is null.

`decision_observed_at` takes the max over **all visible related observations**,
including terminal/coverage and corrections; this is conservative and may delay
availability instead of guessing a minimal evidence set. The validate job independently joins
the label snapshot with the feature, compares lineage and numerator/denominator; it also explodes the references and
joins back the timestamp of each observation in the input. It does not call the feature function again
to prove PIT by itself.

## Why the feature can be 0 — scale fixed with weekly coverage

The old-source run measured the old scale source: coverage was emitted only at `D(observation_days−1)`,
after every Order. Scale45 has 1,434 lines, 1,373 labeled, but every segment only
opens on Oct 12, 2026, when the 90-day cohort has already expired; no original target has history.
This was a limitation of the old source, **replaced by the coverage fix per ADR-0013** with consecutive weekly envelopes
(ADR-0013). The fixture is kept byte-for-byte.

The scale source now emits each `[week_start, week_end)`, non-cumulative, until the end of
follow-up. Coverage is known at least one day after the end of the week, and at least 1µs after the last late
row in the week. A label needs a chain of complete envelopes with valid receipts,
no unresolved scope, covering continuously from the first delivery to past the last deadline.
A week gap, partial status or insufficient receipt is still unknown; maturity is never used in place of coverage.

Pure Python measurement, seed7, UTC, `ScaleConfig(days=60, orders_per_day=4, sku_count=3)`,
reading only the source, checking receipts once, then `source_line` → `label_segments` → `feature_window`:

| Metric | Result |
|---|---:|
| Generator-produced Orders / lines | 240 / 367 |
| Labeled segments / non-empty PIT windows | 382 / 382 |
| **Generator-produced** targets with `labeled_count_90d > 0` | **131** |
| Max coverage records in one line's input | 30 |

The number of segments can exceed the number of lines: label/decision changes with observations.
The first/last real targets with history are reconciled against `sku_return_feature` as-of.
count0 is still possible with a short source, a SKU with no history yet, or before coverage
is sufficient; that result is valid. The fixture still has 30 labeled lines but 0 original targets with
history; the `sku-A` probe on Apr 3 has count15/positive15/rate1. A one-line minimal scale
has a segment opening on Feb 6, 2026; the probe at +1µs gives count1/positive0, exactly the count0 boundary.

Gold sends keyless evidence to the workers once; `source_line` keeps only coverage whose
interval intersects `[min(actual_delivered_at), max(actual_delivered_at)+30d]`, taking the
superset over **every revision** of delivery. It keeps the whole revision/correction group of the related envelope
so that an old claim is not revived when a new revision narrows the interval.
Envelopes outside the range do not help fill the coverage chain, so they do not change state/value;
dropping them also prevents unrelated observations from pushing back the time the label is known.
Authority is still checked on the full source before filtering, with the same feed/producer/epoch and scope.

`input_json` does not grow with the number of tail weeks: the intersection is bounded per line (the scale source
splits deliveries one day apart, so at most 6 weeks × 5 feeds = 30 envelopes).
The single-interval fixture is kept. Revisions/corrections can grow the input with the
change history of the line itself. `source_sequence_ranges` is still dropped at the driver after Silver
verification. `MAX_KEYLESS_EVIDENCE=10_000` is kept: by default 39 weeks × 9 feeds = 351
complete envelopes + 6 unavailable, leaving headroom for uncertainty; this is a limit on the total
keyless records collected to the driver, not on the number of envelopes repeated in each line's input.

Reproduce in pure Python:

```bash
.venv/bin/pytest -q -s tests/unit/spark_jobs/lib/test_return_feature_generated.py
```

The operator runs the scale regression and the small Spark Gold case separately (240 Orders, target
≤10 minutes; Spark not yet measured in the sandbox):

```bash
bash tests/harness/run_order_lifecycle_scale.sh --profile small --driver-memory 6g
docker run --rm --network none --read-only --tmpfs /tmp:rw,size=2g \
  --mount type=bind,src="$PWD",dst=/workspace,readonly \
  --entrypoint spark-submit platform-airflow:2.10.5-spark3.5.3 \
  --master 'local[1]' --driver-memory 6g \
  /workspace/tests/spark/check_order_lifecycle_weekly_gold.py \
  WeeklyGoldChecks.test_generated_orders_have_positive_pit_history
```

Expected Spark Gold result: **131 lines** with count >0, matching the pure Python measurement, without
injecting a later order/probe. The old fixture + late-label tests remain in
`tests/spark/check_order_lifecycle_gold.py`.

## Contract cases and checks

Expected values for L01–L17 are copied from the contract; pytest data is hand-built, not from the simulator.

| Case | Expected | pytest |
|---|---|---|
| L01 | 1, cohort D20, maturity D50 | PASS |
| L02 | 0, allocation D0 window expired | PASS |
| L03 | 0, approved D31 | PASS |
| L04 | 1, product success exactly D30 | PASS |
| L05 | 0, pending/failed do not qualify | PASS |
| L06 | 0, shipping-only | PASS |
| L07 | 0, delivered1/cancelled2 | PASS |
| L08 | immature | PASS |
| L09 | unknown, missing allocation | PASS |
| L10 | unknown/provisional → chose unknown | PASS |
| L11 | 1, event known D35, coverage D39, C D40 | PASS |
| L12 | 1 after buyer cancel, no correction | PASS |
| L13 | before correction 1; after correction 0 | PASS; assumes coverage known D31, invalidation known D40 |
| L14 | ineligible/null | PASS |
| L15 | immature when C = maturity_end | PASS |
| L16 | A/B unknown; resolve A1/B0 | PASS; assumes shared Order, resolution known D65 |
| L17 | snapshot 0/1/immature; old export checksum unchanged | PASS; coverage D31, late event D35, corrected delivery D45 known D50 |

There are also ±1 microsecond boundaries, exclusive cutoff, missing/unavailable coverage,
identity conflict, money reversal, 90-day cohort, different SKU, PIT correction and a
coverage digest check from hand-built envelopes. pytest result: **31 passed**.

The Spark harness generates the default fixture in `/tmp`, local[1], no catalog. It checks
every validate-job invariant, parity for at most 200 lines, fixed count/headline; it adds
one Order observed after follow-up so the feature has real history (15 positives), then
injects a June label snapshot into the April feature to force the PIT check to catch it. It also covers fan-out,
unknown with a label, and labeled but not yet mature.

Spark result (operator): **`Ran 5 tests in 587.128s — OK`** (`tests/spark/check_order_lifecycle_gold.py`, image
`platform-airflow:2.10.5-spark3.5.3`, `--network none --read-only`, driver 3 GB). Covers: official fixture numbers + empty history;
PIT with positive history and catching an injected late-known label; validate catches fan-out; validate catches `unknown` with a label and
`labeled` not yet mature. A 1 GB driver once hit `OutOfMemoryError` at the Gold → warehouse step.

## Commands

```bash
.venv/bin/pytest -q tests/unit/spark_jobs/lib/test_return_label.py

docker run --rm --network none --read-only --tmpfs /tmp:rw,size=1g \
  --mount type=bind,src=$PWD,dst=/workspace,readonly --entrypoint python \
  platform-airflow:2.10.5-spark3.5.3 /workspace/tests/spark/check_order_lifecycle_gold.py

# First publish Silver at the right observation snapshot with spark_jobs/silver/build_order_lifecycle.py --as-of.
scripts/run/run_order_lifecycle_spark.sh /opt/spark_jobs/gold/build_order_lifecycle.py \
  --cutoff 2026-06-01T00:00:00.000000Z --run-id gold-v2-example \
  --receipt /receipts/gold-v2-example.json
scripts/run/run_order_lifecycle_spark.sh /opt/spark_jobs/gold/validate_order_lifecycle.py \
  --cutoff 2026-06-01T00:00:00.000000Z --run-id gold-v2-example \
  --receipt /receipts/gold-v2-validate-example.json
```

## Limitations

- The Codex environment has no pyspark/pandas and the Docker socket denies permission; Spark and
  the Iceberg catalog have not run there; the Spark numbers above were run locally by the operator on 2026-10-19/20.
- Keyless coverage is still repeated in each line's `input_json`, and the keyless set lives on the
  driver/workers; see the memory limit above. There is no e2e evidence for large data yet.
- An allocation with portions delivered at several times: unknown, because the contract does not
  define a single d[a]. A Refund with several allocation money rows is also unknown
  instead of guessing the split; a single row is trusted only when it matches the full product amount.
- The `capability`/`profile_id` of operational coverage is the authority evidence in the
  fixture; no separate authority registry for external sources is integrated yet.
- There is no valid coverage before the placement time of the fixture's first-period orders, so the default feature
  has count0/rate null; the harness adds an Order after follow-up to also check a non-zero denominator.
- The training export in L17 is a pytest file that keeps a checksum; this package has no training pipeline,
  MLflow, v2 serving or v1/v2 migration comparison.
- Facts/features/audit serve the current generation; old labels are append-only. There is no
  atomic multi-table publication or retention/rollback of a whole generation yet.
