<!-- Describes the operational v2 stream, the independent oracle and the harness run by the operator. -->
# v2 source Stream — Kafka → Flink

> **Pipeline v2 (order lifecycle) — designed and tested locally; not the pipeline deployed on AWS.** The pipeline running on AWS is v1 (DP1 → DP4 + ML); see [Data Platform evidence](../evidence.md). Mentions of GCP below are an old plan that was never run; the current cloud target is [AWS](../../shared/aws_deploy.md).

Package D implements four source feeds observable by the Seller. Fix 10b has been checked locally
and is waiting for the operator to rerun the Kafka/Flink harness. The earlier Run 10 had a late check
that was too broad, so it is not used as proof of this version's late count. The truth source is not used, and no labels or
headline sales are produced. The v1 jobs in `flink_jobs/` are unchanged.

| Topic | Kafka key | Event time |
|---|---|---|
| `v2.carrier.delivery_events` | `entity_id` (delivery operation) | `payload.actual_delivered_at` |
| `order_lifecycle.seller_postsales.return_events` | `entity_id` (Return) | `effective_at` of requested/approved/received; only approved enters the window |
| `order_lifecycle.seller_postsales.refund_events` | `entity_id` (transaction + component) | `effective_at` of failed/success; only product success enters the window |
| `v2.seller_wms.stock_operations` | `entity_id` (stock operation) | `effective_at` |

Known-time is `source_observed_at`; event time is never replaced by known-time.
The producer sorts by UTC datetime, tie-broken by feed/sequence/event-ID. A retry is emitted
right after the original, with the same key, envelope and bytes. Exactly
`floor(N sources × duplicate_rate)` records are chosen to retry once, using a separate seeded RNG;
the default is 3% **of the source count**, not of the total after retries.

The current fixture/scale export has no `record_sha256` yet. When it is missing,
the producer adds exactly one field: the SHA256 of the JSONL source line without the LF, consistent
with how Bronze hashes raw JSON. If the field already exists it is kept. Every other field
is kept as is; the source on disk is not modified. The oracle computes the fallback hash itself from the source
line and checks the file SHA256 against the source manifest, without importing the producer.

The Kafka header `epoch` is a UTF-8 encoded integer; it is not the `source_epoch` in the
envelope (the source identity). A replay must choose a new epoch and a new manifest/log.
Use one producer at a time in a proof; there is no epoch-locking mechanism between
several independent producers. Epochs are isolated **before the watermark, by Kafka offset**;
the header is not used for filtering. The SQL outputs the epoch read from the header, and the comparator requires every
record to have the correct epoch; a different or missing header FAILs instead of silently losing messages.

`stream_manifest.json` (path given via `--manifest`) records the dataset, the source manifest hash,
seed/rate, source/emitted/retry counts per topic, inclusive first/last offsets
and record counts per partition. The ledger records each SHA, partition, offset and retry flag
in send order; the Kafka callback confirms the real offset. The overall digest is the SHA256 of the
canonical envelope + LF in emit order, including retries. The file sink simulates partition
0 and a separate offset per topic; this is not treated as evidence of Kafka offsets.
No successful manifest is written on a flush timeout or a delivery callback error.

| Job | Mode and mechanism | Output under `/opt/flink/output/order_lifecycle/` |
|---|---|---|
| `flink_jobs/order_lifecycle/baseline.sql` | Bounded batch, COUNT/MIN/MAX by topic × epoch × partition | `baseline/<epoch>/counts` |
| `flink_jobs/order_lifecycle/late_arrival.sql` | Bounded streaming, event-time watermark; one CASE splits main/late | `late/<epoch>/routes/route=main`, `route=late` |
| `flink_jobs/order_lifecycle/dedup.sql` | Streaming ROW_NUMBER by SHA, PROCTIME ASC keeps the first; audits input and kept; a final batch count after the synchronous DML | `dedup/<epoch>/{input,kept,counts}` |
| `flink_jobs/order_lifecycle/windows.sql` | Bounded batch ROW_NUMBER removes retries, one-UTC-day TUMBLE on event time | `windows/<epoch>/daily` |

Every source uses `scan.startup.mode = specific-offsets` and
`scan.bounded.mode = specific-offsets`, taken from the manifest of the exact epoch. The receipt records
inclusive first/last; the renderer passes start=first and stop=last+1 (exclusive).
The renderer checks for non-negative integers, valid partitions, ledger order and contiguous
offsets, and does not allow one epoch to interleave with another within a partition. An empty topic
without a boundary is rejected; the proof requires all four topics to have data. Producers must
run one after another, with no other writer interleaving in the offset range. Do not emit more while
a proof is running. The dedup job rereads exactly the same boundary in its second batch query.

The connector's specific-offsets only describe partition/offset, so the renderer creates
a separate Kafka table per topic and then UNION ALLs them into a shared source. This way partition
0 of topic A never uses the boundary of partition 0 of topic B by mistake. Both the streaming
source and the dedup batch count source use the same receipt. The renderer requires
`--manifest`; an epoch that differs from the manifest or a wrong boundary is rejected before the SQL is written.

Late uses `event_ts <= CURRENT_WATERMARK(event_ts)`; a NULL watermark goes to main.
The filesystem partition sink may drop the route column from the JSON; the comparator recovers it from
the path. These are two branches via SQL/file partitions, not a DataStream
OutputTag. There is no filter dropping late records, and unknown results are not set to 0.

The oracle simulates the watermark before each record, per topic/partition:
`max(event_ts before the record) - delay`, with timestamps truncated to millisecond precision as in SQL.
This number is only a model for the three-partition version: the upstream minimum and the watermark emit
period can shift the late count. The three-partition version **only requires input preservation
by an exact multiset of offset/hash**; the actually measured late count is still reported separately.

The late-count proof runs separately on `v2.late.<feed>`, **exactly one partition per topic**.
The producer `--late-topics` emits additional copies with the same envelope, order and retries,
writing the manifest `stream-<epoch>-late.json` next to the main manifest; the file sink has the log
`<file-out>.late`. The Kafka producer checks that the topic metadata has exactly partition 0.
The oracle reads this separate receipt and the observed source, not the Flink output. The renderer
`--late-topics` generates four SQL jobs from `flink_jobs/order_lifecycle/late_arrival.sql`, each reading only one topic, parallelism 1,
output `late-proof/<epoch>/routes`. Separate jobs keep one feed's watermark from being
held back by the minimum watermark of another feed.

The proof sets `scan.watermark.emit.strategy = on-event` (watermark pushdown of the
Kafka connector on Flink 1.20) and keeps the auto-watermark interval at 1ms. A watermark
is emitted after each event, so the next record is compared against the max before it, exactly as in the
oracle; the first record, with a NULL watermark, goes to main. No periodic tolerance range
is used. The comparator requires `late == oracle.late` per topic, preservation of the entire
proof input, the same envelope/retry multiset as the three-partition version, and an oracle with
late > 0 in at least two topics. `late_within_periodic_range` has been removed. If the runtime
cannot apply on-event or the result is off by one record, the proof FAILs; it is not relaxed
to the range [0,input]. The operator needs to verify pushdown support and the real output
with the harness; a unit test pass is not taken as a runtime pass.

With the scale source of 200 orders/day × 10 days, seed 7, a constant 3600-second delay,
running the file-sink producer on the observed source the operator generated for the oracle proof gives:
delivery 3412, return 706, refund 566, stock 0 (including retries). These are single-partition
expectations, not Flink measurements. The small unit fixture has only one late topic at
delay 3600; the comparator test that needs two topics uses delay 0, with a separate test at delay
3600 to check lifecycle/window. The harness still uses delay 3600.

The bounded batch window keeps all late records until EOF, avoiding dropping corrections whose
event time belongs to a past day. The two columns mean **counts of observed event revisions**:
`approved_return_events` (approved, quantity > 0) and
`successful_product_refund_events` (success/product/amount_minor > 0). A correction of the same entity
with a different SHA still counts as a new event revision; nothing is collapsed to latest,
and these are not counts of final Return/Refund entities or the official label. Shipping,
failed, requested and received do not increase these two measures. The window comparison does not
depend on the real-time watermark cadence.

The dedup summary has `input`, `dup_removed`, `kept` and `revisions_kept`; revisions_kept
is the number of unique SHAs minus the number of unique entities, per topic/epoch. The comparator also recomputes
these numbers from the audit/kept output and checks that the first offset of each SHA is kept, so the batch
counts cannot hide a dedup/restart bug. No TTL is enabled, since one could cause retries to be forgotten.

## Running locally without Kafka

Use a new output directory for each run:

```bash
.venv/bin/python -m generator.generate_order_lifecycle_fixture --output /tmp/v2-source-new
.venv/bin/python -m generator.publish_order_lifecycle_events --source /tmp/v2-source-new \
  --file-out /tmp/v2-stream-1.jsonl --manifest /tmp/stream_manifest.json --epoch 1
.venv/bin/python -m generator.build_order_lifecycle_stream_oracle --source /tmp/v2-source-new \
  --manifest /tmp/stream_manifest.json --output /tmp/v2-oracle-1.json
.venv/bin/pytest -q tests/unit/generator/test_publish_order_lifecycle_events.py
```

The producer/oracle keep the source and ledger in RAM; this is a small bounded harness,
not yet benchmarked on the maximum scale set of 20 million records.

## Kafka/Flink harness — run by the operator

Prepare the images `apache/kafka:3.9.1` and `platform-flink:1.20.3-kafka3.4.0` (built from
`docker/Dockerfile.flink`) first. The harness uses `--pull never`, does not change Compose,
does not touch existing services, and creates its own network/containers by PID. The KRaft broker
has three partitions per topic; the host port is a random localhost port. Scale source
200 orders/day × 10 days, seed 7; plus four single-partition `v2.late.*` topics.
Two epochs are emitted into the same topics before running,
and each epoch is compared independently. A trap removes the containers/network even on failure; output/logs,
checkpoints, the oracle, the compare JSON and the restart evidence are kept in the run dir.

```bash
bash tests/harness/run_order_lifecycle_stream.sh "$PWD/out/order-lifecycle-stream-proof-new"
```

The template's default checkpoint interval is 1000ms; the harness uses 100ms. The JM/TM share the
checkpoint/output bind mounts. The epoch 2 dedup case reduces Kafka fetches, waits via REST for a checkpoint with
`state_size > 0` while the job is still RUNNING, then stops and restarts the **temporary TaskManager**.
The JobManager stays up and does a fixed-delay restart from the last completed checkpoint. The REST
`latest.restored` field must exist and is written to `restart.json`; if the job finishes before a
checkpoint or does not recover, the harness fails and the proof is not accepted.
The output after the restart must still pass the full comparator. This is TaskManager failure
recovery; it does not prove JobManager HA or recovery from a disk-loss disaster.

To run individual jobs in an environment you prepared yourself:

```bash
.venv/bin/python scripts/run/render_order_lifecycle_stream.py --output /tmp/v2-sql-new \
  --epoch 1 --manifest /tmp/stream_manifest.json \
  --bootstrap kafka:9092 --delay-seconds 3600 --checkpoint-ms 1000
# Proof: the producer emits --late-topics; compute the oracle from the *-late.json manifest, then:
.venv/bin/python scripts/run/render_order_lifecycle_stream.py --output /tmp/v2-proof-sql-new \
  --epoch 1 --manifest /tmp/stream_manifest-late.json --late-topics
# Mount the SQL, output and checkpoints at the matching paths, then run sql-client.sh -f.
.venv/bin/python scripts/evidence/compare_order_lifecycle_stream.py --flink-output out/flink/v2 \
  --oracle /tmp/v2-oracle-1.json --late-oracle /tmp/v2-oracle-1-late.json \
  --output /tmp/v2-compare-1.json
```

GCP: the operator will add the deployment checklist later; this pass does not call the cloud or
replace the separate deployment checklist.

## Measurements

The operator ran `bash tests/harness/run_order_lifecycle_stream.sh` (Kafka 3.9.1 + Flink 1.20.3 in temporary containers,
source `generator.generate_order_lifecycle` 200 orders/day × 10 days, seed 7): **exit 0, both epochs `passed`, 190 s**. Receipt:
[`evidence/stream-v2-local-20261019.json`](../../evidence/stream-v2-local-20261019.json).

| Measurement | Runtime result (epoch 1; epoch 2 identical) | Reconciliation |
|---|---|---|
| Baseline topic/epoch/partition/offset | matches the producer ledger | producer ledger |
| Main + late, 3 partitions | main + late = input for every topic | exact input multiset |
| Late, 1 partition/topic | delivery **3,412**, return **706**, refund **566**, stock **0** | sequential oracle: 3,412 / 706 / 566 / 0 — **identical** |
| Dedup + revisions | delivery 3,610 → 3,494 (116 retries), return 2,113 → 2,052 (61 retries, 1,368 revisions kept), refund 1,121 → 1,092 (29 retries, 328 revisions kept), stock 8,874 → 8,623 (251 retries) | SHA, first offset, summary and derived counts |
| Daily windows | `daily_event_windows` pass | source oracle, retries counted only once |
| TaskManager restart | checkpoint with state after 11.0 s → stop/start TM → restore checkpoint **28** after 3.1 s → kept commit **15,261 = oracle** → REST cancel `CANCELED` | checkpoint state/restore via REST + exact output |

The operator fixed the harness/SQL while running (each fix has an inline comment): the Kafka port is chosen before start (KRaft forbids changing
the advertised listener), `kafka_partition`, watermark `INTERVAL 'H:MM:SS' HOUR TO SECOND`, batch mode set before `CREATE TABLE`,
quoting `` `day` ``, `sql-client` running as user `flink`. The bounded version of the restart case once failed 2/2 because the job finished before the first checkpoint ⇒ switched
to unbounded + cancel via REST (no longer dependent on machine speed).
