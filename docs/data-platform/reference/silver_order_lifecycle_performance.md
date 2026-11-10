# Why Silver v2 is superlinearly slow

> **Pipeline v2 (order lifecycle) — designed and tested locally, not the deployed pipeline.** The pipeline running on AWS is v1 (DP1 → DP4 + ML), see [Data Platform evidence](../evidence.md). Mentions of GCP below are an old plan that was never run; the current cloud target is [AWS](../../shared/aws_deploy.md).

Silver performance analysis. Documentation and proposed diffs only; no Spark changes have been applied.

## Conclusion and scope of evidence

**Fix the sequence-set processing in `coverage_validity` first, then reduce lineage recomputation.**
Medium spends **1,815.877 s** in ResultStage 100/job 53, out of a Silver phase of **2,090.103449 s**:
**86.88%** of Silver time. The corresponding Small ResultStage 103/job 52 takes only **8.706 s**.
The source grows **13.24×**, Silver grows **35.67×**, and this stage alone grows **208.58×**.
The ratios are divisions of the numbers in the sources below, not a new benchmark.

- `M` = `out/e2e-v2-medium.G5BI4k/spark.log` (local log, not committed).
- `S` = `out/e2e-v2-small.bpqNq2/spark.log` (local log, not committed).
- `MR` = medium receipt `out/e2e-v2-medium.G5BI4k/receipt.json` (local, not committed), lines 20–35, 55–87.
- `SR` = small receipt `out/e2e-v2-small.bpqNq2/receipt.json` (local, not committed), lines 20–35, 88–103.
- The form `M:9173` is the physical line number in that exact file (can be checked with `nl -ba`).

| Metric | Small | Medium | Source |
|---|---:|---:|---|
| Bronze rows | 40,398 | 534,853 | SR:21–23; MR:21–23; commit M:4406 |
| line_lifecycle / native_order_lines rows per table | 3,631 | 48,874 | SR:24–29; MR:24–29 |
| observed_evidence rows | 27,646 | 374,574 | SR:30–32; MR:30–32 |
| coverage_validity rows | 15 | 15 | commit S:7217; M:9180; SR/MR:33–35 |
| Silver (wall-clock seconds, including build, write and count of the written tables) | 58.602433 | 2,090.103449 | SR:95–103; MR:62–70 |
| Final stage coverage_validity (seconds) | 8.706 | 1,815.877 | S:7210; M:9173 |
| Final coverage_validity write job (seconds) | 8.706962 | 1,815.890594 | S:7213; M:9176 |

The medium receipt still has `status=running`, `gold_validate=failed/Py4JJavaError` (MR:3,83–87).
It only confirms that Silver completed; medium is not treated as an E2E PASS. The Gold error after Silver
is not taken as the cause of a stage that had already finished. The two profiles are two different datasets with different numbers of days;
two data points do not prove the whole pipeline has quadratic complexity, and the 100 GB time is not extrapolated.

## How the phase is delimited and timings are read

The runner `scripts/run/run_order_lifecycle_local.py:160–167` writes Bronze, then calls `build_silver`, writing in turn
`line_lifecycle`, `native_order_lines`, `observed_evidence`, `coverage_validity`; each write
is counted **from the materialized Iceberg table** (`:94–105`), and only at the end of Silver is `clearCache()` called.

M is delimited from line 4479 (operational scan for `_dedup`) to 9244 (final coverage count job);
S from 3511 to 7278. Logical plan construction time before the first scan is still included in the receipt.
The ranking is of **executed stages**; a job containing a stage is not added to that stage. AQE map-stage jobs
have no corresponding `Job N finished … took` line in this log: looking only at job finished lines
would miss many expensive scans. `Final stage`, `Submitting … missing tasks` and the stage
finished line are the three signals used to get the ID, task count and time. Stages run concurrently, so the sum
of stage times does not equal Silver wall-clock time.

The Spark callsites actually logged are only `NativeMethodAccessorImpl.java:0` for count/create and
`FutureTask.java:264` for broadcast; **there is no Python file:line** for most jobs. The table below
distinguishes the raw callsite from the Python location inferred from action order/code structure:

- `D`: `_dedup`, `spark_jobs/lib/order_lifecycle_lakehouse.py:426–441`; count `:437–438`, collect `:439`.
- `L`: `build_silver:522–523`, `line_lifecycle:646–668`, write `scripts/run/run_order_lifecycle_local.py:100`.
- `N`: `native_order_lines:147–243`, write runner `:100`.
- `E`: `observed_evidence:317–353`, write runner `:100`.
- `V`: `coverage_validity:356–412`, write runner `:100`. V includes the parent branches
  that run before the write-start message at M:9094/S:7132; not just the final write stage.

## The 20 most expensive medium Silver stages, compared with small

`C` = `create at NativeMethodAccessorImpl.java:0`; `K` = `count at NativeMethodAccessorImpl.java:0`;
`P` = `collect at /workspace/spark_jobs/lib/order_lifecycle_lakehouse.py:439`.
`Sh` = ShuffleMapStage, `R` = ResultStage. The line column records `submit/finished`: the submit line has the task count.
Stages are compared by the same role/branch order, **not** matched by equal IDs. AQE changes the DAG between the two runs;
branches that cannot be matched with certainty are marked. The V root labels by submit order are only a positional
comparison; there is no physical plan yet to confirm whether each root is `cov`/`rows`/`declared`.

| # | Medium stage / type | Role / callsite | s medium | Tasks | M submit/finished | Small stage: s / tasks / S submit/finished | Shuffle |
|---|---|---|---:|---:|---|---|---|
| 1 | 100 / R | V: finish aggregate/sequence comparison + write / C | 1815.877 | 4 | 9106:9173 | 103: 8.706 / 5 / 7144:7210 | See final fetch below |
| 2 | 84 / Sh | V: read/gate/dedup coverage branch / C | 69.159 | 14 | 8457:8997 | 87: 5.112 / 6 / 6592:6813 | No stage-attached metric |
| 3 | 83 / Sh | V: read/gate/dedup coverage branch / C | 61.250 | 14 | 8444:8832 | 86: 5.120 / 6 / 6557:6781 | No stage-attached metric |
| 4 | 82 / Sh | V: read/gate/dedup coverage branch / C | 38.178 | 14 | 8405:8627 | 85: 4.992 / 6 / 6531:6720 | No stage-attached metric |
| 5 | 23 / Sh | L: materialize cache events / C | 29.102 | 13 | 5358:5454 | 23: 5.065 / 6 / 4177:4233 | No stage-attached metric |
| 6 | 17 / Sh | D: collect conflict IDs / P | 25.699 | 13 | 5068:5154 | 17: 4.709 / 6 / 3967:4026 | No stage-attached metric |
| 7 | 11 / Sh | D: unique.count / K | 25.087 | 13 | 4819:4922 | 11: 4.497 / 6 / 3786:3847 | No stage-attached metric |
| 8 | 5 / Sh | D: ops.count first time / K | 24.986 | 13 | 4510:4602 | 5: 5.478 / 6 / 3542:3598 | No stage-attached metric |
| 9 | 8 / Sh | D: ops.count second time / K | 24.491 | 13 | 4669:4752 | 8: 4.327 / 6 / 3667:3719 | No stage-attached metric |
| 10 | 76 / Sh | E: read/gate/dedup evidence / C | 23.235 | 14 | 7981:8108 | 79: 3.521 / 6 / 6180:6237 | No stage-attached metric |
| 11 | 87 / Sh | V: intermediate aggregate/shuffle / C | 20.134 | 8 | 8707:8920 | Intermediate V: 89/91/93, see below; no unique match | No stage-attached metric |
| 12 | 86 / Sh | V: intermediate aggregate/shuffle / C | 19.328 | 8 | 8655:8798 | Intermediate V: 89/91/93, see below; no unique match | No stage-attached metric |
| 13 | 72 / R | N: native write (parser union) / C | 8.672 | 84 | 7058:7874 | 75: 1.261 / 36 / 5664:6071 | No stage-attached metric |
| 14 | 78 / R | E: write observed_evidence / C | 8.030 | 8 | 8130:8214 | 81: 0.692 / 8 / 6259:6342 | No stage-attached metric |
| 15 | 94 / Sh | V: intermediate aggregate/shuffle / C | 5.327 | 8 | 9015:9079 | 98: 0.536 / 8 / 7044:7119 | No stage-attached metric |
| 16 | 89 / Sh | V: intermediate aggregate/shuffle / C | 5.172 | 8 | 8864:8951 | Intermediate V: 89/91/93, see below; no unique match | No stage-attached metric |
| 17 | 28 / R | L: lifecycle branch / C | 4.278 | 8 | 5552:5928 | 28: 0.530 / 8 / 4330:4601 | No stage-attached metric |
| 18 | 29 / R | L: lifecycle branch / C | 4.255 | 8 | 5587:5925 | 29: 0.508 / 8 / 4365:4578 | No stage-attached metric |
| 19 | 30 / R | L: lifecycle branch / C | 4.218 | 8 | 5632:5920 | 30: 0.519 / 8 / 4409:4688 | No stage-attached metric |
| 20 | 31 / R | L: lifecycle branch / C | 4.190 | 8 | 5653:5931 | 31: 0.500 / 8 / 4430:4708 | No stage-attached metric |

Small intermediate V: stage 89 = 0.153 s / 8 tasks (S:6739–6804),
91 = 0.336 s / 8 tasks (S:6866–6977), 93 = 0.224 s / 8 tasks (S:6921–7001).
M has shuffle stages 86/87/89 in the corresponding positions; S has a broadcast ResultStage 93 branch.
Per-operator ratios are not inferred from these AQE pairs.

The shuffle log has fetches but no event-log/task-metrics read/write totals per stage.
The fetch lines in the final V stage are all local, remote = 0:

| Run / stage | Observed fetches (each line is one fetch, not a task total) | Lines |
|---|---|---|
| M / 100 | 67.1 MiB; 13.9 MiB; 110.5 MiB; 15.9 MiB | M:9122–9123,9131–9132 |
| M / 100 | 312.6 KiB; 1,385.1 KiB; 503.4 KiB; 2.5 MiB, plus small blocks | M:9139,9142,9150,9153 |
| S / 103 | 5.4 MiB; 3.7 MiB; 2.9 MiB; 1,949.1 KiB; 1,157.6 KiB | S:7168–7174 |

Interleaved fetch lines of concurrently running stages are not attributed to a specific stage;
rounded numbers are not summed into an "exact shuffle".

Cross-check with job finished: M job 53/V = 1,815.890594 s (M:9176),
38/N = 8.679103 s (M:7877), 42/E = 8.033009 s (M:8217);
S job 52/V = 8.706962 s (S:7213), 37/N = 1.266062 s (S:6074),
41/E = 0.693781 s (S:6345). The final `L` write job takes only 0.285210 s (M:6798)
because the scan/dedup/join work already ran in earlier map/broadcast stages.
The final V count job = 0.409205 s (M:9244), proving that counting the table after the write
does not repeat the whole V lineage.

## Causes, hypotheses and refutation tests

### H1 — Comparing sequence sets with `array_distinct(array<struct>)`: highest priority

`coverage_validity:391,398–406` collects two arrays of `(partition,seq)` structs, sorts them,
then runs `array_distinct` on each array before comparing for equality. The arrays grow with the number of coverage
events, not with the number of output rows (only 15). This is a candidate for quadratic cost
if the runtime handles struct distinct via a linear search over already-seen elements.
**The Scala implementation of this runtime has not been checked; this is a hypothesis, not a
profiler conclusion.** Source code was not downloaded from the network, per the task constraints.

Evidence distinguishing it from scan/I/O: the four M stage 100 tasks start together at M:9108–9115;
they finish at **31,996 / 79,025 / 170,964 / 1,815,838 ms** respectively
(M:9159,9163,9167,9171). The five small tasks finish at **431 / 636 / 1,323 / 3,798 / 8,675 ms**
(S:7192,7196,7200,7204,7208). M fetches start at 13:34:27–29, but the slowest task
finishes at 14:04:43. A single task holds most of the wall-clock time: consistent with processing a large array
within one group, not consistent with concluding that every task had to wait for remote shuffle.

Refutable prediction: apply only diff A, keep digest/source/resources unchanged, remeasure V;
if the final stage does not drop clearly, H1 does not explain the bottleneck. A write/collect
containing `receipt_status` must be used; a bare `count()` may let the optimizer eliminate the expression
being measured. Take a thread dump/profiler of the slow task to distinguish `ArrayDistinct` from
JSON sort/hash if A is not enough. Do not claim H1 has been proven with Python pytest.

### H2 — Collecting the whole JSON payload and sorting/hashing per coverage: definitely present, share of time not yet isolated

`:385–394` joins on dataset/feed/producer/epoch and observation time, then
`collect_list(struct(sid,raw_json))`, sorts, builds the full JSON string and SHA-256.
Coverage revisions make the number of matched pairs grow with the number of envelopes sharing the same scope.
It does not join per Order Line; but one large coverage still gathers many raw JSON values
into one task. Fetches M:9122–9132 and the task spread above are consistent with skewed group sizes.
There are no metrics for matched count/group or heap/GC, so fan-out or OOM is not proven.

Prediction: after A, profile the sort/concat/SHA part and measure matched count per evidence_event_id.
If CPU/memory is still mostly in the digest, a hash computed by iterator/stream in exactly the
canonical order is needed, rather than `collect_list` into a huge string. Do not replace it with an XOR/hash
of child hashes: that would change `record_set_sha256` and the source contract. This long-term proposal does not yet
have a safe diff, because serialization equivalence and size limits must be proven.
Increasing shuffle partitions cannot split one coverage group across several tasks.

### H3 — Recomputing gate/dedup because only `events` is cached: definitely present

`_dedup:437–439` performs two `ops.count()` calls, one `unique.count()` and a conflict collect;
`events.cache()` is only registered **after** all of these actions (`build_silver:522–523`).
The gate `:464–492` has many `get_json_object` expressions to check required fields; there is no Python UDF.

M stages 5/8/11/17 all read operational Bronze with 13 tasks, about 25 s per stage
(M:4479,4640,4788,5037 and the table). Stage 23 reads the same branch again to materialize events
(M:5358–5454). Caching events helps lifecycle, but does not help actions that ran
before the cache, the native parser, or evidence, which comes separately from `known`.
The current state cannot be described as "no caching at all".

Evidence is physically written by M:8222–8224, but `built['coverage_validity']` was already
built from the original evidence DataFrame (`build_silver:639–641`). Assigning `result[name] = spark.table`
in `write_tables` does not replace the lineage of the V already created. That is why the three V root stages 82/83/84
reread Bronze with 14 tasks after E had been written, from 13:33:12 to 13:34:22
(M:8400–8457,8627,8832,8997). They overlap, so they do not add up to time saved.
Diff C cuts this lineage at the physical E table in the runner; it does not address H1/H2.

### H4 — Lifecycle fan-out, global window or Python UDF: not a main candidate in this run

`line_lifecycle:648–666` aggregates each fact to line level before the left join;
`_dedup`, `_latest`, and the coverage/uncertainty windows all have `partitionBy`.
The native explode is one row per item, and the native/lifecycle totals both equal the line count
in the receipt. `coverage_receipts(known)` has `sequence/explode` and anti joins, but
the runner does not materialize this table; V stages are not attributed to that function just because of a similar name.
`reconcile()` is not called by the Silver runner. `Registry` and `required_json_paths`
are read while building expressions; they are not UDFs run per row.
There is no log evidence of a quadratic join per Order Line in Silver.
Equal counts do not prove each identity/amount is correct: the row sets still need reconciling.

## Proposed diffs, order of application and conditional impact

The diffs below are only proposals for the operator/next implementer to apply. No
`spark_jobs/*.py` or runner changes were made in this task. Every "saved" second below is a time
budget that could be affected, not a measured result; the diffs do not add up mechanically.

### A — Distinct inside the aggregate, compare normalized arrays

Location `spark_jobs/lib/order_lifecycle_lakehouse.py:391,398–406`:

```diff
@@ def coverage_validity(evidence: DataFrame) -> DataFrame:
-        F.array_sort(F.collect_list(F.struct(F.col("partition"), F.col("seq")))).alias("actual_seq"),
+        F.array_sort(F.collect_set(F.struct(F.col("partition"), F.col("seq")))).alias("actual_seq"),
@@
-        F.array_sort(F.collect_list(F.struct("partition", "seq"))).alias("declared_seq"))
+        F.array_sort(F.collect_set(F.struct("partition", "seq"))).alias("declared_seq"))
@@
-             & (F.array_distinct(F.coalesce("declared_seq", empty_seq))
-                == F.array_distinct(F.coalesce("actual_seq", empty_seq)))
+             & (F.coalesce("declared_seq", empty_seq)
+                == F.coalesce("actual_seq", empty_seq))
```

Keep received_rows, digest, the time filter, null coalescing and every other condition unchanged.
Both sides are still sorted sets of structs; distinct after sort and sort after collect_set
must give the same result. A struct with a null field is still a non-null struct, so it needs its own
test; rows missing partition/seq cannot be assumed droppable.

Largest impact budget: the final V stage **1,815.877 s** (M:9173). If H1 dominates,
most of that stage is saved; the remaining Silver time by subtraction is **274.226449 s**
(MR:64 minus M:9173), which is **not** a new runtime forecast: A still needs sort/hash/write.
The most sensitive test is old/new `coverage_validity` on the same evidence with large sequence
groups, an action writing `receipt_status`, and a two-way result comparison.

### C — Runner uses the written evidence as the coverage_validity source

Second in time budget. Location `scripts/run/run_order_lifecycle_local.py:94–105`.
Precondition: keep the current SILVER order, E before V. Check this order during review;
do not move all the data to a driver-side collect.

```diff
@@ def write_tables(spark, frames, zone, receipt, item):
     for name, frame in frames.items():
+        if zone == 'build_silver' and name == 'coverage_validity':
+            from spark_jobs.lib.order_lifecycle_lakehouse import coverage_validity
+            frame = coverage_validity(result['observed_evidence'])
         target = f'local.{zone}.{name}'
```

This is an E2E-specific seam; the production caller
`spark_jobs/silver/build_order_lifecycle.py` is not optimized yet. Any caller that writes E then V also needs to decide the cache lifecycle
or reread the table in a similar way; do not silently add an indefinitely living cache to the library API.
Tests that call `build_silver` directly still use the original DataFrame.

Observed budget: the V root segment from **13:33:12 to 13:34:22 ≈ 70 s**
(M:8400–8457,8997), plus the intermediate shuffle before the final stage at **13:34:27** (M:9094).
C may reduce part of this segment, but it still has to read Iceberg E, parse the coverage fields,
join and aggregate. No reduction of the total 38.178+61.250+69.159 s is promised, because the three roots run in parallel.
C does not remove the original E cost of **23.235 s** scan + **8.030 s** write (M:8108,8214).

Risks: the E/V ordering dependency, and schema/type source changes caused by the Iceberg round-trip.
Compare schema, nulls, timestamps and every result row of V before/after; test SILVER output
in the correct order, and confirm no table dependency on an old run appears (each run has its own catalog).

### B — Do not count `ops` twice

Location `spark_jobs/lib/order_lifecycle_lakehouse.py:436–439`:

```diff
@@ def _dedup(ops: DataFrame) -> tuple[DataFrame, dict]:
+    input_rows = ops.count()
     stats = {
-        "input_rows": ops.count(),
-        "duplicate_rows": ops.count() - unique.count(),
+        "input_rows": input_rows,
+        "duplicate_rows": input_rows - unique.count(),
```

Removes stage 8, a budget of **24.491 s** M:4752 (small **4.327 s**, S:3719),
minus scheduling/cache changes between runs. It does not change the dedup window, conflict IDs
or the definition of a duplicate. Low risk on a fixed Bronze snapshot; check that the
retry/conflict/correction stats are unchanged. The `unique.count()`/collect/materialize
events calls still rerun: B is not claimed to solve dedup as a whole.

If you want to cache `known` or `unique` for further optimization: an API is needed that owns the cache and releases it
in `finally` for every caller, with memory/spill measured before deciding. `known` holds the raw_json
of every feed; this cache plus events/digest could worsen the local heap. Blanket caching or a larger driver RAM
is not yet proposed as evidence of solving H1.

## How to prove results are unchanged and measure the impact

This analysis uses recorded logs; the slow expression has not been reproduced directly, and there is no profiler or post-diff benchmark yet. The protocol below must be run to verify the changes.

The operator applies each diff independently and keeps before/after evidence; A first, then C, then B, with the same source,
seed/cutoff, local[*]/driver memory/shuffle configuration, and a machine running no other workload.
Record the event log/physical plan on rerun to attach operator and shuffle metrics precisely
to stages, and capture heap/GC/task CPU if the V stage is still slow. Compare several runs
and report each one; stage/job numbers may change, so compare by role rather than pinning IDs.

Cases to add to the Spark regression suite before applying A (not written in this documentation task):

- Compare old/new V fully with two-way `exceptAll` and schema; keep a reference of the old function
  in the test environment to run both implementations on the same DataFrame.
- Empty coverage; missing partition; null sequence; overlapping/repeated ranges; duplicate sequences
  with wrong record count/digest; missing/unexpected sequence; unavailable;
  different epoch/dataset scopes; multiple revisions and a cutoff exactly equal to observed_at.
- Keep the canonical digest unchanged: raw_json with whitespace, sid order, duplicate-sid events
  with different content, retry and correction. `observed_evidence` still keeps every revision and
  conflict; do not replace it with the latest `events` that has conflicts removed.
- Reconcile more than counts: the multiset of all columns of the four Silver tables, dedup stats,
  quarantine and receipts; point tests before/after a refund correction and native as_of.
- A separate measurement for A: materialize shared evidence, increasing sequence groups,
  write V with receipt_status, record time/CPU and old/new equality. This script does not
  exist in the repo yet; the operator needs to add it, and the existing pytest suite does not measure Spark performance.

Commands the operator must run once a fix exists (with the image/cache available offline):

```bash
# Silver fixture and scale regression, no network, no project catalog.
docker run --rm --pull never --network none --read-only --tmpfs /tmp:rw,size=1g \
  --mount type=bind,src="$PWD",dst=/workspace,readonly --entrypoint python \
  platform-airflow:2.10.5-spark3.5.3 /workspace/tests/spark/check_order_lifecycle_silver.py

docker run --rm --pull never --network none --read-only --tmpfs /tmp:rw,size=2g \
  --mount type=bind,src="$PWD",dst=/workspace,readonly --entrypoint python \
  platform-airflow:2.10.5-spark3.5.3 /workspace/tests/spark/check_order_lifecycle_silver_scale.py

# E2E creates its own PostgreSQL/catalog, keeps log/receipt, does not use the project DB.
bash tests/harness/run_order_lifecycle_scale.sh --profile small --driver-memory 6g
bash tests/harness/run_order_lifecycle_scale.sh --profile medium --driver-memory 6g
```

The harness has commands to check the cache/image and an internal Docker network by design;
the operator runs these commands, they were not run as part of this analysis. Compare small/medium Silver seconds,
the final V stage, task distribution, and the business tables/checks; if medium still has the current Gold error,
report it separately, and do not treat `status=running`/Silver passed as E2E passed.
For a fair control measurement, rerun the old version with the same `--driver-memory 6g` and source as the new one;
do not use the historical small run as evidence of resources identical to medium.

## Checks performed in this task

- Extracted stage/job/task from both spark.log files, delimiting Silver by scan/write/count
  in the runner; did not run Spark and did not modify executable code.
- Read all of `build_silver`, the helpers it calls and the runner, the terminology and
  `docs/shared/architecture_decisions.md` relating to Bronze/Silver.
- `.venv/bin/pytest -q`: **679 passed, 1 skipped in 44.95s** (exit 0). These are Python tests; they do not confirm the performance or Spark equivalence of the unapplied diffs.

No scope deviations from the task: the document is at the intended path;
the proposed diffs have not been applied; there is no evidence yet of time saved after a fix.

## Reproducing the ranking from logs, without Spark

Run from the repo root; the script only reads logs. Submit lines are extracted together with the stage ID, to avoid
counting `Finished task` lines and mistaking task retries for partition counts.

```bash
python - <<'PY'
from pathlib import Path
import re
for folder, lo, hi in [('e2e-v2-medium.G5BI4k', 4479, 9244),
                       ('e2e-v2-small.bpqNq2', 3511, 7278)]:
    tasks, ranked = {}, []
    for n, line in enumerate(Path('out', folder, 'spark.log').read_text().splitlines(), 1):
        m = re.search(r'Submitting (\d+) missing tasks from (?:ShuffleMapStage|ResultStage) (\d+)', line)
        if m:
            tasks[int(m[2])] = (int(m[1]), n)
        m = re.search(r'(ShuffleMapStage|ResultStage) (\d+) \((.*)\) finished in ([\d.]+) s', line)
        if m and lo <= n <= hi:
            sid = int(m[2])
            ranked.append((float(m[4]), sid, tasks[sid], n, m[3]))
    print(folder, '(seconds, stage, (tasks, submit_line), finish_line, callsite)')
    for row in sorted(ranked, reverse=True)[:20]:
        print(row)
PY
```
