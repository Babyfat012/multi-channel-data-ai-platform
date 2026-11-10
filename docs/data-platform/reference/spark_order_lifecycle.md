# Spark baseline and skew — v2 workload

> **Pipeline v2 (order lifecycle) — designed and tested locally, not the deployed pipeline.** The pipeline running on AWS is v1 (DP1 → DP4 + ML), see [Data Platform evidence](../evidence.md). Mentions of GCP below are an old plan that was never run; the current cloud target is [AWS](../../shared/aws_deploy.md).

The lifecycle v2 workload, independent of the Bronze v1 evidence in
[spark.md](spark.md). **Spark has not been measured in the agent sandbox**. Numbers in the table
are filled in only after the operator runs it and saves the JSON/event log; they are not inferred from timings.

The job [measure/measure_order_lifecycle_skew.py](../../../spark_jobs/measure/measure_order_lifecycle_skew.py) calls the real module's
`bronze_from_root` → `build_silver` → `build_gold` directly, including the
`line_evidence` path by line/order/correction. It does not copy the Silver/Gold pipeline.
It then checkpoints the same fact/segments once and runs the original `sku_features` and
[`sku_features_salted`](../../../spark_jobs/lib/order_lifecycle_skew.py). Job groups are separated into
`silver_gold_build`, `baseline`, `salted`, `correctness`; the feature comparison only covers the
sweep and related joins, without recomputing Silver for each option. It does not write
to the catalog/Iceberg, and it is not a benchmark of lakehouse write speed or Walmart JDBC.

Default baseline: Spark local[2], UTC, shuffle partitions=16, AQE=false,
`skewJoin.enabled=true` (only effective when AQE is on), auto-broadcast=-1 to make the shuffle
distribution easy to observe. Salted keeps exactly that configuration. This is an experimental configuration,
not a proposal to change the production configuration. `--aqe` allows a separate control measurement;
do not merge AQE-on numbers into the AQE-off table. When AQE changes the plan or the broadcast exchange,
stage identification must be rechecked before comparing; keep the full SQL plan/event log.

Default generated source: seed=7, days=90, orders_per_day=20, sku_count=100,
Zipf s=1.2, followup=270, observation=285. The order-creation range is long enough for features to have
mature history, unlike a 10-day sample that usually yields only zero features. The small Spark test uses
90 days × 4 orders/day, 32 SKUs, the same seed. The two sets are not claimed to share the same
`dataset_version`. The specific source is pinned in the JSON via `dataset_versions`,
the full manifest (seed/configuration/digest), the manifest SHA-256 and the cutoff; to replay, reuse
`--source` pointing at the exact immutable root. The dataset ID depends on code/Python; do not guess
the ID before generating. The runtime JSON has the Spark/Python/app ID; the operator also records the actual image ID
along with the evidence. The pinned 30-day source in
[source-order-lifecycle-scale.md](source_order_lifecycle_scale.md) can be used, but `nonzero_features` must be checked before
using it as a correctness proof with history.

| Metric, same input + AQE off | Baseline SKU | Salted SKU + bucket |
|---|---|---|
| dataset_version + seed + manifest hash | not measured | same source |
| Spark/Python/image ID/app ID | not measured | same runtime |
| sweep stage ID + task count | not measured | not measured |
| records/task max / median / p95 | not measured | not measured |
| records/task max ÷ median | not measured | not measured |
| shuffle records/task max / median / p95 | not measured | not measured |
| shuffle bytes/task max / median / p95 | not measured | not measured |
| top-5 keys by sweep event count | not measured | not measured |
| exceptAll baseline-only / salted-only | not measured | required 0 / 0 |

**Not measured:** run `python tests/spark/check_order_lifecycle_skew.py`
in a prepared Spark runtime; the Docker command and the CLI that saves the full JSON/event log are at the
end of this document. There are no stage/task metrics,
manifest hash, bag equality or v2 skew-reduction conclusion yet; the table above stays
not measured, and v1 evidence is not used as a substitute.

The JSON has every stage and each successful task (partition index, task attempt, input
records, shuffle records, shuffle bytes, duration), max/median/p95 and max/median.
`records_read` is input records + shuffle records; reading from cache may return 0, so
**do not call it the output row count**. Shuffle bytes include remote + local bytes.
The median includes empty tasks; if median=0 then ratio=null, the denominator is not replaced with 1. p95 is
nearest-rank. Stage/task retries keep stage attempts separate, keeping only the last successful attempt
of each partition; failed stages are not used for conclusions.

Sweep identification relies on the metric accumulators of the SQL Exchange hashpartitioning
(dataset_version, seller_sku[, _bucket]) feeding the Window through Sort/wrapper,
and the direct consumer stage of that exchange. Aggregate/join exchanges with the same
key are not automatically assigned the sweep role. Both SQLExecutionStart and AQE plan
updates are read. `sweep_exchange_inputs` marks candidates; check the physical plan, parent stage
and task metrics in the JSON before choosing a stage. Salted also has a per-SKU prefix over
**one total row per bucket**, which must be distinguished from the local `bucket_sweep`.
A consumer may also receive shuffle from a join; the JSON keeps the task's real totals
and does not attribute all those bytes to the window. `top5_partition_keys` counts open/close endpoints
and queries from the real feature windows; this is **logical key load**, not an exact
assignment of keys to tasks. The order/line joins in the build group still have their own metrics.

The Spark test requires a measurable stage, nonzero features and bag equality via
**two-way** `exceptAll`. The test prints max/median and bytes before/after. It only asserts that the ratio
drops when the baseline ratio≥2, at least 8 tasks, both medians>0, and the max logical key after
bucketing ≤60% of before. If not met, it prints `SKEW_INCONCLUSIVE`; a passing test is not treated
as proof that skew was reduced; the operator must increase the scale/choose the bucket count and rerun.
A single run's time is not used to claim a speedup.
The script also has hand-built fixtures with two-way comparison at microsecond/bucket boundaries,
half-open windows, query-only buckets, self-history subtraction, multiple datasets, null SKUs and
lines without created_at; it checks specific counts beyond the comparison with the baseline.

AQE `skewJoin` handles skewed partitions in a **join**. The cumulative Window over
(dataset_version, seller_sku) still requires a SKU's whole timeline to be ordered and in the same
partition; enabling AQE does not split that window. The top Zipf SKU can account for >10% of lines.
The v1 lesson "AQE does not fix write skew" has a similar operator limitation, but
v2 here is **window skew**, not write skew or skewJoin.

Salting uses fixed UTC ranges counted from the epoch, `ceil(180/buckets)` days wide:
18 buckets → ranges of about 10 days, not random hashing of lines. Open/close deltas
and queries are split by bucket; the dc/dp total of each bucket is computed, the prefix total of all
**preceding** buckets, and then added to the local cumulative sum. A delta at the same timestamp as a query
comes before the query, preserving [a,b), microseconds and the subtraction of the line's own history.
Query-only buckets also participate in the prefix; negative totals closing a window propagate correctly across
sparse ranges. History is not cut into segments or replicated into every bucket. Every SKU is
split, including cold SKUs: this saves a step collecting the hot SKU list but adds overhead.

Trade-off: extra aggregate, prefix, join and shuffle; many small groups/partitions, which may
increase total shuffle bytes and scheduling cost. The prefix is still per SKU, but its input
is reduced to the number of buckets with data; if the timeline is very long, that prefix also needs measuring.
If a hot SKU is concentrated in one bucket, this approach does not fix the skew.
Do not infer that it is faster at every scale, and do not apply it to the original Gold module
without Spark evidence. The Python oracle in
[unit/spark_jobs/lib/test_order_lifecycle_skew.py](../../../tests/unit/spark_jobs/lib/test_order_lifecycle_skew.py) checks the bucket/prefix mechanism;
DataFrame correctness must go through the Spark test.

Required operator command (repo mounted read-only, temporary data inside the container):

```bash
docker run --rm --network none --read-only --tmpfs /tmp:rw,size=2g -e PYSPARK_SUBMIT_ARGS='--driver-memory 4g pyspark-shell' \
  --mount type=bind,src=$PWD,dst=/workspace,readonly --entrypoint python \
  platform-airflow:2.10.5-spark3.5.3 /workspace/tests/spark/check_order_lifecycle_skew.py
```

To save the JSON stdout on the host, append `> /tmp/codex-15-spark-report.log` to the command.
This run does not delete the test's own `/tmp`, so the operator can investigate when running outside
the container; `--rm` still cleans up the container when it finishes. To keep the event log as well, run the
CLI with a host evidence directory mounted **separately** with write access:

```bash
mkdir -p /tmp/codex-15-evidence
docker run --rm --network none --read-only --tmpfs /tmp:rw,size=2g \
  -e PYSPARK_SUBMIT_ARGS='--driver-memory 4g pyspark-shell' \
  --mount type=bind,src=$PWD,dst=/workspace,readonly \
  --mount type=bind,src=/tmp/codex-15-evidence,dst=/evidence \
  --entrypoint python platform-airflow:2.10.5-spark3.5.3 \
  /workspace/spark_jobs/measure/measure_order_lifecycle_skew.py \
  --workdir /evidence/run-01 --output /evidence/report.json \
  --days 90 --orders-per-day 20 --seed 7 --sku-count 100 --buckets 18 --partitions 16
```

The workdir must be new for each run; scale data, temporary checkpoints and Parquet
output use disk space. For large sources, use a read-only source mount with `--source`; increasing temporary
disk capacity is up to the operator. Docker/cloud are not invoked from the agent sandbox.
