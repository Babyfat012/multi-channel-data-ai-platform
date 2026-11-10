# End-to-end v2 run on a local scale source

> **Pipeline v2 (order lifecycle) — designed and tested locally; not the pipeline deployed on AWS.** The pipeline running on AWS is v1 (DP1 → DP4 + ML); see [Data Platform evidence](../evidence.md). Mentions of GCP below are an old plan that was never run; the current cloud target is [AWS](../../shared/aws_deploy.md).

The runner `scripts/run/run_order_lifecycle_local.py` checks the whole chain scale source → Bronze → Silver → Gold →
PostgreSQL → EXPLAIN before/after indexing. One Spark session `local[*]`, an Iceberg Hadoop
catalog in `/tmp`, no Lakekeeper/MinIO needed. Truth is never read as input.
This is a runnability check before GCP, not GCP evidence and not 100 GB.

The operator runs it from the repo root; the image/JAR cache must already be present:

```bash
bash tests/harness/run_order_lifecycle_scale.sh --profile small
bash tests/harness/run_order_lifecycle_scale.sh --profile medium --driver-memory 6g
```

Small = 50 orders/day × 45 days; medium (the default) = 500 × 60, seed 7.
`--orders-per-day`, `--days`, `--seed`, `--sku-count`, `--sku-zipf-s`,
`--cutoff` and `--run-id` can be passed through the harness. `--source /workspace/out/nguon-co-san` uses
a source already generated in the repo; this path is inside the container. The source is mounted read-only.
Do not pass `--receipt` again through the harness: it already sets `/proof/receipt.json`.

The harness creates an internal Docker network and a dedicated PostgreSQL 15 that publishes no port,
waits for the DB to be ready, runs the image `platform-airflow:2.10.5-spark3.5.3`, and uses the Iceberg/JDBC JARs
from the volume `platform_airflow-data:/ivy:ro` without downloading anything. Spark runs as the image's default user,
without changing HOME or forcing the host uid; cleanup chowns the output back to the host and removes exactly its own
container/network. Logs/receipts are kept in `out/e2e-v2-<profile>.<suffix>/` even
on failure; the catalog and the generated source in the container's `/tmp` are removed after the run.

To run directly in a runtime that already has Spark/JARs, set `WAREHOUSE_TEST_DSN` in the env
(a PostgreSQL URI, as for the warehouse harness), or `POSTGRES_HOST/PORT/DB/USER/PASSWORD`:

```bash
spark-submit --master 'local[*]' --driver-memory 6g --jars /cache/iceberg-runtime.jar,/cache/postgresql.jar \
  scripts/run/run_order_lifecycle_local.py --profile small --receipt out/e2e-manual/receipt.json
.venv/bin/pytest -q
```

The DB must be empty, without a `warehouse_lifecycle` schema; the runner refuses to write over an existing
schema. The DSN/password is not stored in the receipt. Use the harness's temporary DB to isolate the measurement.

Receipt schema 1 contains the parameters, dataset_version, cutoff, row counts per table and per
stage, wall-clock time, all Gold checks/validation, the warehouse export receipt and
the full EXPLAIN. Silver writes the four tables Gold needs and reads them back; Gold validates before writing,
with `feature_sample=50` and the validator's default `sample_limit=200`. Gold also reads back from
the catalog before export. The gold_build stage has a count so that a real action is measured; uncached lineages
can be recomputed in validate/write, so stage times include recomputation.
The export has its own receipt, `receipt-warehouse.json`, and reconciles counts/amounts before publishing.
A failed stage records the exception type, the time and the row counts measured; the overall status is passed only when
every stage completes. A hard kill/OOM may leave only the most recent running receipt, which must
not be treated as PASS.

The benchmark SKU is the SKU with the most rows in the fact table, tie-broken by SKU name; event_timestamp is taken from
a real feature row of that SKU, with `as_of=created` of that same row. Use the benchmark
in `scripts/evidence/measure_order_lifecycle_warehouse.py`: 3 queries × 5 runs × two phases, pk_only/indexed,
rolling back the indexes after measuring. The pk_only phase still keeps the primary key. Compare the median shared hit+read
and read the full plan; do not infer a benefit only from lower wall time/reads, because the cache warms up gradually.
The default cutoff = manifest.horizons.source_observation_end; with the default source,
Q1's last-30-day window has no new orders, so it may return 0 rows. Report this number as is,
do not treat an empty Q1 as evidence that the index helps; a suitable source/horizon is needed when
evaluating Q1. Q2 still uses the real label cutoff; do not silently change the benchmark cutoff.

`peak_rss.python_driver_kib` measures `resource.RUSAGE_SELF.ru_maxrss` (Linux KiB).
`terminated_children_kib` measures RUSAGE_CHILDREN, not the total Python+JVM RSS;
a JVM that is still alive or a JVM managed by spark-submit may not be included in this metric.
Do not use that number as the peak of the whole Spark/executors/container.

| Profile | Dataset / source row count, facts | Source / Bronze / Silver (s) | Gold build / validate / write (s) | Warehouse / EXPLAIN (s) | Python RSS KiB |
|---|---|---|---|---|---|
| small | `dataset-15244a34…` / 40,398 source records, 3,631 facts | 3.3 / 7.4 / 51.5 | 7.6 / 10.9 / 1.8 | 2.8 / 0.3 | 161,296 |
| medium | 534,853 source records, 48,874 facts | 50.7 / 23.9 / 2,090.1 | 42.2 / **OOM** / — | — / — | — |

| Profile / query | Before: node / median buffers hit+read / rows | After: node / median buffers hit+read / rows |
|---|---|---|
| small / Q1 | Aggregate / 173 / 0 | Aggregate / 2 / 0 |
| small / Q2 | Aggregate / 583 / 5 | Aggregate / 583 / 5 |
| small / Q3 | Limit / 104 / 1 | Limit / 4 / 1 |
| medium / Q1 | not measured — medium did not reach the EXPLAIN step | — |
| medium / Q2 | not measured — medium did not reach the EXPLAIN step | — |
| medium / Q3 | not measured — medium did not reach the EXPLAIN step | — |

Local measurements, `--driver-memory 6g`. **small**: run after the rename and after three memory/performance fixes; every Gold check = 0; row counts of every table match the run before the rename. **medium**: the run *before* the fixes: Silver 2,090 s (87% in `coverage_validity`, fixed with `collect_set`), `gold_build` passed, `gold_validate` OOM in the feature oracle (fixed: grouped by SKU). Medium has not been rerun after the fixes. Small EXPLAIN: Q1's last-30-day window has no orders (0 rows), so it does not prove an index benefit; Q3 drops 104 → 4 buffers thanks to the index; Q2 is unchanged (583). Q1/Q2/Q3 are the three sample queries of `scripts/evidence/measure_order_lifecycle_warehouse.py`.


Docker/Spark/PostgreSQL were not run in the Codex sandbox. Local runs use the dev machine's CPU/disk/cache
and a Walmart bootstrap file instead of real source CDC/JDBC; nothing is inferred about performance,
cost or durability on GCP. The operator keeps the real JSON/logs and fills in the table after running.
