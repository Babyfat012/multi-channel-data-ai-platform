# Spark optimization — Bronze stage

This document records **how the lakehouse's first Spark job was optimized**: from a deliberately
bad baseline, through an assumption that the measurements refuted, to the real cause and the result at full scale.
Bronze measurements are kept per run.

Benchmark measurements and App IDs are recorded in the [Spark UI evidence index](../../evidence/spark-ui/README.md) —
no re-measuring, no guessing. Where a number is missing, it says "not measured".

---

## 1. Goals and scope

**What Bronze is** (per [terminology and data layers](../../shared/terminology.md)): a faithful copy of the data the Channel hands to
the Seller. It only converts files to a columnar format and adds technical columns (Export Date, source
file name, ingestion time). It does **not** rename columns, does **not** merge schemas, and does **not** drop
duplicates. Bronze has **one table per CHANNEL**, not one table per schema shape: Amazon's two schema shapes
(v0 and 2026-01-01) share one table because Bronze **keeps the raw JSON string** and does not parse it —
without parsing there is no need to choose a set of column names. Normalizing key names into one shape belongs to **Silver**
(see `docs/data-platform/reference/silver.md`). Tables in this layer use the `raw_` prefix.

**What this job does:** `spark_jobs/bronze/ingest_amazon_order_snapshots.py` reads Amazon's JSONL export files
in `/data/raw` (mounted read-only into the Spark container) and writes them to the Iceberg table
`lakehouse.bronze.raw_amazon_orders` through the Lakekeeper REST catalog; the table data lives on MinIO.
The job **does not parse JSON and does no cleaning** — exactly Bronze's "copy as-is" role.

**Scope of this document:** the baseline, the performance optimization sequence and the Spark UI evidence for
Bronze Amazon. Section 9 connects this job to the later Airflow run; the Silver/Gold design and measurements
are in [`silver.md`](silver.md),
[`silver-five-channels.md`](silver_five_channels.md) and [`gold.md`](gold.md).

---

## 2. Table shape and why

The four settled design decisions are recorded in full in the table below. What matters is not
what was chosen, but **why**:

| # | Decision | Reason |
|---|---|---|
| Q1 | Bronze **keeps the raw JSON string** in the `payload` column; only Silver parses | Amazon key names change mid-period (`AmazonOrderId` in January → `orderId` in May). Parsing in Bronze would mean solving schema evolution in Bronze — contrary to its "copy as-is" role |
| Q2 | **Two separate tables** `raw_amazon_orders` + `raw_walmart_orders` | Iceberg commits are atomic: with separate tables, if one source fails the other can still be rerun on its own |
| Q3 | Provenance **option A**: each table has its own source column name | Amazon uses `source_file` (read from disk, so it has a file name); Walmart uses `source_table` (read over JDBC, `input_file_name()` returns an empty string). Both have `ingested_at`, `run_id`; Walmart also keeps the **source's** `fetched_at`, not merged with `ingested_at` |
| Q4 | `raw_amazon_orders` PARTITIONED BY (`export_date`); `raw_walmart_orders` PARTITIONED BY (`days(fetched_at)`) | The partitioning matches exactly how the files are laid out on disk. `days()` is safe even if the source changes its pull cadence |

**Accepted consequence: the Amazon table has NO order ID column.** Its columns are only
`payload, source_file, export_date, ingested_at, run_id`. This is the price of Q1: to look up
an order, look it up in Silver. Bronze exists to **rerun when Silver fails**, not for lookups.

**Another consequence:** the *schema evolution* step lives in `02_silver_*.py`, **NOT** in Bronze.
Documentation must point to that place.

---

## 3. Source data

Source path:
`/data/raw/source-exports/channel=amazon/dataset=orders/export_date=YYYY-MM-DD/part-*.jsonl`

| Measure | Value |
|---|---|
| Days | 180 days (2026-01-01 → 2026-06-29) |
| Files | 3,316 files |
| Rows | 108,248,563 rows |
| Size | 80.77 GiB |

**At the time of this Bronze benchmark there were only two large Batch sources.** The three Channels TikTok Shop,
Shopify and Shopee were added later with a small JSONL source set and are not part of the
100 GB measurement or the three Spark UI runs here (see
[`silver-five-channels.md`](silver_five_channels.md)). Walmart
lives in `/data/raw/pg-load/walmart.orders/export_date=*/` (JSONL waiting to be loaded into PostgreSQL), with three
fields `purchase_order_id`, `raw_payload`, `fetched_at`; `fetched_at` = `...T23:59:59Z` for the whole
day, so it has 180 distinct values.

Why Walmart's source is PostgreSQL (not JSONL files), and why the `raw_payload` column is declared `JSONB` so key order is rearranged without that being a bug: see `docs/data-platform/reference/channel_sources.md`, section **Trust boundary: Walmart's source is PostgreSQL, not JSONL files**.

**The skew is real, not assumed.** Size per Amazon day:
- smallest 2026-01-01 = **7.3 MB**
- median 2026-03-22 = **495 MB**
- largest 2026-06-29 = **1.64 GB**

The largest day is **224 times** the smallest by **source file size**. This is skew in source size, not skew in task processing time.

**Reading the 224 correctly.** It measures JSONL source file size, not the load on one task. Measured
on the written Iceberg table itself (metadata `raw_amazon_orders.partitions`, 180 days,
108,248,563 rows, 187 files, 8,525,005,070 bytes), the shape is much milder:

| Measure | Number |
|---|---|
| smallest rows/day (2026-01-01) | 8,530 |
| median rows/day | 582,228 |
| largest rows/day (2026-06-29) | 2,173,164 |
| **max / median** | **3.73×** |
| max / min | 254.8× |
| heaviest day accounts for | **2.01%** of total rows |
| 5 heaviest days account for | 7.18% of total rows |
| bytes/day max / median | 3.56× (matches the row measure) |

The number that slows a task down is **3.73**, not 224. Over 180 days, the skew is **mild to moderate**. It shows clearly in
the **evenly spaced 20-day sample** because that sample stretches exactly the range 8,530..2,173,164. Load also **grows over
time** (first 10 days 8,530..400,384; last 5 days 999,062..2,173,164).

**Small files are a READ-side problem.** 1,064 / 3,316 files are smaller than 1 MB. So the tail of tiny files
is a cost at **read** time, not at write time — this decides the choice in section 5e.

---

## 4. Baseline and why AQE is off

The baseline **deliberately disables optimization**:
- `spark.sql.adaptive.enabled=false` (AQE off)
- `spark.sql.shuffle.partitions=200`

**Why AQE is off:** a control configuration is needed to measure AQE's effect in isolation.
If AQE were on from the start, Spark would fix things itself and there would be no "before" picture to compare. AQE must be **an
option that is measured**, not something hidden.

**Principle:** the baseline must be **genuinely slow, but not artificially slow**. Disabling AQE is
a valid configuration (many real clusters still run that way), not added artificial latency.
That is why, when re-enabling AQE turns out **not faster**, the number can be trusted.

---

## 5. Optimization sequence — in the order it actually happened

This section keeps the real order, including the failed step. Step (b) is the most valuable part: an
assumption refuted by measurement, and it is not hidden as a failure.

### a. Initial assumption
"Skew comes from missing optimization → **enabling AQE will fix the skew**." This is the usual expectation, since AQE is
advertised as fixing skew.

### b. Measure — ASSUMPTION REFUTED
Run **the same 20 evenly spaced days** (both small and large days), changing only the AQE flag:

| Run | AQE | Time | Write stage | Tasks | Longest task |
|---|---|---|---|---|---|
| 1 | off | **72.3s** | stage 1 | 200 (only ~20 do work, 180 empty at 0.01s) | **18.7s** |
| 2 | on | **81.7s** | stage 2 | **8** (coalesced by AQE) | **23.6s** |

The result is **the opposite of the expectation**: with AQE on it is **slower** (72.3s → 81.7s) and the slowest task is **worse**
(18.7s → 23.6s). The initial assumption is refuted by measurement.

> **Tone down the TOTAL TIME comparison.** Running **exactly one configuration three times**
> in the same environment gave 55.9 / 63.2 / 64.8s — i.e. **noise of 8.9s on a base of 61.3s = 14.5%**
> (n=3, an order of magnitude only, not a confidence interval). The 72.3 / 81.7 pair differs by 9.4s = **13%**, with
> **n=1 on each side**. The difference is smaller than the noise, so the table above is **not enough** to conclude "AQE makes it slower"
> at the total-time level. Concluding that would require rerunning each side several times — **not done**.
>
> **What is NOT affected by noise and still stands**, because these are counts and distributions rather than
> timings: AQE coalesces **200 tasks → 8 tasks**; only **19/200 tasks have data** (181 empty tasks); the heaviest task
> holds exactly **2,173,164 rows** and **384.5 MiB** of shuffle read — these three numbers are identical across
> the `run1` run and all three control runs. The conclusion in (c) below rests on these numbers, so it
> is not shaken.

### c. Why AQE cannot fix this job's skew
AQE is good at two things: (a) coalescing many small tasks, and (b) **splitting skew in JOINs**. This job **has no
JOIN**. The bottleneck is one task having to write an entire 1.64 GB partition — AQE **does not split writing
one partition across several tasks**. Worse, AQE also packs 2-3 days into one task, so the slowest task
gets heavier (exactly as the "tasks" column shows: 200 → 8).

### d. The real cause — `write.distribution-mode`
The real lever is not AQE but how Iceberg distributes the write:

| Value | How it works | Trade-off |
|---|---|---|
| `hash` (Iceberg 1.5 default for partitioned tables — **in use**) | routes all rows with the same `export_date` to **one** task | 1 file/partition, tidy; but a large day = 1 task holds everything → bottleneck |
| `none` | no shuffle, 114 tasks write whatever they read | parallel writes, no bottleneck; but **many more small files** — directly related to the conclusion "small files are a read-side problem" |

### e. Decision: KEEP `hash` (settled)
The settled deployment decision: **keep `hash`, no rerun**.

**`hash` is an IMPLICIT default, not a value stored on the table.**
`SHOW TBLPROPERTIES lakehouse.bronze.raw_amazon_orders` has **no**
`write.distribution-mode` row (only `write.parquet.compression-codec=zstd`). So "keep `hash`" means
**keeping Iceberg's default for partitioned tables** — there is no configuration line in the repo
to point to. Test tables set via `spark.sql.catalog.lakehouse.table-default.write.distribution-mode`
**do** show the property in `SHOW TBLPROPERTIES`, so that way of setting it does apply.

Reason: at the 180-day scale, skew **is still present** (image `06`: median 33s, max 1.1 min ≈ 2× the
median) **but it does not block the work** — the job still finishes and writes every row. Switching to `none` would
trade a controlled bottleneck for **many more small files**, and small files are already a
read-side problem (1,064/3,316 files < 1 MB, section 3). Keeping `hash` keeps exactly 1 file/partition, in exchange for
accepting that a large day is written by one task. This is a reasoned trade-off, not ignoring the skew.

### f. Three methods tried, and the before/after table

Part (e) settled on keeping `hash` by **argument**. This part measures with **numbers**: three methods, each
method in its own run, the same 20 sample days (`pick_sample_days` is deterministic, so the same data), AQE off,
written to a test table and then dropped. No real table was written to, and the job's defaults were not changed.

**The baseline had to be remeasured, because the environment had changed.** The old 72.3s baseline ran the driver on `spark-master`;
the three methods ran the driver on `worker-1` (`--driver-memory 600m`, because the 512 MB `spark-master` was
OOM killed). So a **control** was run: exactly the baseline configuration in the new environment, **three times**.

| Run | "DONE after" |
|---|---|
| control 1 | 55.9s |
| control 2 | 63.2s |
| control 3 | 64.8s |
| **mean = new baseline** | **61.3s** |
| spread across three identical runs | **8.9s = 14.5%** |

Every comparison below uses **61.3s**. The 72.3s row is still kept in section (b) but must be read with the label "old
environment, driver on `spark-master`".

**Before/after table.** For each method, the test table's `SHOW TBLPROPERTIES` was checked **before
reading the numbers**, to make sure the configuration was really applied to the table — without that check every number could be a
`hash` number.

| | Time | vs baseline 61.3s | Rows in heaviest task | Longest task | Files | Fixes skew? |
|---|---|---|---|---|---|---|
| **control** (`hash`, shuffle 200) | 61.3s | — | 2,173,164 | 11.0-12.8s | 20 | — |
| **(a)** `shuffle.partitions` 200 → 24 | 70.9s | +9.6s, **close to noise → inconclusive** | **2,173,164** | 22.3s | 20 | **NO** |
| **(b)** `distribution-mode=none` | **48.4s** (44.6 / 52.2) | **−12.9s = −21%, REAL DIFFERENCE** | **181,050** | 6.1s | **309** | **YES**, at task level |
| **(c)** `distribution-mode=range` | 79.4s | **+18.1s = +30%, REAL DIFFERENCE** | **2,173,164** | 10.7s | 20 | **NO** |

"REAL DIFFERENCE" means the two ranges **do not overlap**: the slowest (b) run (52.2s) is still faster than the
fastest control run (55.9s). That is the right way to read small n, more robust than comparing mean to mean.

**Why each method came out this way:**
- **(a)** only changes the number of tasks after the shuffle. The distribution key is still `export_date`, so the heaviest task still
  holds **exactly** 2,173,164 rows — not one row changed. It only reduces empty tasks (200 → 24, of which 16
  have data), so the median task rises from 0.01s to 4.18s while the peak does not drop. The assumption stated **before**
  running was "won't fix it", and the measurement confirms it.
- **(b)** removes the shuffle entirely: **one** read-and-write stage remains, 114 tasks, **all 114 have data**,
  shuffle read = 0. Since nothing gathers a day into one task anymore, rows in the heaviest task drop from
  2,173,164 to 181,050 (**12×**).
- **(c)** does not fix it, and the reason is worth remembering: `range` splits on **range boundaries** of the key, but
  the key here is `export_date` and **one day is one value** — it cannot be split further. So it pays
  for two extra sampling stages (13.6s + 24.7s) while max rows/task is still exactly 2,173,164.

**The cost of (b), and the measured part of that cost.** (b) produces **309 files** instead of 20 (**15.5×**),
median file 1.45 MiB instead of 44.5 MiB, files per partition ranging 1..23. So the skew does not disappear;
it **changes form**: from "one task holds everything" to "files per partition are uneven". Section 3 settled that
small files are a **read**-side cost, so that cost was measured too:

`SELECT count(*), sum(length(payload)), count(distinct export_date)` — one query for all three
tables, run **twice** per table in the same Spark session. All three returned identical results (12,627,457 rows /
10,058,940,405 characters / 20 days), so the three tables hold exactly the same data.

| Table | Files | Run 1 | Run 2 |
|---|---|---|---|
| control (`hash`) | 20 | 25.34s | 18.40s |
| (c) `range` | 20 | 24.02s | 15.90s |
| (b) `none` | **309** | 27.86s | 19.10s |

The two tables **with the same 20 files** already differ by 1.3-2.5s, and run 1 is always about 7-8s slower than run 2 on all three
tables (cache effect). The 309-file table is +2.5..3.8s slower (run 1) and +0.7..3.2s slower (run 2) —
**within the noise of the measurement itself**. Honest conclusion: at 20 days / 12.6 million rows, the extra
read cost of 15.5× more files **cannot yet be separated from noise**; the direction is slightly slower,
not enough to assert. Do **not** conclude "309 files cost nothing".

**Three limits of the read measurement, written down so nobody over-reads it:** (1) the query scans the **whole table**,
with no partition filter; (2) filtered queries, the kind where small files usually hurt
most, are **not measured**; (3) **not measured** at 180 days, where the file count would be about 9× larger.

**Decision (e) to keep `hash` is unchanged**, but its rationale is partly weaker: (e) relied on
"switching to `none` would trade for many small files, and small files are a read-side problem", and that read cost
measured at 20 days cannot yet be separated from noise. It has not been measured at 180 days, so there is no basis to change the decision yet.
The numbers are here so that the next review does not have to remeasure from scratch.

**Known limitation, unrelated to the measurements.** `DROP TABLE ... PURGE` prints the Lakekeeper error
`BadRequestException: Table does not exist or user does not have permission to view it at
location ...metadata.json` **after** dropping, repeated across all 7 drops. Each time the table really is gone (it disappears
from `SHOW TABLES` and from MinIO), and `raw_amazon_orders` stays at exactly 187 files / 8,525,005,070 bytes
throughout the whole series of experiments. Purge runs **in the background**: MinIO is clean after about 20-25s. The cause is unknown and
has not been investigated.

---

### g. Remeasured on AWS (EC2 r6i.xlarge 4 vCPU, `s3a://` source)

The same experiment as 5f, run with `PLATFORM_STORAGE=s3 bash scripts/evidence/measure_order_snapshots_bronze.sh
skew_0|skew_b` on EC2 (driver in the Airflow container, Spark 2 workers × 2 cores, reading raw S3 through an IAM role):
20 sample days, AQE off, 12,627,457 rows per run.

| Run | "DONE after" | Files | Mean file size |
|---|---|---|---|
| control `hash` (runs 1/2/3) | 93.8 / 88.7 / 90.7 s → **mean 91.1 s**, noise 5.1 s (5.6%) | 20 | 47.3 MB |
| **(b)** `distribution-mode=none` | **71.6 s** (−19.5 s = **−21.4%**, ~4× noise) | **309** | **3.1 MB** |

Spark UI (EC2 History Server): with `hash`, the write job has **2 stages** — stage 0 reads 9.4 GiB / 114 tasks (44 s) and
shuffles 2.2 GiB; stage 1 receives 2.2 GiB / 200 tasks (31 s) and writes 945.4 MiB. Stage 1 Summary Metrics: **median 2 ms /
0 rows, max 12 s / 2,173,164 rows** — 180 tasks are empty because there are only 20 days, and the task for the busiest day holds 2.17 million
rows (same as the local number). With `none` there is only **1 stage** / 114 tasks (1.0 min): dropping the shuffle makes it faster, but in exchange
each task writes its own file for every day it encounters ⇒ 309 small files.

The conclusion matches local and keeps the decision in (e): **keep `hash`** — being 21% faster at write time does not make up for
15× more small files on every later read (see compaction in [storage.md](../data_storage.md)).
AWS screenshots are embedded in the [AWS section](#on-aws).

## 6. Results at full scale — run 3

`--aqe on --table raw_amazon_orders`, all 180 days,
`run_id=e02f0e9f-3635-499e-a47d-aabc7ff55df9` (19 minutes).

| Measure | Value |
|---|---|
| Time | **1,150.7s** (19 min 11 s) |
| Rows written | **108,248,563** — exactly matches the source total, no rows lost |
| Write stage tasks | **1,002** (before AQE coalescing; AQE coalesces to **69 tasks**, see image 06) |
| Table size on MinIO | `/data/platform/lakehouse` = **10 GB** (initial estimate 17 GB) |
| Compression | **zstd ~10×** (from ~80.77 GiB JSONL down to a 10 GB table) |
| Disk `/` after the run | **25 GB** free (33 GB before → the table takes ~8 GB) |
| Event log | `out/spark-events/app-20261011080203-0001` (5.7 MB, complete) |

New numbers taken from the run 3 screenshots: write stage = **stage 3**, output **7.9 GiB / 108,248,563 rows**,
Duration min 11s / median 33s / **max 1.1 min**. Read stage (stage 1) median **2s** — reading
is not the bottleneck, just as at the 20-day scale.

---

## 7. Spark UI screenshots

The nine screenshots below were taken from the **Spark History Server** (not the port 4040 UI — that UI dies
with the job, so it cannot track a 19-minute job). Source: three event logs in `out/spark-events/`.
Each screenshot comes with an explanation **pointing to the number to read**.

### 7.1. List of the three runs

![History Server home page, showing all 5 apps](../../evidence/spark-ui/00-history-danh-sach-3-lan-chay.png)

This screenshot **identifies the three evidence apps** in the list. The other two apps in the list
are not evidence (one died midway, one broke because MinIO was missing) — see section 8.

### 7.2. Run 1 — AQE off (20 days)

![Run 1 stage list](../../evidence/spark-ui/01-lan1-aqe-off-stages.png)

Run 1 stage list: **stage 1 is the write stage**. This is the stage to inspect to see the skew.

![Run 1 write stage — the clearest skew picture](../../evidence/spark-ui/02-lan1-aqe-off-stage-ghi.png)

**The clearest skew picture.** Stage 1 has **200 tasks**, Duration min **1.0ms** / median **3.0ms** /
max **19s**. 180/200 tasks are empty while a few tasks hold all the work — exactly the shape of skew.

### 7.3. Run 2 — AQE on (20 days)

![Run 2 stage list](../../evidence/spark-ui/03-lan2-aqe-on-stages.png)

Run 2 stage list: **stage 2 is the write stage** (AQE inserts an extra stage compared with run 1).

![Run 2 write stage — AQE coalesces tasks](../../evidence/spark-ui/04-lan2-aqe-on-stage-ghi.png)

**AQE coalescing tasks.** Stage 2 has only **8 tasks**, Duration min **6s** / median **21s** / max
**23s**. Tasks are more even, but the slowest task is **worse** than in run 1 (19s → 23s) — evidence that AQE
cannot fix this job's skew.

### 7.4. Run 3 — 180 days (AQE on)

![Run 3 job list](../../evidence/spark-ui/05-lan3-180ngay-jobs.png)

Run 3 job list: used to read the **total job time** at full scale.

![Run 3 write stage — 69 tasks](../../evidence/spark-ui/06-lan3-180ngay-stage-ghi.png)

Stage 3 = the **write** stage of run 3, **69 tasks** (AQE coalesced from 1,002). Output **7.9 GiB /
108,248,563 rows**. Duration min **11s** / median **33s** / max **1.1 min** — skew remains
but does not block the work.

![Run 3 read stage](../../evidence/spark-ui/07-lan3-180ngay-stage-doc.png)

Stage 1 = the **read** stage of run 3, 1,002 tasks, Input **80.8 GiB / 108,248,563 rows**.
Duration median **2s**, max **11s** → **reading is NOT the bottleneck**.

![All run 3 stages](../../evidence/spark-ui/08-lan3-180ngay-stages.png)

All run 3 stages: **stage 2 is SKIPPED** because AQE replaced it with stage 3 — exactly how AQE
replaces the old plan.

---

## 8. How to rerun

**MinIO MUST be started first.** `spark-master` does not `depends_on` MinIO, so if you start only the 3 Spark
services, MinIO stays down; the job reads the files and finishes computing, then **breaks right at the write** with
`BadRequestException: Malformed request: IO error at s3://platform/lakehouse/...`. If you hit this error,
check `docker ps` to see whether MinIO is running; **do not** go fixing Lakekeeper or S3 permissions.

```bash
cd /path/to/checkout
docker compose up -d minio minio-init                                        # REQUIRED
docker compose --profile spark up -d spark-master spark-worker-1 spark-worker-2
bash scripts/setup/setup_lakekeeper.sh                                             # rerunnable, skips what already exists
```

**Run the job** (the script runs all three runs or one at a time):
```bash
bash scripts/evidence/measure_order_snapshots_bronze.sh run3   # 180 days, AQE on  → real table
bash scripts/evidence/measure_order_snapshots_bronze.sh run1   # 20 days, AQE off  → skew screenshot
bash scripts/evidence/measure_order_snapshots_bronze.sh run2   # 20 days, AQE on   → comparison screenshot
```

**Rerun the skew experiment series from section 5f** (each run writes a test table, then `DROP TABLE
... PURGE` after measuring; do not let two test tables exist at once, each table is ~945 MiB):
```bash
bash scripts/evidence/measure_order_snapshots_bronze.sh skew_0   # control: baseline configuration, new environment
bash scripts/evidence/measure_order_snapshots_bronze.sh skew_a   # shuffle.partitions 24
bash scripts/evidence/measure_order_snapshots_bronze.sh skew_b   # distribution-mode=none
bash scripts/evidence/measure_order_snapshots_bronze.sh skew_c   # distribution-mode=range
```
These four targets are **not** part of `all`, and do **not** write to the two real tables. Each
`spark-submit` leaves ~108 MB of jars in `/opt/spark/work` on **each** worker — run it many times and
that directory grows; it can be deleted since it only holds re-downloadable jars plus `stderr`/`stdout`
(**do not** touch `out/spark-events`, that is the actual evidence).

> **DO NOT run a bare `docker compose --profile spark up -d`.** It also starts the
> default-profile service `generator-batch` and begins regenerating 100 GB into MinIO on `/`. Always name
> the 3 Spark services explicitly.

**Review with the History Server** (the port 4040 Spark UI dies with the job):
```bash
docker run -d --rm --name platform-spark-history -p 18080:18080 --user root \
  -v "$PWD/out/spark-events:/spark-events:ro" \
  -e SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=file:///spark-events" \
  apache/spark:3.5.3-java17 /opt/spark/bin/spark-class \
  org.apache.spark.deploy.history.HistoryServer
# open http://localhost:18080 ; when done: docker stop platform-spark-history
```

The three event logs usable by the History Server are `app-20261010103931-0000` (run 1),
`app-20261010104107-0001` (run 2), `app-20261011080203-0001` (run 3). The two files
`app-20261010104633-0002.inprogress` (died midway) and `app-20261011080057-0000` (broke because
MinIO was missing) **are not evidence** and have been deleted.

---

## 9. Integration into the Airflow pipeline

The job `spark_jobs/bronze/ingest_amazon_order_snapshots.py` is not only run by hand for measurement: the DAG
[`dp1_bronze`](../../../dags/dp1_bronze.py) calls it through `SparkSubmitOperator` in the task
`ingest_amazon`, then calls `spark_jobs/bronze/validate_order_snapshots.py` in `validate_amazon`.
`ingest_amazon → validate_amazon` are the first two tasks of the five-Channel chain. The DAG
passes `--window-days`, `--window-start`, the table name and `--run-id` from Airflow
Variables / the current run, so a Bronze table can be traced back to the exact log of its run.
The tasks share the Airflow Connection `spark_default` (Spark standalone) and the
Variable `platform_spark_packages`, loaded by
[`scripts/setup/setup_airflow.sh`](../../../scripts/setup/setup_airflow.sh). The tables/parameters for the three
new Channels are loaded separately by
[`scripts/setup/setup_dp1_file_channels.sh`](../../../scripts/setup/setup_dp1_file_channels.sh).

| DAG | Main Spark job | Quality gate | Run evidence |
|---|---|---|---|
| DP1 `dp1_bronze` | Amazon/Walmart + three file Channels → Bronze | 5 Validate tasks after 5 Ingest tasks | `manual__2026-10-13T15:30:09+00:00`: **10/10 success**; Amazon source = Bronze **5,827,514**, errors 0 |
| DP2 `dp2_silver` | Bronze → five-Channel Silver → Gold fact + Amazon SCD2 | Validate each Silver, dim and Gold | `manual__2026-10-14T03:53:52+00:00`: **14/14 success**; Gold **10,194,358**, errors 0, duplicate keys 0 |
| DP3 `dp3_features` | Gold fact → table `feat_order_line_return_dp3` | `validate_return_features` | `manual__2026-10-14T04:11:15+00:00`: **2/2 success**; features **10,194,358**, errors 0, duplicate keys 0 |

![Airflow Graph DP1: all five Ingest/Validate pairs succeeded](../../images/airflow/dp1_bronze-graph-five-channels.png)

![Airflow Graph DP2: Silver, Gold and Amazon SCD2 all succeeded](../../images/airflow/dp2_silver-graph-scd2-five-channels.png)

![Airflow Graph DP3: building and checking offline features](../../images/airflow/dp3_features-graph-scd2-five-channels.png)

The Graph shows the order of the Spark tasks and their run status; row counts and check results
are taken from the logs of that exact run. The Grid, logs and per-task explanations are in
[`airflow.md`](../pipeline_orchestration.md). Job → table lineage and PASS assertions in the
DataHub UI are in [`datahub.md`](../data_governance.md).

**Separating measurement scopes:** the three Spark UI runs in sections 5–7 use 20 or 180 days of
Amazon to study skew; the Airflow DP1 run in the table above uses a window of **10 consecutive
Amazon days** to catch duplicates, Walmart from the existing PostgreSQL, and the three new Channels
from small sources. The Airflow task does not pass `--aqe`, so the job uses the default `on`;
do not take the 20/180-day benchmark times as performance measurements of the Airflow run,
and do not treat this DP1 run as having processed the full 100 GB of Batch source.

## 10. Silver proof: schema evolution and dedup in execution 3

The screenshots and REST data below were retrieved from the History Server, using
the run's event log, without rerunning the job:
`app-20261014035356-0002`, app name
`silver-amazon-stg_amazon_order_lines_dp2`. The application has finished, with no
failed jobs. This is the Amazon Silver task in DP2 run
`manual__2026-10-14T03:53:52+00:00`, using the 10-day Amazon window.

![Silver Amazon job list](../../evidence/spark-ui/09-silver-amazon-jobs.png)

![SQL UI: execution 2 has sub-execution 3](../../evidence/spark-ui/10-silver-amazon-sql.png)

Execution **2** is the `AtomicReplaceTableAsSelect` table write command; the data
processing is in sub-execution **3**. That is why the earlier screenshot `11` of execution 2
is not used to illustrate parsing or dedup.

![Execution 3: two source branches through Union, then WindowGroupLimit and Window](../../evidence/spark-ui/12-silver-amazon-dedup-plan.png)

The graph shows two `BatchScan` branches over the same Bronze table, each filtering and parsing its own
schema, `Generate` exploding the Order Line array, then `Union`. Spark then sorts/shuffles
by key, runs `WindowGroupLimit`/`Window` and filters to the row with `row_number=1`.
The plan Details show `from_json` with `AmazonOrderId`/`OrderItems` for v0
and `orderId`/`orderItems` for the 2026 schema, along with the status/currency mapping into
the Silver column set. v0 fields that no longer exist in the new schema are kept NULL by design.

![Execution 3 Details: schema parsing and source fields](../../evidence/spark-ui/13-silver-amazon-schema-details.png)

[REST execution 3](../../evidence/spark-ui/silver-amazon-execution-3.json) stores
`status=COMPLETED`, `planDescription` and the metrics of this exact run:

| Execution 3 measure | Rows |
|---|---:|
| Order Lines from the v0 branch (`Generate`, node 19) | 4,222,964 |
| Order Lines from the 2026 branch (`Generate`, node 27) | 4,226,338 |
| Before dedup (`WindowGroupLimit`, node 13) | 8,449,302 |
| After dedup (`Filter`, node 6) | 8,314,312 |
| **Duplicates removed** | **134,990 (1.5976%)** |

So the before/after counts are reconciled directly on the run, in addition to
the historical measurement in [Silver section 10.6](silver.md).
The 1.5976% rate belongs to the 10-day window and is not directly comparable with the generator's
full-period 1.98%, because duplicate pairs near the window edge may fall outside the loaded source.

Regenerate the screenshots with `scripts/evidence/capture_spark_silver_evidence.py` while the History Server
is reading `out/spark-events`. The script checks that the execution has completed and contains both
key names, `from_json`, `Union` and `row_number` before saving the proof. Capturing
does not need a Spark worker or rereading the source data.

## On AWS

The skew experiment ran Compose on EC2 `r6i.xlarge`, two Spark workers × two cores, S3 source;
20 Amazon days, AQE off. [Section 5g](#g-remeasured-on-aws-ec2-r6ixlarge-4-vcpu-s3a-source)
and the [receipt](../../evidence/aws/data-platform-aws-20261027.json) (`spark_skew_aws`) hold the measurements.
Code: [measurement script](../../../scripts/evidence/measure_order_snapshots_bronze.sh),
[ingest job](../../../spark_jobs/bronze/ingest_amazon_order_snapshots.py).

The `hash` baseline UI has 314 tasks across two stages. The write stage has a median of 2 ms/0 rows but a max of 12 s/2,173,164 rows:
most hash partitions are empty, while a few tasks carry the data-heavy days. This is skew by write date, not evidence
of SKU skew in a join. Dropping hash distribution with `write.distribution-mode=none` removes the shuffle, leaving 114 tasks/one stage.
Time drops from a mean of 91.1 to 71.6 seconds; files grow 20 → 309. `hash` is kept to avoid many small files;
the spec change for small Channels is measured separately in [Storage AWS](../data_storage.md#on-aws).

![EC2 terminal: three skew_0 runs take 93.8, 88.7, 90.7 seconds and skew_b takes 71.6 seconds; each run writes 12,627,457 rows](../../evidence/aws/spark-skew-runs-20261029.png)

![Spark Jobs, hash control: createOrReplace has 2 stages and 314 successful tasks](../../evidence/aws/spark-ui-skew0-jobs-20261029.png)

![Spark Stages, hash: stage 0 has 114 tasks reading 9.4 GiB, stage 1 has 200 tasks receiving 2.2 GiB of shuffle](../../evidence/aws/spark-ui-skew0-stages-20261029.png)

![Spark stage 1: median 2 ms and output 0 rows; max 12 seconds and output 2,173,164 rows](../../evidence/aws/spark-ui-skew0-stage1-summary-20261029.png)

![Spark Jobs, none: createOrReplace completes one stage, 114 tasks, duration 1.0 min](../../evidence/aws/spark-ui-skewb-jobs-20261029.png)

Integration: [dp1_bronze](../../../dags/dp1_bronze.py) submits ingest, then validate, for each source;
[dp2_silver](../../../dags/dp2_silver.py) submits normalization/dedup, then Gold/SCD2;
[dp3_features](../../../dags/dp3_features.py) submits feature build/validate.
[Airflow AWS](../pipeline_orchestration.md#on-aws) embeds the Graph/Gantt/logs and explains each component.
The Master screenshot below belongs to DP1; the skew experiment above ran separately via the script,
and the benchmark's run_id is not attributed to a DAG run.

![Spark Master: two workers ALIVE, two cores each; the dp1-validate-amazon application of user airflow is RUNNING](../../evidence/aws/spark-master-2x2core-20261027.png)

![Airflow DP1: list of 10 green tasks and the ingest-to-validate edges in the Graph](../../evidence/aws/dp1-bronze-graph-20261027.png)

DP2 on AWS has validated Silver/Gold with no errors ([Silver AWS](silver.md#on-aws)).
There is not yet an AWS Spark UI screenshot showing the Union parsing of the old/new schemas and the Window dedup like the local execution in section 10;
the PASS log does not replace that capture for schema evolution and dedup evidence.
