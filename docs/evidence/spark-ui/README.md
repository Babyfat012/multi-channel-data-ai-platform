# Spark UI evidence — Bronze and Silver

## Silver addition on 2026-10-15

App `app-20261014035356-0002` belongs to the Amazon Silver task in the DP2 run of
2026-10-14 and completed with no failed jobs. Execution **3** is the actual
processing; execution **2** is only the outer table write command.

| Artifact | Content and how to read it |
|---|---|
| `09-silver-amazon-jobs.png` | Jobs of the Silver application that ran |
| `10-silver-amazon-sql.png` | SQL execution 2 has sub-execution 3 |
| `11-silver-amazon-query-plan.png` | Old screenshot of execution 2; shows only `AtomicReplaceTableAsSelect`, not used as parse/dedup proof |
| `12-silver-amazon-dedup-plan.png` | Execution 3 graph: two schema branches → Union → window dedup; 8,449,302 → 8,314,312 rows |
| `13-silver-amazon-schema-details.png` | Plan Details with source schema parsing |
| `silver-amazon-execution-3.json` | Completed REST execution, physical plan and metrics of that exact run |

The screenshots are embedded and explained in [Spark section 10](../../data-platform/reference/spark.md). The script
`scripts/evidence/capture_spark_silver_evidence.py` captures the real History UI and saves the REST
response, without rerunning the job.

## Historical Bronze evidence

Screenshots taken on 2026-10-11 from the **Spark History Server**, not the port 4040 UI (that UI dies with the
job, so it cannot capture a 19-minute job). Source: three event logs in `out/spark-events/`.

Restart the History Server to view them yourself:
```bash
cd <repo-root>
docker run -d --rm --name platform-spark-history -p 18080:18080 --user root \
  -v "$PWD/out/spark-events:/spark-events:ro" \
  -e SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=file:///spark-events" \
  apache/spark:3.5.3-java17 /opt/spark/bin/spark-class \
  org.apache.spark.deploy.history.HistoryServer
# then open http://localhost:18080 ; when done: docker stop platform-spark-history
```

| Run | App ID | Configuration | Time |
|---|---|---|---|
| 1 | `app-20261010103931-0000` | 20 days, AQE **off** | 75.8s |
| 2 | `app-20261010104107-0001` | 20 days, AQE **on** | 86.6s |
| 3 | `app-20261011080203-0001` | 180 days, AQE **on** | 1,153.8s (19 min 11 s) |

The other two apps in the History Server **are not evidence**; do not use them:
`app-20261010104633-0002` (run 3 that died midway on Oct 10) and `app-20261011080057-0000`
(broke after 14s because MinIO was not started).

## What each screenshot shows

| Screenshot | Content | Number to point at |
|---|---|---|
| `00-history-danh-sach-3-lan-chay.png` | History Server home page, showing all 5 apps | identifies the 3 evidence apps |
| `01-lan1-aqe-off-stages.png` | Stage list, run 1 | stage 1 is the write stage |
| `02-lan1-aqe-off-stage-ghi.png` | **The clearest skew picture.** Stage 1, 200 tasks | Duration: Min **1.0ms** / Median **3.0ms** / Max **19s**. 180/200 tasks empty, a few tasks hold all the work |
| `03-lan2-aqe-on-stages.png` | Stage list, run 2 | stage 2 is the write stage |
| `04-lan2-aqe-on-stage-ghi.png` | **AQE coalescing tasks.** Stage 2, down to **8 tasks** | Duration: Min **6s** / Median **21s** / Max **23s**. Tasks are more even but the slowest task is **worse** than run 1 (19s → 23s) |
| `05-lan3-180ngay-jobs.png` | Run 3 job list | total job time |
| `06-lan3-180ngay-stage-ghi.png` | Stage 3 = the **write** stage of run 3, **69 tasks** (AQE coalesced from 1,002) | Output **7.9 GiB / 108,248,563 rows**. Duration: Min **11s** / Median **33s** / Max **1.1 min** |
| `07-lan3-180ngay-stage-doc.png` | Stage 1 = the **read** stage of run 3, 1,002 tasks | Input **80.8 GiB / 108,248,563 rows**. Duration Median **2s**, Max **11s** → reading is NOT the bottleneck |
| `08-lan3-180ngay-stages.png` | All run 3 stages | stage 2 is SKIPPED because AQE replaced it with stage 3 |

## Three-screenshot line of reasoning (finding and fixing the performance problem)

1. Screenshot `02`: AQE off → tasks are extremely uneven (3ms vs 19s). The skew is real.
2. Screenshot `04`: AQE on → tasks are more even, **but slower** (75.8s → 86.6s) and the slowest task is
   **worse** (19s → 23s). **The initial assumption is refuted by measurement.**
3. The real cause: this job **has no JOIN**, and AQE only splits skew in JOINs. The bottleneck
   is `write.distribution-mode=hash` routing each `export_date` to one task — AQE cannot split
   writing one partition across several tasks.
4. Screenshot `06`: at the 180-day scale, skew remains (median 33s, max 1.1 min = 2×) but
   does not block the work. Decision on 2026-10-11: **keep `hash`** (reasoning and follow-up measurements in [Spark section 5](../../data-platform/reference/spark.md)).
