# Debezium CDC for the Walmart source

The source `walmart.orders` simulates the database of another department. DP1 currently
reads this table in batches. Debezium reads the PostgreSQL WAL and writes each committed
change to the Kafka topic `platform_cdc.walmart.orders` for the CDC Bronze stage to process.

## Configuration

- PostgreSQL runs with `wal_level=logical`; the table `walmart.orders` uses
  `REPLICA IDENTITY FULL` because it has no primary key, so that update/delete events carry
  the old data.
- The publication includes only `walmart.orders`; the connector uses the slot
  `platform_walmart_orders` and `pgoutput`.
- The connector uses `snapshot.mode=no_data`: historical data already went through DP1 Batch,
  and this branch only receives changes made after registration. To run CDC from
  scratch on a new source, choose a different snapshot mode or load history separately.
- The topic has one partition to preserve the change order of the demo record; the source table
  has no primary key, so the default Kafka key may be empty.
- Kafka Connect keeps configuration and offsets in three dedicated internal topics. The REST port
  from the host defaults to 8084.

Designed following the [PostgreSQL connector guide](https://debezium.io/documentation/reference/3.4/connectors/postgresql.html)
and [Kafka Connect in Docker](https://debezium.io/documentation/reference/3.4/tutorial.html).

## Running

```bash
env -u POSTGRES_PASSWORD docker compose --profile cdc up -d debezium-connect kafka-ui
.venv/bin/python scripts/data/setup_order_snapshots_debezium.py --env-file .env
.venv/bin/python scripts/evidence/measure_order_snapshots_cdc.py --env-file .env
```

`scripts/data/setup_order_snapshots_debezium.py` checks the WAL, creates the publication/topic and registers the connector.
`scripts/evidence/measure_order_snapshots_cdc.py` temporarily copies one source order with its own ID, commits three changes
**insert → update → delete**, waits for each event to appear in Kafka and writes the
commit → Kafka time to `out/cdc/probe-*.json`. After the delete, the source table returns to
its original row count.

Once the topic has events, the always-on Flink job `lakehouse-bronze-streams`
([`flink_jobs/lakehouse/bronze_streams.sql`](../../../flink_jobs/lakehouse/bronze_streams.sql)) writes every non-null
change event into `raw_walmart_cdc_v2`, with the same columns as before. Kafka offsets and Iceberg commits both ride on
the checkpoint (every minute), and a redeploy stops the job with a savepoint and resumes from it
([`run_bronze_stream_jobs.py`](../../../scripts/data/run_bronze_stream_jobs.py)), so no offset is read twice or
skipped. `source_epoch` is still the cluster ID/topic UUID. The first task of `dp_nightly`,
`check_walmart_cdc_complete` ([`validate_walmart_order_changes.py`](../../../spark_jobs/cdc/validate_walmart_order_changes.py)),
captures Kafka's end offsets, waits up to 10 minutes for Flink to commit that far, then checks missing/extra/duplicate
rows, payload, change type, order ID and LSN; a failure stops the chain before Silver.

**Before (until this change):** `dags/dp1_cdc.py` ran `spark_jobs/cdc/ingest_walmart_order_changes.py` (permalinks in
[CDC v2 results](#cdc-v2-results)) every hour and the validator. The ingest read Kafka over an offset range frozen once
per `run_id`, stored under `/opt/airflow/platform-control/cdc` in the Airflow driver volume. Both tasks used
the same boundary, partition set and start-inclusive/end-exclusive offsets;
a retry did not re-pin latest. V2 records `(source_epoch, topic, partition, offset)`,
where the epoch is the cluster ID/topic UUID, and uses `read_committed`. The validator read
exactly that range and checked missing/extra/duplicate rows, payload, change type, order ID and LSN.

By default the updated setup selects the new **`raw_walmart_cdc_v2`** table;
a legacy table without an epoch is rejected, with no automatic migration and no overwrite of the old proof.
The results below were produced by that Spark path.

The helper has unit regression tests, and **one successful
Kafka–Spark–Iceberg v2 pass** has been run (section [CDC v2 results](#cdc-v2-results)).
That pass covers only the normal path. A retry between the two tasks, a topic being
recreated/reset, retention truncating offsets and a producer writing more while reading
are **not yet tested**. Checking the UUID before/after is not a broker lock: admins must not
recreate/reset the topic while a task is reading/writing. The branch is currently local PLAINTEXT,
not a secured Kafka adapter for the cloud. The results from the "Run results" section
onward are the **legacy** proof of the `raw_walmart_cdc` table.

## CDC v2 results

Rerun with the Spark code of that time (jobs `spark_jobs/cdc/ingest_walmart_order_changes.py`, `spark_jobs/cdc/validate_walmart_order_changes.py`, DAG `dp1_cdc`, setup selecting
`raw_walmart_cdc_v2`). Kafka was recreated because the old container had been removed while freeing
disk space, so Debezium re-registered the connector (`snapshot.mode=no_data`) and then
`scripts/evidence/measure_order_snapshots_cdc.py` created three changes `CDC-DEMO-24f0630c0caa`
([probe](../../evidence/cdc-probe-CDC-DEMO-24f0630c0caa.json)).

| Offset | Change | Order |
|---:|---|---|
| 0 | Delete | `CDC-DEMO-8383d4cfe5a7` (probe) |
| 1 | Insert | `CDC-DEMO-24f0630c0caa` |
| 2 | Update | `CDC-DEMO-24f0630c0caa` |
| 3 | Delete | `CDC-DEMO-24f0630c0caa` |

Offset 0 is the delete event of the old probe. The old Kafka was lost, but the replication slot
`platform_walmart_orders` in PostgreSQL still held the unacknowledged change,
so Debezium replayed it. No event was lost when the broker was lost.

Run `manual__2026-10-18T05:14:10+00:00` succeeded for both tasks (about 1.5
minutes). Validate log:

```text
Kafka=4, Bronze=4, unique offsets=4, missing=0, extra=0, payload mismatch=0, invalid=0
Validate CDC Bronze: PASS
```

Ingest and Validate share one boundary JSON in
`/opt/airflow/platform-control/cdc` (volume `platform_airflow-data`); Validate reads only
Bronze rows with the exact `source_epoch` (cluster ID/topic UUID) that fall within the frozen
offset range. The manifest is checked in
[`datahub-validation-v2.json`](../../evidence/datahub-validation-v2.json); lineage and
Quality on DataHub are in [DataHub](../data_governance.md#rerun-evidence).

```bash
env -u POSTGRES_PASSWORD docker compose --profile spark up -d spark-master spark-worker-1 spark-worker-2
env -u POSTGRES_PASSWORD docker compose --profile airflow up -d airflow
env -u POSTGRES_PASSWORD python3 -m scripts.data.setup_airflow --stage dp1-cdc
env -u POSTGRES_PASSWORD docker compose --profile airflow exec -T airflow airflow dags trigger dp1_cdc
```

With the current code the last line is replaced by deploying the stream job once and running the gate:

```bash
docker compose build flink-jobmanager      # adds the Iceberg Flink runtime
env -u POSTGRES_PASSWORD docker compose --profile stream --profile flink up -d --wait kafka flink-jobmanager flink-taskmanager
python3 -m scripts.data.run_bronze_stream_jobs
env -u POSTGRES_PASSWORD docker compose --profile airflow exec -T airflow airflow tasks test dp_nightly check_walmart_cdc_complete "$(date -u +%F)"
```

Evidence for **commit → Bronze** needs the completion time of the ingest task for the same
probe; that time is not inferred from the commit → Kafka measurement.

## Run results

Connector `platform-walmart-orders` and task 0 are in state `RUNNING` on Debezium
3.4.3.Final. Probe `CDC-DEMO-783bee6f9cad` produced exactly three events:

| Change | Kafka offset | Commit → Kafka |
|---|---:|---:|
| Insert | 0 | 583.2 ms |
| Update | 1 | 451.6 ms |
| Delete | 2 | 608.7 ms |

The raw file `out/cdc/probe-CDC-DEMO-783bee6f9cad.json` records the commit time, offset,
LSN and the `before`/`after` state of each event. The probe reports no sample rows left
in PostgreSQL after the delete.

The first Airflow run (`manual__2026-10-13T22:30:58+00:00`) had both Ingest and Validate
succeed. Validate log: **Kafka 3 = Bronze 3**, unique offsets 3, missing 0, errors 0.

To measure once the services were ready, a second probe
`CDC-DEMO-6647c5e27b8c` created three more changes at offsets 3–5. Airflow run
`manual__2026-10-13T22:34:22+00:00` loaded more data into
`lakehouse.bronze.raw_walmart_cdc`; the Validate log records **Kafka 6 = Bronze 6**,
unique offsets 6, missing 0, errors 0. Both runs are captured in the UI:

![Airflow Grid: both CDC runs succeeded](../../images/airflow/dp1_cdc-grid-two-runs.png)

![Airflow Graph: ingest Walmart CDC, then validate](../../images/airflow/dp1_cdc-graph-two-runs.png)

The second run's Ingest task finished at **22:34:47.559 UTC**. Subtracting from that time
the time each `COMMIT` returned in the probe file gives the upper bound of the
**commit → Bronze** latency:

| Change | Offset | Commit → Kafka | Commit → Bronze, upper bound |
|---|---:|---:|---:|
| Insert | 3 | 623.3 ms | 44.380 seconds |
| Update | 4 | 466.6 ms | 35.178 seconds |
| Delete | 5 | 554.3 ms | 31.261 seconds |

The upper bound uses the time the task reported success; the Iceberg snapshot may have been
committed earlier. The measurement includes the **manual DAG trigger** time (6–19 seconds after
the commits) and Spark startup. This is a Batch configuration on a development machine, not
the latency of Debezium alone or of a continuously running CDC service.

After the probe finished, `SELECT count(*) FROM walmart.orders` returned **1,293,525**.
A Batch pass reading the whole table to detect three changes would have to examine this many rows;
CDC emits exactly three new messages. The once-a-day Batch schedule is a policy of
the problem setup, so the 0–24 hour wait comes from the schedule, not from a SQL speed limit.

## Confirmation in kafka-ui (rerun)

The `kafka-ui` service (image `kafbat/kafka-ui:v1.5.0`, profile `cdc`, port 8085)
shows the topic Debezium writes to directly, instead of only reading offset numbers with the CLI:

```bash
env -u POSTGRES_PASSWORD docker compose --profile cdc up -d postgres debezium-connect kafka-ui
.venv/bin/python scripts/data/setup_order_snapshots_debezium.py --env-file .env
.venv/bin/python scripts/evidence/measure_order_snapshots_cdc.py --env-file .env
```

The topic list shows `platform_cdc.walmart.orders` with 1 partition and **4 messages**
(3 KB), next to the generator's two Stream topics:

![kafka-ui: topic list, platform_cdc.walmart.orders has 4 messages](../../images/cdc/kafka-ui-cdc-topics.png)

The topic's Messages tab shows exactly four events at offsets 0–3; each preview is a
Debezium JSON with `before`/`after` and `purchase_order_id`:

![kafka-ui: four CDC events at offsets 0–3 in the topic platform_cdc.walmart.orders](../../images/cdc/kafka-ui-cdc-messages.png)

The new probe `CDC-DEMO-8383d4cfe5a7` created three changes at offsets 1–3:

| Change | Offset | Commit → Kafka | Note |
|---|---:|---:|---|
| Insert | 1 | 14,529.9 ms | first pass after the connector started, still warming up |
| Update | 2 | 300.1 ms | |
| Delete | 3 | 492.0 ms | |

Offset 0 is the **delete** event of the old probe `CDC-DEMO-6647c5e27b8c`, replayed:
the logical slot `platform_walmart_orders` lives in PostgreSQL, so when Debezium restarts
it continues reading the unacknowledged WAL. This is the correct behavior of slot-based CDC, and
also the reason the probe always records the real offset instead of assuming it starts at 0.

The connector and task 0 are both `RUNNING` on Debezium 3.4.3.Final, stored in
[`docs/evidence/debezium-connector-status.json`](../../evidence/debezium-connector-status.json).

## On AWS

Debezium 3.4.3, PostgreSQL, Kafka, Airflow and Spark run with Compose on EC2 in Sydney; Bronze Iceberg writes to S3.
Idea: capture the WAL to keep create/update/delete after the batch load, instead of rescanning the whole source table.
[DAG dp1_cdc](https://github.com/Babyfat012/multi-channel-data-ai-platform/blob/f32c8181ceb310ee2443b30ba83e91158fe93ac3/dags/dp1_cdc.py) run `manual__2026-10-28T10:51:33.422898+00:00`:
[ingest_walmart_cdc](https://github.com/Babyfat012/multi-channel-data-ai-platform/blob/f32c8181ceb310ee2443b30ba83e91158fe93ac3/spark_jobs/cdc/ingest_walmart_order_changes.py) freezes the offsets and writes the envelope/provenance;
[validate_walmart_cdc](../../../spark_jobs/cdc/validate_walmart_order_changes.py) compares Kafka/Bronze within the exact source epoch.
Connector configuration: [setup_order_snapshots_debezium.py](../../../scripts/data/setup_order_snapshots_debezium.py).

The [AWS probe](../../evidence/aws/cdc-probe-20261028.json) `CDC-DEMO-fb4b96588a6d` records topic
`platform_cdc.walmart.orders`, partition 0, offsets 0/1/2 for `c/u/d` respectively.
Commit → Kafka was 730.7 / 452.1 / 295.3 ms respectively; create has after, update has before/after, delete has before.
After the delete the source has 0 probe rows left. These numbers are not used as commit → Bronze latency.
The validator records Kafka = Bronze = unique offsets 3; missing, extra, bad payload and errors are all 0.

![Airflow validate_walmart_cdc: Kafka 3, Bronze 3, unique offsets 3; missing, extra, bad payload and errors all 0](../../evidence/aws/dp1-cdc-validate-20261028.png)

![DataHub CDC: Kafka orders through ingest_walmart_cdc to raw_walmart_cdc_v2 and validate_walmart_cdc](../../evidence/aws/datahub/dp1-cdc-lineage.png)

![DataHub Quality raw_walmart_cdc_v2: assertion dp1_cdc.validate_walmart_cdc has a green check](../../evidence/aws/datahub/dp1-cdc-validation.png)

The lineage shows the topic flowing through the ingest task to `bronze.raw_walmart_cdc_v2`; Quality records the validator assertion as green.
The [AWS manifest](../../evidence/aws/datahub-validation-aws-20261029.json) keeps the same run_id and metrics.
This is a three-event probe, not a CDC load test or proof of automatic Silver/Gold updates.
