<!-- Describes the design, operation and limits of CDC on the Walmart v2 source; no service measurements yet. -->
# Walmart CDC v2 → Bronze with an oracle

> **Pipeline v2 (order lifecycle) — designed and tested locally; not the pipeline deployed on AWS.** The pipeline running on AWS is v1 (DP1 → DP4 + ML); see [Data Platform evidence](../evidence.md). Mentions of GCP below are an old plan that was never run; the current cloud target is [AWS](../../shared/aws_deploy.md).

This branch is for `walmart_lifecycle.orders`, fully separate from CDC v1 on `walmart.orders`.
The name “v2” in `docs/data-platform/reference/cdc.md` previously referred only to the boundary/epoch version of the v1 branch;
it is not the `sim-native-v2` source CDC described in this document.

| Component | Value |
|---|---|
| Connector | `platform-walmart-order-lifecycle` |
| Publication and replication slot | `platform_walmart_order_lifecycle_orders` |
| Topic (one partition) | `platform_cdc_order_lifecycle.walmart_lifecycle.orders` |
| Bronze (Iceberg) | `lakehouse.bronze.lifecycle_raw_walmart_cdc_events` |
| Contract | `sim-native-v2/walmart/1` |
| DAG | `dp_order_lifecycle_cdc`, paused, manual trigger |

The real DDL in `scripts/setup/load_order_lifecycle_walmart.py` has **PRIMARY KEY
(dataset_version, purchase_order_id)**. Setup checks for exactly this key and then selects
`REPLICA IDENTITY DEFAULT`: UPDATE/DELETE keep the old key, so FULL is not needed as it was for
v1, which has no PK. `raw_payload` is TEXT. With DEFAULT, the before image of an update/delete
may contain only the key, so `payload_before` may be null; a full old image is not promised.
The after image of insert/update keeps the TEXT as is, without re-canonicalizing in Spark.
`snapshot.mode=no_data` because history was already bootstrapped over JDBC by the job `spark_jobs/bronze/ingest_order_lifecycle.py`; the CDC branch
only picks up commits after the connector/slot is ready. The gap
before connector registration is not backfilled automatically. Do not reload the fixture through the loader while measuring: the loader
deletes/inserts the dataset, which creates additional real CDC events, and the validator will report extra.

## Oracle and reconciliation

`scripts/run/mutate_order_lifecycle_cdc.py` checks that the connector and task are RUNNING and the slot is active, picks a sample
in a deterministic order and copies it to a probe key derived from the seed. Each operation is a
separately committed transaction: insert (Created), update raw_payload (Shipped), delete.
Bootstrap orders are not modified. C/u are checked with `Registry` and `validate_record` from
`spark_jobs.lib.data_contracts`; the payload has the full line, charge, SKU and quantity structure.
On normal completion the source returns to its original row count.

The default oracle `order_lifecycle_cdc_expected.json` records op `c/u/d`, the composite key,
`after_sha256`, `commit_order`, raw_payload and the time COMMIT returned. The hash is
SHA256 over the exact UTF-8 bytes of the after TEXT; **delete has a null after and a null hash**.
The oracle is independent of Kafka/Bronze. The plan content is reproducible from the seed and the bootstrap sample;
commit times are real measurements and so are not reproducible. The oracle is written atomically after each
commit; it is `complete=true` only after all three commits. If the process fails between a commit and
writing the oracle, the validator rejects the incomplete file. Successful commits are not
rolled back automatically and the old oracle is not overwritten automatically; on error the operator needs to check the source and choose
a new isolated run.

The job `spark_jobs/cdc/ingest_order_lifecycle_changes.py` uses the existing helper `spark_jobs/lib/cdc_offsets.py`: it pins the cluster ID, topic UUID,
partition set and start-inclusive/end-exclusive offsets before reading; stores the boundary checksum
atomically in a POSIX control directory and copies the boundary into the receipt. Kafka is read with
`read_committed`, `failOnDataLoss=true`; a retry keeps the old end offset. Incarnation
and retention are checked before/after reading and after writing. Do not recreate/reset the topic while the job is running;
this is not a broker lock. By default the bounded range runs from the earliest retained offset to the latest committed
offset at start time. A proof run needs a new/isolated topic containing only this oracle;
on a topic that already has history, the oracle must describe that entire range, otherwise the validator
will report extra. Events outside the probe are not skipped automatically.

Bronze columns: `op`, `dataset_version`, `purchase_order_id`, `payload_before`,
`payload_after`, `source_lsn`, `source_ts` (epoch milliseconds of the source commit),
`topic`, `kafka_partition`, `kafka_offset`, `source_epoch` (cluster/topic UUID),
`run_id`, `record_sha256`, `event_json`. Error/null messages are not silently filtered out.
The ingest receipt stores the boundary before the append, the completion status and the Kafka row count.

Bronze **appends**; it does not MERGE into `raw_source_records`: this keeps the history of changes
and the offset evidence, at the cost that Silver does not yet receive the new CDC state. Handling
snapshot + CDC, delete/tombstone and a consistent cutoff is the next decision.
If the same `run_id` already has rows, the job `spark_jobs/cdc/ingest_order_lifecycle_changes.py` skips the append on retry, and then the job `spark_jobs/cdc/validate_order_lifecycle_changes.py` checks the whole
run; the Iceberg append is atomic. The DAG limits concurrency to one run/task. Two
drivers writing the same run concurrently are not supported; a new run that appends the old range again creates a separate
proof, not a global incremental ingest.

The job `spark_jobs/cdc/validate_order_lifecycle_changes.py` reads all rows of the same run, without filtering out wrong epochs before comparing. It reconciles
the multiset by composite key/op and then the after-payload hash (recomputed from the TEXT, also checking the
hash column); it checks op order by Kafka offset within the same key, missing, extra,
payload_mismatch, order_mismatch and epoch_mismatch. It also catches duplicate offsets,
invalid rows and rows outside the boundary. The JSON receipt has checks, row counts, boundary,
epoch and completion time, and the exit code is non-zero on a violation. The validator uses the frozen
boundary in the receipt/control store and does not need Kafka to be online. Because it collects
the oracle and the run to the driver, this proof branch is meant for a controlled number of mutations;
it is not yet distributed large-scale CDC reconciliation.

## Manual run and DAG

Do not use these commands on running services unless the operator has chosen to.
Configure `.env` as for the existing loader; setup/mutate also accept
`POSTGRES_HOST`, `POSTGRES_CONNECT_HOST` (DNS from Connect to the DB),
`KAFKA_BOOTSTRAP_SERVERS` and `DEBEZIUM_URL`. By default setup uses host
`127.0.0.1` to call the DB/Kafka/REST, and the connector reaches PostgreSQL through the DNS name `postgres`.

```bash
.venv/bin/python scripts/setup/load_order_lifecycle_walmart.py --root <fixture> --env-file <env>
.venv/bin/python scripts/setup/setup_order_lifecycle_debezium.py --env-file <env>
# Wait for the connector/task to be RUNNING and the slot to be active (the harness polls this itself).
.venv/bin/python scripts/run/mutate_order_lifecycle_cdc.py --env-file <env> --seed 11 \
  --output <control-dir>/order_lifecycle_cdc_expected.json
# Wait until Kafka has all three events and pin the range afterwards, or use the harness below.
```

The DAG only runs `ingest_cdc` → `validate_cdc` and never mutates the source itself.
`scripts/setup/setup_airflow.sh` adds the Variables `platform_order_lifecycle_cdc_control_dir`,
`platform_order_lifecycle_cdc_oracle` and `platform_order_lifecycle_cdc_spark_packages`. RAM/cores reuse
`platform_order_lifecycle_driver_memory`, `platform_order_lifecycle_executor_memory` and `platform_order_lifecycle_executor_cores`.
The subclass adds all three to `template_fields`. The new script/helper and the
oracle/control directory must be mounted at the same path across drivers, with write permission for receipts,
and the Connection `platform_kafka` must be configured. The v2 topic is fixed by the job and does not use the
v1 topic Variable. After mutating, poll until all events are present before triggering the DAG; triggering
too early can pin an incomplete range, which needs a new run once the source has caught up.

## Harness for the operator to run

```bash
bash tests/harness/run_order_lifecycle_cdc.sh
```

It does not run Compose. The harness creates a dedicated Docker network and temporary containers
`postgres:15` (`wal_level=logical`), `apache/kafka:3.9.1`,
`quay.io/debezium/connect:3.4` and `platform-airflow:2.10.5-spark3.5.3`.
All images must already be present (`--pull never`); no dependencies are added. The host Python
`.venv/bin/python` needs the existing psycopg and confluent_kafka. The cache volume
`platform_airflow-data` must exist. The harness runs `ls /ivy/.ivy2/jars`, and stops and names the
missing jar if the cache lacks any of the following:

- `org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.11.0.jar`
- `org.apache.spark_spark-sql-kafka-0-10_2.12-3.5.3.jar`
- `org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.5.3.jar`
- `org.apache.kafka_kafka-clients-*.jar`
- `org.apache.commons_commons-pool2-*.jar`

Spark uses the jars in the cache and does not use `--packages` to download from Maven. The local Hadoop
warehouse is in `/tmp/platform-cdc-v2.*/warehouse` on the host (mounted as `/proof`
in the container), with no Lakekeeper/S3 needed. The harness generates a new fixture with the
producer and loads it through the loader. Readiness polling has a timeout; it waits for the connector and slot,
mutates, polls messages/offsets and compares against the oracle until complete, saves `poll.json`, and runs jobs
26 + 27. After the clean pass, it appends exactly one fake record to Bronze: the following validator pass
must exit non-zero and the receipt must have `extra=1`. A `trap` removes exactly its own containers/network
and keeps the `/tmp` directory and receipts for the operator. The harness services were not run
during implementation because the task constraints prohibited starting/stopping Docker services.

## GCP proposal — operator decides

`docker-compose.gcp.yml` does not yet have Kafka + Debezium. Proposal: a **new overlay**
in a later step: Kafka 3.9.1 as a single KRaft broker on an internal network with its own volume;
Connect 3.4 on the same network, with config/offset/status topics at replication factor 1;
PostgreSQL logical WAL, publication/slot and replication permissions; durable storage for the oracle and
control directory alongside the Airflow driver. Do not expose PLAINTEXT/REST to the
Internet. Configure correct listener DNS from the executors and Connect, limit the slot's WAL
and monitor backlog when Connect stops.

Budget estimate for the operator to try: Kafka heap 768 MiB, container 1.5–2 GiB;
Connect heap 768 MiB, container 1.25–1.5 GiB; in total about 3–4 GiB on the VM
alongside PostgreSQL and Spark. Spark per the existing Variables: driver 3g, executor 4g,
2 cores; the total must be computed from the number of executors, without assuming it already fits the VM. This is
an unbenchmarked proposal, with no production security configuration and no Compose replacement.

## Acceptance measurements

The operator ran `bash tests/harness/run_order_lifecycle_cdc.sh` (temporary containers postgres:15, Kafka 3.9.1, Debezium 3.4,
Spark in `platform-airflow:2.10.5-spark3.5.3`): **`CDC v2 harness PASS`**, 58 s. Receipt:
[`evidence/cdc-v2-local-20261019.json`](../../evidence/cdc-v2-local-20261019.json).

| Run | Oracle | Bronze | Missing | Extra | Payload | Order | Epoch | Result |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| Clean (insert/update/delete) | 3 | 3 | 0 | 0 | 0 | 0 | 0 | passed |
| 1 fake record injected into Bronze | 3 | 4 | 0 | **1** | 0 | 0 | 0 | failed (as expected; `outside_boundary` = 1) |

The operator fixed the harness while running it: Spark runs as the image's default user (the host uid is not in passwd ⇒ Hadoop
login NPE), `HOME`/ivy are writable, driver 3g, WORK permissions opened for the container, and files are `chown`ed back to the host uid after each step.
