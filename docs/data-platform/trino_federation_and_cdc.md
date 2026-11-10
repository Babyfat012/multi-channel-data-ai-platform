# Trino Federation and CDC on AWS

This page covers two additions to the platform: the idea, how it is deployed, and evidence that it runs. First,
Trino is used to reconcile data between Iceberg on S3 and PostgreSQL in one SQL statement. Second, Debezium is used
to keep every create/update/delete change of the source and move it through Kafka.

## On AWS

**Part 1: cross-source queries with Trino.** Trino 483 runs on EC2 (profile `query`, listening only on loopback port
8083). The catalogs are declared in [`trino/catalog-aws/lakehouse.properties`](../../trino/catalog-aws/lakehouse.properties):

- `lakehouse`: Iceberg REST (Lakekeeper) with real S3, accessed through the EC2 IAM role (no access keys).
- `postgres`: PostgreSQL on EC2.

The SQL below reads the Gold table on S3 and the 30-day warehouse table on PostgreSQL at the same time:

```sql
SELECT 'iceberg' AS nguon, count(*) AS dong, sum(line_net_amount) AS tien
FROM lakehouse.gold.fact_order_lines_dp2
WHERE seller_sku = 'APP-00482'
  AND created_at >= TIMESTAMP '2026-05-31 00:00:00 UTC' AND created_at < TIMESTAMP '2026-06-30 00:00:00 UTC'
UNION ALL
SELECT 'postgres', count(*), sum(line_net_amount)
FROM postgres.warehouse.fact_order_lines_30d_dp2
WHERE seller_sku = 'APP-00482'
```

(`nguon`, `dong`, `tien` are the column aliases used in the recorded run: source, rows, amount.)

| nguon | dong | tien |
|---|---|---|
| iceberg | 4,517 | 49,632,312.55 |
| postgres | 4,517 | 49,632,312.55 |

The two sources match in both row count and total amount. This result means two things: it is a working cross-source
query, and it is a cross-check showing that the `dp4_warehouse` DAG copied exactly 30 days from Gold to PostgreSQL.

Query `20261029_092844_00002_zfrx9` finished in the FINISHED state after 15.50 s (planning took 571 ms), with 3
stages. Stage 1 reads Iceberg: it scanned 7.18M rows, with a total processing time (scheduled) of 27.3 s on 1 worker.

![Trino Cluster overview: one worker, UNION ALL query over two sources FINISHED](../evidence/aws/trino-queries-20261029.png)

![Trino query 20261029_092844_00002_zfrx9 FINISHED, elapsed 15.50 seconds](../evidence/aws/trino-federation-overview-20261029.png)

![Trino: SQL reading lakehouse.gold and postgres.warehouse, all three stages FINISHED](../evidence/aws/trino-federation-stages-20261029.png)

Benefit: Gold on S3 can be cross-checked against the PostgreSQL warehouse with a single SQL statement, removing the manual step of combining results.

Limitations and sources of the numbers:

- `UNION ALL` keeps the result of each source separate. Two matching aggregates do not prove that every row is equal.
- The numbers 4,517 and 49,632,312.55 come from the [receipt](../evidence/aws/data-platform-aws-20261027.json)
  (`novel_trino_federation`). The UI screenshots only show the SQL reading two catalogs and the FINISHED state; they do not show the CLI
  result table.
- PostgreSQL catalog: [postgres.properties](../../trino/catalog-aws/postgres.properties).

## On AWS

**Part 2: CDC (Change Data Capture) with Debezium.** Debezium 3.4.3, PostgreSQL, Kafka, Airflow and
Spark run with Compose on EC2 in Sydney; the Bronze Iceberg table is written to S3. The idea is to read the WAL (PostgreSQL's
write-ahead log) to keep every create/update/delete that happens after the batch load, instead of rescanning the whole source table.

DAG [dp1_cdc](../../dags/dp1_cdc.py), run `manual__2026-10-28T10:51:33.422898+00:00`, has two steps:

1. [ingest_walmart_cdc](../../spark_jobs/cdc/ingest_walmart_order_changes.py) pins a fixed offset range to read, then
   writes the envelope (the full change message) together with provenance (source information) into Bronze.
2. [validate_walmart_cdc](../../spark_jobs/cdc/validate_walmart_order_changes.py) compares Kafka with Bronze, only within the exact
   source epoch of that run.

Connector configuration: [setup_order_snapshots_debezium.py](../../scripts/setup/setup_order_snapshots_debezium.py).

Results of the [AWS probe](../evidence/aws/cdc-probe-20261028.json) `CDC-DEMO-fb4b96588a6d` (the probe is three test events written
to the source):

- The events land in topic `platform_cdc.walmart.orders`, partition 0, offsets 0/1/2 for create/update/delete
  (`c/u/d`) respectively.
- The time from commit at the source to arrival in Kafka is 730.7 / 452.1 / 295.3 ms respectively.
- The content matches the event type: create has an after image, update has both before and after, delete has a before image.
- After the delete, the source has 0 probe rows left.
- The validator records: the number of messages in Kafka equals the number of Bronze rows equals the number of unique offsets, all 3. Missing, extra, wrong
  payload and error counts are all 0.

Do not use the times above as the latency from commit to Bronze.

![Airflow validate_walmart_cdc: Kafka 3, Bronze 3, unique offsets 3; missing, extra, wrong payload and errors all 0](../evidence/aws/dp1-cdc-validate-20261028.png)

![DataHub CDC: Kafka orders through ingest_walmart_cdc to raw_walmart_cdc_v2 and validate_walmart_cdc](../evidence/aws/datahub/dp1-cdc-lineage.png)

![DataHub Quality raw_walmart_cdc_v2: assertion dp1_cdc.validate_walmart_cdc with a green check](../evidence/aws/datahub/dp1-cdc-validation.png)

The Lineage screenshot shows data flowing from the topic, through the ingest task, to `bronze.raw_walmart_cdc_v2`. The Quality screenshot shows
the validator's assertion with a green check. The [AWS manifest](../evidence/aws/datahub-validation-aws-20261029.json) keeps the same
run_id and measurements. Limitation: this is only a three-event probe, not a CDC load measurement, and it does not prove Silver/Gold
are updated automatically.

[Section index](README.md) · [Main index](../README.md)
