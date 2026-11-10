# Flink on the generator's two Kafka topics

The SQL jobs in `flink_jobs/` read from the start of a Kafka topic and stop at the last offset
fixed when the job starts (`scan.startup.mode=earliest-offset`,
`scan.bounded.mode=latest-offset`). This way each run can count its input and
output. If the Stream is replayed into the same topic, a new run also reads the earlier
replays; counts must be reconciled against the actual offsets before drawing conclusions.

`docker/Dockerfile.flink` uses Flink 1.20.3 and the Kafka SQL connector
3.4.0-1.20. The two Compose services are the JobManager (UI on port 8082) and the
TaskManager. Configured following [Flink Docker](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/resource-providers/standalone/docker/)
and the [Kafka Table connector](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/table/kafka/).

| Job | Source | Mechanism | File sink |
|---|---|---|---|
| `flink_jobs/order_snapshots/baseline.sql` | Amazon notification | bounded read, keeps ID and EventTime | `out/flink/baseline-amazon` |
| `flink_jobs/order_snapshots/late_arrival.sql` | Amazon notification | EventTime watermark one hour late; `CURRENT_WATERMARK` separates late messages | `out/flink/late-amazon-1ms` |
| `flink_jobs/order_snapshots/dedup_webhooks.sql` | TikTok webhook | `ROW_NUMBER` by notification ID, keeps the first; Flink maintains keyed state | `out/flink/unique-tiktok` |
| `flink_jobs/order_snapshots/windows.sql` | New TikTok Orders | 5-minute tumbling and 5-minute sliding with a 1-minute step | `out/flink/tiktok-*` |

`CURRENT_WATERMARK` returns the watermark at the operator and can be null before the source
emits its first watermark; the late job's condition only selects messages that have a watermark and
`event_ts <= watermark`. This number can differ from the generator's `out_of_order_over_1h=19,729`
because the generator counts against one global send order, while Flink emits
watermarks per partition and periodically.

## Run measurements, late messages remeasured

| Measurement | Result | Reconciliation |
|---|---:|---|
| Amazon baseline | 399,410 rows / 3.0 seconds | matches the Kafka total |
| Amazon late, default 200 ms | 242 rows | watermark progress still depends on the emit schedule |
| Amazon late, `0ms` | 0 rows | not used as proof; no watermark satisfied the filter in this bounded run |
| Amazon late, `1ms` | 9,143 rows / 3.6 seconds | 2.29% of 399,410; lower than the generator's global count of 19,729. One measurement gave 6,310 rows: this number depends on the watermark emit schedule relative to the bounded read speed and is not fixed |
| TikTok dedup | 92,157 out of 93,554 | 1,397 dropped, matching the Kafka manifest |
| 5-minute tumbling | 7,555 windows | each window emits once |
| 5-minute sliding, 1-minute step | 37,870 windows | about five times as many windows because of overlap |

Output is in `out/flink/`. Example maxima in the run: tumbling **20 Orders**
in 13:55–14:00 UTC; sliding **22 Orders** in 13:59–14:04 UTC on
2026-07-14. These are `UNPAID` Orders, not total webhooks.

See
[the CURRENT_WATERMARK function](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/functions/systemfunctions/)
and [watermark SQL](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sql/create/).

## Catching late messages with a watermark

`flink_jobs/order_snapshots/late_arrival.sql` assigns the watermark `event_ts - INTERVAL '1' HOUR`:
Flink treats a message as late when its event time is already before the current
watermark. The condition `event_ts <= CURRENT_WATERMARK(event_ts)` keeps exactly the
messages the watermark has passed (more than one hour behind the watermark), then writes them separately to
`out/flink/late-amazon-1ms`. So this measurement does not count every late message; it
counts the part the watermark has already passed.

The run replayed exactly the batch of 399,410 notifications; job
`b903a3491a21b1bd0c48a1252f3ee78a` finished after 3.648 seconds and wrote **9,143 rows**
(see the measurement table above). This number is not fixed because it depends on the watermark emit schedule
(`pipeline.auto-watermark-interval`) relative to the bounded read speed.

![Flink UI: job late_amazon FINISHED, source amazon_notifications → Calc → StreamingFileWriter](../../images/flink/flink-late-arrival.png)

## Tumbling and sliding windows

`flink_jobs/order_snapshots/windows.sql` declares two windows on the same stream of new Orders
(`new_orders`, filtered on `order_status = 'UNPAID'`): a 5-minute tumbling window and a 5-minute hopping window
with a 1-minute step. Both read the `window_start`/`window_end` produced by the window function
and then `GROUP BY` them:

```sql
INSERT INTO tiktok_tumble_5m
SELECT CAST(window_start AS STRING), CAST(window_end AS STRING), COUNT(*)
FROM TABLE(TUMBLE(TABLE new_orders, DESCRIPTOR(event_ts), INTERVAL '5' MINUTE))
GROUP BY window_start, window_end;

INSERT INTO tiktok_hop_5m_1m
SELECT CAST(window_start AS STRING), CAST(window_end AS STRING), COUNT(*)
FROM TABLE(HOP(TABLE new_orders, DESCRIPTOR(event_ts),
               INTERVAL '1' MINUTE, INTERVAL '5' MINUTE))
GROUP BY window_start, window_end;
```

`TUMBLE` cuts the time axis into disjoint 5-minute windows, and each event
falls into exactly one window. `HOP` creates overlapping windows: a 1-minute hop, 5 minutes wide,
so each event falls into five windows and the observed window count is about five times larger. The
results match: **7,555** tumbling windows versus **37,870** hopping windows.

This run's job ID is `c2bb95bb43d8a522d34155fa4a7c3946`, finished after
1.503 seconds. The Flink graph shows the `tiktok_webhooks` source fanning out into two
`LocalWindowAggregate`/`GlobalWindowAggregate` branches writing to two separate sinks, matching
the two windows in `EXECUTE STATEMENT SET`.

![Flink UI: tumbling and hopping window branches running on the same source](../../images/flink/flink-windows.png)

Startup commands:

```bash
env -u POSTGRES_PASSWORD docker compose --profile flink build flink-jobmanager
env -u POSTGRES_PASSWORD docker compose --profile flink --profile stream up -d kafka flink-jobmanager flink-taskmanager
```

Run each script from the JobManager container, for example:

```bash
env -u POSTGRES_PASSWORD docker compose --profile flink exec -T flink-jobmanager ./bin/sql-client.sh -f /opt/flink/jobs/order_snapshots/baseline.sql
```

Each job's output is written to `out/flink`; the baseline Flink UI screenshot is stored in
`docs/images/flink/`.

![Flink 1.20.3 UI: one TaskManager, two slots and the Amazon baseline job Finished](../../images/flink/flink-overview.png)

Screenshot taken after the latest successful baseline; the Completed Jobs list shows
job `baseline_amazon` in state `FINISHED`. The file
`out/flink/baseline-amazon-proof/` contains 399,410 rows.

## Keyed state evidence for TikTok dedup

The same dedup was rerun with
`flink_jobs/order_snapshots/dedup_state_proof.sql`, writing to a separate sink
`out/flink/unique-tiktok-state-proof/` to keep the old run. Job ID
`a1ee19976c64cee653614ffdbef6aafe` finished after 2.531 seconds. The Flink REST API
returns an execution plan with the operator
`Deduplicate(keep=[FirstRow], key=[tts_notification_id], order=[PROCTIME])`
and a `HASH` edge from the source to the operator. So two webhooks with the same ID reach the same
keyed operator; `ROW_NUMBER` in SQL keeps the first by processing time.

The source sends **93,554** records to the operator; the output file has **92,157** rows,
removing **1,397** duplicate webhooks. Job figures, plan and metrics are kept in
[`docs/evidence/flink-dedup-state.json`](../../evidence/flink-dedup-state.json).

![Flink UI: completed TikTok job, HASH edge and Deduplicate operator](../../images/flink/flink-dedup-state.png)

The screenshot above shows the job `FINISHED`, the graph `Source → Deduplicate → Writer` and
93,554 records leaving the source. The screenshot and plan prove that Flink executed the keyed
deduplication; because the job reads bounded and finishes after a few seconds, this measurement
is not evidence about long-term state size or checkpoint recovery.

## On AWS

Flink 1.20.3 and Kafka run with Compose on EC2; bounded output goes to EBS `/mnt/platform-data/flink`.
The [AWS manifest](../../evidence/aws/stream-manifest-20261028.json) records job_id, input/output counts and the generator configuration.
The Completed Jobs screenshot shows baseline, late, dedup, state proof and windows, all FINISHED.

| Step / code | Problem and handling | AWS figures from the manifest |
|---|---|---|
| [Baseline](../../../flink_jobs/order_snapshots/baseline.sql) | Bounded Amazon read, late messages not separated | 399,410 rows, job `ffd9de8a94c5bf67179482bfe03d3b59` |
| [Late arrival](../../../flink_jobs/order_snapshots/late_arrival.sql) | Event-time watermark 1 hour late; filter `event_ts <= CURRENT_WATERMARK(event_ts)` to a separate sink | 10,166 rows, job `b172bcf2510b47e1cbcd532f3078a82d` |
| [Dedup](../../../flink_jobs/order_snapshots/dedup_webhooks.sql), [state proof](../../../flink_jobs/order_snapshots/dedup_state_proof.sql) | Duplicate TikTok IDs; `ROW_NUMBER` keeps the first, the HASH edge routes the same ID to the keyed Deduplicate | 93,554 in → 92,157 out, 1,397 dropped; state-proof job `f5b642f64b133e28ec0f57021f1eab22` |
| [Window SQL](../../../flink_jobs/order_snapshots/windows.sql) | Count new Orders with a 5-minute tumbling window and a 5-minute HOP sliding by 1 minute | 7,555 / 37,870 windows, job `dad323a91493cdf365c5a3d3d4dd0adf` |

![Flink 1.20.3 Completed Jobs: Amazon baseline, Amazon late, two dedup jobs and the window job, all FINISHED](../../evidence/aws/flink-completed-jobs-20261028.png)

![Flink dedup_tiktok_state_proof: HASH edge to Deduplicate, source sent 93,554 records, job FINISHED](../../evidence/aws/flink-dedup-graph-20261028.png)

The dedup UI shows HASH → Deduplicate and 93,554 records received, proving the keyed operator ran.
The 92,157 output rows come from the manifest; the file sink's Records Sent column of 0 must not be misread as data loss.
The Completed Jobs screenshot only confirms the late job FINISHED; there is no AWS UI yet that opens the operator/watermark with an example late event.
10,166 is not deterministic: watermarks are emitted periodically, unlike the generator's global out-of-order count of 19,729.
The long-running AWS window of 24h sliding by 1h has [its own operator/checkpoint screenshot](../../ml-llm-platform/stream_features.md#on-aws-pipeline-a-2026-10-30).
