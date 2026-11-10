# Data Generator data quality report (Stream)

This report records the actual measurements of the streaming source. The Batch part is in [data-quality-report-batch.md](data_quality_report_batch.md). For the design and code map see [generator-design.md, Stream section](generator_design.md#stream).

## Scope

The Batch reaches the 100 GB threshold with `configs/scale-100gb.yaml`, but the Stream is not emitted at that scale: 851,506 Orders/day times 30 Stream days would be tens of millions of notifications, beyond the purpose of testing streaming. The Stream in this report uses `configs/default.yaml` (4,000 Orders/day) — with the same `streaming_problems` and `drift.tiktok_live` sections as the 100 GB version, so all rates (late, duplicate, burst) are the same; only the absolute numbers are smaller. The 100 GB threshold applies only to the Amazon + Walmart Batch in `/data/raw`.

## The run

- Configuration: `configs/default.yaml` (seed 42, `cutoff` 2026-06-30, 30 Stream days to 2026-07-30, TikTok livestream on simulated day 195).
- Destination: single-node Kafka in Docker Compose, two topics `amazon.order-notifications` and `tiktok_shop.order-webhooks`. For the setup see [Docker.md](../docker.md).
- `STREAM_SPEED=60`, i.e. emitting 60 times faster than the configured pace. One run takes 43.7 seconds. At real pace (`STREAM_SPEED=1`) it would take about 40 minutes.
- Commands:

```bash
docker compose up -d kafka
docker compose run --rm generator-stream        # emit and print the measurements
uv run python scripts/evidence/measure_order_snapshots_stream.py        # print a concrete example of each problem
```

Two sources of numbers in this report:

- **Totals** are counted by the generator while emitting and written to `stream_manifest.json` on MinIO. The number of messages in Kafka is recounted with `kafka-get-offsets.sh` to confirm no messages are lost or extra.
- **Concrete examples** come from `scripts/evidence/measure_order_snapshots_stream.py`, which replays the Stream in memory (no Kafka needed) and points out one real case for each problem. It is the counterpart of the `python -m generator report` command on the Batch side.

## Measurement results

| Property | Status | Main measurement |
|---|---|---|
| Simulate late arrival | Measured | Amazon median delay 5.27 minutes, 4.98% later than 1 hour (targets 5 minutes and 5%) |
| Simulate out-of-order | Measured | 19,729 Amazon messages arrive after a message whose `EventTime` is more than 1 hour later, 98.8% of the heavily late messages |
| Simulate duplicate | Measured | TikTok Shop duplicates 1.49% (target 1.5%), duplicates keep the same `tts_notification_id` |
| Simulate burst | Measured | The livestream peak hours are 5.89 times a normal hour (target 5 to 8 times; the run's answer is 5.45) |
| Using generator configuration | Measured | Every parameter is in the `streaming_problems` and `drift.tiktok_live` sections of `configs/*.yaml` |
| Publish to a streaming platform | Measured | 492,964 messages into two Kafka topics; the count in Kafka matches the generator's count |

## Measurements of the run

```json
{
  "cutoff": "2026-06-30T00:00:00+00:00",
  "stream_end": "2026-07-30T00:00:00+00:00",
  "speed": 60.0,
  "topics": {
    "tiktok_shop.order-webhooks": {
      "rows": 93554,
      "unique_keys": 92157,
      "duplicate_rate": 0.014933,
      "injected_duplicates": 1397,
      "median_delay_minutes": 0.06,
      "share_late_over_1h": 0.0,
      "out_of_order_over_1h": 0
    },
    "amazon.order-notifications": {
      "rows": 399410,
      "unique_keys": 399410,
      "duplicate_rate": 0.0,
      "injected_duplicates": 0,
      "median_delay_minutes": 5.27,
      "share_late_over_1h": 0.049763,
      "out_of_order_over_1h": 19729
    }
  },
  "burst": {
    "live_day": 195,
    "multiplier_truth": 5.454,
    "new_orders_per_hour_live": 148.5,
    "new_orders_per_hour_normal": 25.2,
    "measured_ratio": 5.89
  }
}
```

![Terminal printing the measurements of the Stream run](../../images/stream/manifest.png)

## Details of each problem

### Message counts

| Topic | Messages | Channel's share of Orders | Estimate |
|---|---|---|---|
| `amazon.order-notifications` | 399,410 | 67% (US 45%, UK 12%, DE 10%) | 30 × 4,000 × 0.67 × 5 = 402,000 |
| `tiktok_shop.order-webhooks` | 93,554 | 15% | 30 × 4,000 × 0.15 × 5 = 90,000, plus the livestream part |

The estimate assumes 5 Order Status changes per Order (`pending_payment`, `awaiting_shipment`, `shipped`, `delivered`, `completed`). The measurement is slightly below the estimate because Orders created near `stream_end` have not finished their lifecycle, and about 3% of Orders are cancelled and so have only three status changes.

Only Amazon and TikTok Shop are in the Stream at this step. Shopify (webhook), Walmart and Shopee (batch polling) are done in step 4.

### Duplicate

| Topic | Messages | Distinct keys | Injected duplicates | Duplicate rate | Target |
|---|---|---|---|---|---|
| `tiktok_shop.order-webhooks` | 93,554 | 92,157 | 1,397 | 1.49% | 1.5% |
| `amazon.order-notifications` | 399,410 | 399,410 | 0 | 0% | 0% |

The three TikTok Shop numbers are consistent: 93,554 − 92,157 = 1,397, exactly the number of duplicates the generator injected. Amazon at 0 is by design, because only TikTok Shop and Shopify webhooks send duplicates (see [generator-design.md, section 5](generator_design.md#streaming)).

A duplicate keeps **the same** `tts_notification_id` and the same content, differing only in arrival time (2 or 30 minutes later, per TikTok Shop's retry schedule). So Flink can dedup on exactly that key.

The duplicate rate is measured as `1 - count(distinct tts_notification_id) / count(*)`. To get exactly `r` after injection, the resend probability per message must be `p = r / (1 - r)`, because resends also increase the total number of messages. With `r` = 1.5%, `p` is about 1.52%.

A real pair, taken from `scripts/evidence/measure_order_snapshots_stream.py`:

```text
tts_notification_id : 7702500355104028488
original arrived at : 2026-06-30 00:30:16
resend arrived at   : 2026-06-30 00:32:16   → 2 minutes later
identical content   : True
payload             : {"order_id":"577629363552679140","order_status":"UNPAID","is_on_hold_order":false,"update_time":1782779413}
```

### Late arrival and out-of-order

| Metric | Amazon | TikTok Shop | Target (Amazon) |
|---|---|---|---|
| Median delay | 5.27 minutes | 0.06 minutes | 5 minutes |
| Share later than 1 hour | 4.98% | 0% | 5% |
| Messages out of order by more than 1 hour | 19,729 | 0 | yes |

An Amazon notification carries two timestamps: `EventTime` is when the Order changed status, `NotificationMetadata.PublishTime` is when the message arrived. The gap between the two is the delay, and Flink measures delay by `EventTime`, so the emission speed does not affect this number.

The measured median delay is 5.27 minutes, slightly above the 5-minute target, because the 5% heavily late messages (1 to 6 hours) pull the median up.

The out-of-order column counts: messages that arrive when their `EventTime` is already more than 1 hour older than the largest `EventTime` that arrived before them. Cross-check: 5% of 399,410 is 19,970 heavily late messages, of which 19,729 end up out of order, i.e. **98.8%**. Almost every heavily late message arrives after messages newer than itself, as designed.

TikTok Shop at 0 in both columns is correct: TikTok webhooks are only a few seconds late, and the slowest resend is only 30 minutes, below the 1-hour threshold.

The most delayed message of the run, taken from `scripts/evidence/measure_order_snapshots_stream.py`:

```text
EventTime   (occurred)   : 2026-07-03 14:37:39
PublishTime (arrived)    : 2026-07-03 20:37:20   → 360 minutes late
newest message arrived   : 2026-07-03 20:36:31   → this message is 6.0 hours older
AmazonOrderId            : 113-2330946-1046314
```

This message occurred at 14:37 but only arrived at 20:37, while messages from other Orders that occurred as late as 20:36 had already arrived. Flink must keep a wide enough window, or accept dropping messages, to handle this case correctly.

### TikTok livestream burst

| Metric | Value |
|---|---|
| Simulated day with the livestream | 195 (12:00 to 18:00 UTC) |
| Multiplier, the run's answer | 5.454 |
| New TikTok Orders per hour during the live | 148.5 |
| New TikTok Orders per hour, average of the other 29 Stream days | 25.2 |
| Measured ratio | 5.89 |

The burst is created in the **simulation core**, not in the delivery layer: during the 6 live hours, the core generates extra TikTok Shop Orders so that the total rate is `live_multiplier` times normal. This works like the Batch's Prime Day-style Sale Event, i.e. changing the core's parameters on a schedule rather than editing data after generation. The multiplier is drawn once per run within the configured range `[5, 8]` and written to the manifest as the answer.

The extra Orders use their own random number generator (`rng_for(seed, "tiktok_live_orders", day)`), so turning the livestream on or off does not change the other Orders of that day.

**Note on `measured_ratio`.** 5.89 is higher than the answer 5.45, and this is not a bug. The denominator is **the average of the other 29 Stream days** (25.2 Orders per hour), while that time window on day 195 was already busier than average:

| Comparison | Orders per hour | Ratio |
|---|---|---|
| Average of the other 29 Stream days | 25.2 | 5.89 |
| Day 195 itself, with the livestream off | 29.2 | 5.09 |
| Answer drawn by the generator | | 5.45 |

Orders per day are drawn from a Poisson distribution and so fluctuate: Stream days range from 21.8 to 28.3 Orders per hour in the 12:00 to 18:00 window, and day 195 falls at the high end. Counting only the Orders added by the livestream gives 716 Orders, versus an expected 668 (Poisson error about ±26).

Both comparisons fall within the target range of 5 to 8 times. When reading `measured_ratio`, remember that it compares against the general level of the other days, so it is affected by whether the livestream day happens to be busy or quiet.

The last two lines of `scripts/evidence/measure_order_snapshots_stream.py` print all three numbers, so they can be re-verified without Kafka.

New TikTok Shop Orders per hour, the livestream day next to the day before, taken from `scripts/evidence/measure_order_snapshots_stream.py`:

```text
regular day 2026-07-13:   30   24   26   21   21   29   20   26   36   18   18   22   25   24   32   29   14   25   28   24   25   21   20   29
 LIVESTREAM 2026-07-14:   16   17   24   29   21   24   29   17   20   14   25   27  130  150  162  162  137  150   18   25   25   23   34   25
              UTC hour:    0    1    2    3    4    5    6    7    8    9   10   11   12   13   14   15   16   17   18   19   20   21   22   23
```

The six hours from 12:00 to 18:00 jump sharply (130 to 162 Orders per hour), while the remaining hours of that day stay at the normal level. The burst starts and ends exactly at the configured time window and does not spill into the rest of the day.

![Evidence for the three Stream problems](../../images/stream/evidence.png)

### Emission pace

The burst must last long enough in wall-clock time to be observable in the Flink UI (throughput, backpressure). If every simulated day were emitted evenly over 60 real seconds, the 6-hour live would last only about 15 seconds.

| Day type | Real seconds per simulated day | Live duration |
|---|---|---|
| Normal day | 60 | |
| Livestream day (195) | 600 | about 150 seconds |

Total for one full 30-day run: 29 × 60 + 600 = 2,340 seconds, about 40 minutes. The `--speed` parameter divides the wait time for quick test runs: the run in this report uses `--speed 60`, so it takes 43.7 seconds. Late arrival is not affected by the emission speed, because Flink measures by the `EventTime` in the payload.

### Destination

The Kafka key of each message is the original order ID (`AmazonOrderId`, `data.order_id`), so all messages of the same Order go to the same partition and keep their relative order. Out-of-order only happens **between different Orders**, as intended.

```bash
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 --topic amazon.order-notifications
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 --topic tiktok_shop.order-webhooks
```

| Topic | Partition 0 | Partition 1 | Partition 2 | Total | Generator count |
|---|---|---|---|---|---|
| `amazon.order-notifications` | 132,222 | 133,332 | 133,856 | 399,410 | 399,410 |
| `tiktok_shop.order-webhooks` | 31,375 | 30,918 | 31,261 | 93,554 | 93,554 |

The last two columns are equal, meaning no message was lost or duplicated on the way into Kafka.

Kafka retains messages from all previous runs. For the total to match `rows` exactly, delete the Kafka data before replaying:

```bash
docker compose stop kafka && docker compose rm -f kafka && docker volume rm platform_kafka-data
docker compose up -d kafka
```

![Offsets of the two topics in Kafka](../../images/stream/kafka-offsets.png)

Read a few messages to confirm they have the Channel's native shape:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic tiktok_shop.order-webhooks \
    --from-beginning --max-messages 3
```

```json
{"type": 1, "tts_notification_id": "7611157955723032014", "shop_id": "7110114833574",
 "timestamp": 1782777648,
 "data": {"order_id": "573366978841156122", "order_status": "AWAITING_SHIPMENT",
          "is_on_hold_order": false, "update_time": 1782777648}}
```

![Reading sample messages from Kafka](../../images/stream/kafka-messages.png)

## Three Channels from the small source

The small source `out/channels-small` generates all five Channels for the **Batch** (see the Batch report). The Stream does not yet emit the three new Channels because only Amazon and TikTok Shop have event renderers (notification/webhook); Shopify, Walmart and Shopee arrive as polling batches and belong to a later step. The two existing topics already cover the two main streaming shapes to test: Amazon's late arrival/out-of-order and TikTok Shop's duplicate/burst.

## Assumptions and open items

- **The Amazon notification shape is an assumption, level Low.** The official SP-API Notifications documentation could not be read, so the field names were chosen by the project following `ORDER_CHANGE`. This note is also at the top of `generator/channels/amazon.py` and in [channel-sources.md](channel_sources.md). The TikTok Shop webhook shape comes from official documentation, level High.
- Amazon notifications in the Stream always use Orders v0-style status values (`Shipped`) and do not change with the schema switch date as the Batch does.
- The livestream so far only causes a burst, not concept drift (apparel return rate nearly doubling). That part is done in step 4.
- Shopify (webhook, 1.5% duplicates), Walmart and Shopee (polling, arriving in batches every 5 to 15 minutes) are not in the Stream yet.

## On AWS

The generator emitted into Kafka on Compose on EC2, `configs/default.yaml`, seed 42, `speed=0`, taking 41 seconds.
The [AWS manifest](../../evidence/aws/stream-manifest-20261028.json) is the source of the numbers in the table below;
this is a separate cloud run and does not use the `STREAM_SPEED=60` pace of the local run above.

| Property | Amazon notification | TikTok webhook |
|---|---|---|
| Rows / unique keys | 399,410 / 399,410 | 93,554 / 92,157 |
| Duplicate | 0 | 1,397, rate 0.014933 ≈ 1.49% |
| Median delay | 5.27 minutes | 0.06 minutes |
| Share later than 1 hour | 0.049763 ≈ 4.98% | 0 |
| Out-of-order by more than 1 hour | 19,729 | 0 |

Lateness is Amazon `EventTime` compared with arrival time; TikTok duplicates keep the same `tts_notification_id`.
Livestream burst: 148.5 Orders/hour vs 25.2 in a normal hour, ratio 5.89 per the manifest.
[Generator code](../../../generator/__main__.py), [configuration](../../../configs/default.yaml),
[measurement script](../../../scripts/evidence/measure_order_snapshots_stream.py).

![Flink 1.20.3 Completed Jobs: Amazon baseline, Amazon late, two dedup jobs and the window job all FINISHED](../../evidence/aws/flink-completed-jobs-20261028.png)

![Flink dedup_tiktok_state_proof: HASH edge to Deduplicate, source sent 93,554 records, job FINISHED](../../evidence/aws/flink-dedup-graph-20261028.png)

The screenshots show the Flink consumer completed and dedup received 93,554 records; the lateness/duplicate properties in the table come from the manifest.
Flink separated 10,166 late messages, fewer than the generator's count because of periodic/per-partition watermarks;
[processing and job_id](flink.md#on-aws). There is no detailed AWS UI capture of watermarks/late events yet.
