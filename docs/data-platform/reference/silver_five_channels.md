# Silver for the three new Batch Channels

TikTok Shop, Shopify and Shopee have JSONL source exports in
`out/channels-small/source-exports`. DP1 loads each response verbatim into a separate
Bronze table; DP2 reads Bronze and writes a separate Silver table for each Channel.
This is small data for testing the pipeline, not a replacement for the 100 GB raw data of
Amazon/Walmart in `/data/raw`. The `out/` directory is not committed; regenerate it with:

```bash
.venv/bin/python -m generator batch --config configs/small.yaml --output ./out/channels-small
env -u POSTGRES_PASSWORD bash scripts/setup/setup_dp1_file_channels.sh
env -u POSTGRES_PASSWORD bash scripts/setup/setup_dp2_file_channels.sh
```

| Channel | Order key | Order Line key | Source time | Source amount |
|---|---|---|---|---|
| TikTok Shop | `data.orders[0].id` | `line_items[].id` | Unix seconds | String per currency |
| Shopify | `order.id` | `line_items[].id` | ISO 8601 | String per currency |
| Shopee | `response.order_list[0].order_sn` | `item_list[].order_item_id` | Unix seconds | VND integer |

The job `spark_jobs/silver/build_file_order_snapshots.py` explodes the arrays into Order Lines and deduplicates
on the pair `(order_id, order_item_id)`. The record with the latest `last_updated_time` wins;
on a tie, the latest `export_date` and then `source_file` is chosen. `export_date` is only the time
the snapshot was taken, not part of the Order Line key. Silver keeps
`source_file`, `export_date`, `processed_at` and `run_id` to trace the winning record.

The three Silver tables share the same main columns: `order_id`, `order_item_id`, `created_at`,
`last_updated_time`, `order_status`, `channel_status`, `seller_sku`, `quantity`,
`line_gross_amount`, `line_discount_amount`, `line_net_amount`, `currency`,
address and provenance. `line_net_amount = line_gross_amount - line_discount_amount`.
Shopee keeps VND amounts in whole units and does not divide by 100.

Status mapping is deliberately conservative:

- TikTok Shop has separate API statuses `DELIVERED` and `COMPLETED`; Silver keeps both
  the native status and the normalized form. A snapshot's `update_time` is not assumed
  to be the actual delivery date of every Order.
- Shopify only has `financial_status`, `fulfillment_status`, `closed_at` and
  `cancelled_at` in the simulated response. Silver derives the status from this combination;
  `fulfilled` is called `shipped`, and the current renderer's `closed_at` is
  called `completed`. This projection is tied to the simulated renderer, not a guarantee
  that every real Shop follows the same process.
- Shopee uses `COMPLETED` for both delivered and finished; Silver does not build
  `delivered_at` from `update_time`.

`spark_jobs/silver/validate_file_order_snapshots.py` checks the distinct Order count between Bronze and
Silver, unique Order Line keys, required fields and the amount arithmetic. The five-Channel
DP2 run succeeded; measurements and UI screenshots are recorded in `docs/data-platform/pipeline_orchestration.md`.
