# Source v2 at large scale — package B+

> **Pipeline v2 (order lifecycle) — designed and tested locally, not the deployed pipeline.** The pipeline running on AWS is v1 (DP1 → DP4 + ML); see [Data Platform evidence](../evidence.md). Mentions of GCP below are an older plan that was never run; the current cloud target is [AWS](../../shared/aws_deploy.md).

Extends the v2 source set to measure Spark baseline/skew, try lakehouse/warehouse optimizations,
streaming and Iceberg maintenance on varied data. This is a seeded simulated source;
it is not yet acceptance evidence for 100 GB or a Spark result.

## Design

API `generate_scaled(output_root, config: ScaleConfig) -> manifest` in
[`generate_order_lifecycle.py`](../../../generator/generate_order_lifecycle.py). `ScaleConfig` is a frozen
dataclass; `validate()` checks parameters before creating directories, `estimate()` returns the expected
record/byte counts and the worst-case cap. Stdlib only, with a dedicated RNG `random.Random(seed)`.

| Parameter | Default | Domain / meaning |
|---|---|---|
| `seed` | 7 | Integer; reproduces each world independently |
| `start` | 2026-01-01 00:00 UTC | Timestamp with UTC timezone |
| `days` | 10 | 1..180 order creation days, exclusive range |
| `orders_per_day` | 200 | 1..20,000, total across all Channels |
| `sku_count` | 100 | 2..5,000, catalog shared by the Seller |
| `sku_zipf_s` | 1.2 | 0 is uniform, >0 gives hot SKUs by rank |
| `channel_weights` | (1,1,1,1,1) | Amazon, Walmart, Shopify, TikTok Shop, Shopee; non-negative weights, sum >0 |
| `lines_per_order` | (0.55; 0.30; 0.12; 0.03) | Weights for 1, 2, 3, 4 lines, configurable via the API |
| `followup_days` | 270 | Mark from start, ≥ days + 50 |
| `observation_days` | 285 | Mark from start, ≥ followup_days + 9 |
| `native_schemas` |  | Tuple `(Channel, version)`; override like the fixture |

The three horizons have the same semantics as in the fixture. The defaults 270/285 replace 90/105 to
cover the full 180-day domain. Order creation times are spread to the microsecond within the day;
the ordered quantity per line is still 3 and money uses minor integers/decimal strings as in the fixture.

A cap of **20 million source records** is enforced up front with a conservative bound:
`days × orders_per_day × (24 × max_lines_supported + 1) + sku_count × 11 + 32 + 9 × ceil(followup_days / 7)`.
`max_lines_supported` considers non-zero weights. The bound can reject configurations whose
sampled expectation is below 20 million. Truth does not count as source.

## Scenarios

Each line picks exactly one scenario; lifecycle rows always use the shared emitter
`_emit_sources`. Windows use exact UTC 30 × 24 hours per the
[label contract](return_label_contract.md). There are no labels in the source tree.

| Scenario | Share | Events / physical outcome |
|---|---:|---|
| `delivered` | 30% | All 3 delivered, no post-sales |
| `partial_cancel` | 10% | 1 canceled before shipping, 2 delivered |
| `split_delivery` | 12% | Two allocations 2+1, delivered at created+3 and +4 days |
| `return_with_refund` | 7% | Requested → approved at delivery+29 days → received, **with** a successful product refund (the most common real-world case) |
| `return_only` | 3% | As above but **without** a refund (exchange / voucher / rejected after inspection) |
| `return_after_window` | 7% | Approved at delivery+31 days; no refund within the window |
| `product_refund` | 8% | Product refund success of 10,000 minor at delivery+29 days, tied to an allocation; no Return |
| `refund_retry` | 5% | Product refund failed then succeeded in the same transaction, revision 1→2 |
| `late_observation` | 8% | Fully delivered; carrier observation delayed randomly by 1 hour..4 days, propagated through dependencies |
| `correction` | 6% | Return approved; initial refund not tied to a line, correction revision 2 and uncertainty resolved; separate shipping refund |
| `right_censored` | 4% | Commitment/reservation/planned allocation still open at the end of the horizon; never inferred as delivered |

`failed` is a value of the existing `status` field; the old fixture only emitted `success`.
No event types or payload fields are added. Retry uses `state_revision`; a real correction
uses `operation=correction` and `corrects_source_event_id`.

The manifest records the weights, seed, all parameters, counts and actual shares. Shares are
sampled; the 5,000-line test accepts ±3 percentage points. Zipf checks the top SKU >10% at s=1.2;
s=0 checks the top SKU <2 times the mean.

## Writing sources and memory

The first pass hashes the world day by day to lock the identity; the next pass uses the same seed, emits one
day and writes it to a temporary SQLite in the output root. SQLite is stdlib, with the cache limited to
8 MiB and temp sorting on disk. It then iterates by known-time, assigns contiguous sequences
per producer, and writes JSONL by `export_date=source_observed_at[:10]`. SQLite
is deleted after export. The world or events of all days are never held in RAM.

Opening stock once per SKU, in the same Seller pool; the opening quantity covers the worst case.
Listings are emitted once per `(Channel, SKU)`. Inbound and snapshot are at the end of follow-up,
after all emitted movements; reservations of still-open lines are kept on the snapshot.
The coverage hash is a canonical JSON array sorted by event ID via an iterator over SQLite;
sequence ranges are compressed when contiguous. Memory still depends on the size of one day,
the catalog, snapshot order IDs and coverage metadata/ranges. Temporary disk on the same filesystem
as the output needs roughly the source size plus spool and indexes.

Coverage is emitted **in consecutive weeks** per feed, non-cumulative. The last week covers up to
the end of follow-up; observed is after end of week +1 day or after the last late row +1µs,
whichever is later. The SQLite index `(feed,effective)` selects each week's set and then hashes by
ID, so each row is hashed only once. An empty week has count0/digest `[]`; the ranges
can be disjoint because the continuous producer sequence is shared by several feeds.

Silver checks the interval before the digest and joins by UTC day bucket to avoid comparing every row against
every week. The label policy concatenates adjacent weeks; gaps/partials remain unknown. Gold only
brings coverage that intersects the delivery+30d window into the line input and keeps the related revisions.
Measured with seed7 `days=60, orders_per_day=4, sku_count=3`: 367 lines, **131 real targets**
with PIT history, at most 30 coverage records/line; [details and Spark command](gold_order_lifecycle.md).
The cost: more envelopes/files, labels waiting for the week to close plus latency, and an extra
interval union operation; ADR-0013 records the choice.

Layout and envelope stay compatible with the fixture: 9 operational feeds, coverage,
uncertainties and 5 native feeds. A feed with no events still has an empty JSONL. The native
snapshot manifest keeps the full order scope; Walmart is a JSONL bootstrap for loading into a DB,
not yet JDBC evidence. Shopee schema 2 is still a breaking override as in the fixture;
Amazon 2 and TikTok Shop 2 use the matching render.

`_truth/lines.jsonl` sits outside `dataset_version=*/`; the manifest records
`truth.not_a_source=true`, the row count and the hash. Each row contains line identity, SKU,
scenario, ordered/canceled/delivered/returned quantities and approved/refund-success time. Only tests read truth.
Pytest scans the Data Platform jobs to block references to the `_truth/` path; the name `ground_truth`
in the existing v1 job is not the scale truth directory.

## Keeping the default fixture unchanged and reproducible

Before the refactor, the fixture `/tmp/x-before`, the physical digest, the dataset ID and the
SHA-256 of every file in the manifest were saved. The regression suite pins every digest in
[`tests/unit/generator/order_lifecycle_fixture_digests.json`](../../../tests/unit/generator/order_lifecycle_fixture_digests.json).
The fixture has an execution `recorded_at`; the test fixes the clock at
2026-10-19T00:00:00Z so byte comparison is meaningful. The fact hash and the default fixture dataset stay unchanged; the fixture's `contract_sha256` pins the
historical baseline and does not change with the new documentation describing scale weekly coverage.
Runtime seconds and the manifest execution timestamp naturally vary between runs.

The scale source freezes `recorded_at` at the `source_observation_end` mark so that the same seed,
parameters, code and Python reproduce the same file SHA-256. This is the timestamp of the simulated
export; the manifest still records the real execution time, and this policy is documented separately.
A native schema override changes the dataset identity but keeps the physical digest.

```bash
.venv/bin/python -m generator.generate_order_lifecycle \
  --output /tmp/scale-v2-new --orders-per-day 200 --days 10 --seed 7 \
  --sku-count 100 --sku-zipf-s 1.2 \
  --channel-weights amazon=3,walmart=2,shopify=1,tiktok_shop=1,shopee=1 \
  --native-schema-for amazon=2
.venv/bin/pytest -q tests/unit/generator/test_generate_order_lifecycle_fixture.py tests/unit/generator/test_generate_order_lifecycle.py
```

The target directory must be new. The CLI prints the estimate before generating. The estimate uses a pilot of at most
100 orders through the renderer itself, plus inventory cost and the expected number of Listings; it is not
an absolute guarantee for every very small seed.

## Measured results

**Baseline before weekly coverage** (the byte/file/dataset figures below belong to the old code;
throughput/RSS have not been re-measured after the coverage fix per ADR-0013). Run of `2,000 orders/day × 30 days`, seed 7, SKU=100, s=1.2:

| Metric | Measured |
|---|---:|
| Order / Order Line | 60,000 / 97,890 |
| Source records / files | 1,061,702 / 447 |
| Source bytes | 1,546,066,142 (about 1.55 GB decimal) |
| `resource.getrusage(...).ru_maxrss` | 128,536 KiB = 125.52 MiB |
| `generate_scaled` time | 421.738301 seconds |
| Right-censored / terminal | 3,905 / 93,985 |
| Estimate error, rows / bytes | 2.58% / 4.07% |

Command: `.venv/bin/python -m generator.generate_order_lifecycle --output
out/codex-04-scale-final --orders-per-day 2000 --days 30 --seed 7`.
The output directory (1.5 GB) was deleted after measuring to free disk space; at the baseline version the command above produced
dataset `dataset-e16b9a828d69a3f6fb9cf507f2c9087d` (same seed ⇒ same digest). The code hash was checked against the final version;
the 447 source files and truth were re-verified by SHA-256. RSS is for the Python process
alone, excluding the OS page cache. The machine had other jobs running concurrently and a preliminary run
partly overlapped; this time is not an isolated CPU/IO benchmark.

Pytest: **41 passed in 31.68s** for the two source files, including the bit-identical fixture check,
5,000 lines, two SKU distributions, gate/coverage and the 180-day boundary.


Spark: **not run in the sandbox**. The unittest suite in
[`tests/spark/check_order_lifecycle_silver_scale.py`](../../../tests/spark/check_order_lifecycle_silver_scale.py) generates
200 orders/day × 10 days, calls `build_silver` with keyword arguments including
`contracts_dir="/workspace/contracts"`; it checks that every reconcile check is 0,
quarantine is 0 and the Silver line set equals truth.

Result (operator run): **`Ran 3 tests in 980.842s — OK`** (`tests/spark/check_order_lifecycle_silver_scale.py`, 200 orders/day ×
10 days, image `platform-airflow:2.10.5-spark3.5.3`, `--network none --read-only`, 3 GB driver): every `reconcile` check = 0,
quarantine = 0, Silver line set = `_truth`. The long time is because the machine was running other jobs in parallel; do not use it as a performance figure.

```bash
docker run --rm --network none --read-only --tmpfs /tmp:rw,size=2g \
  --mount type=bind,src=$PWD,dst=/workspace,readonly --entrypoint python \
  platform-airflow:2.10.5-spark3.5.3 /workspace/tests/spark/check_order_lifecycle_silver_scale.py
```

## Limitations

- Does not load PostgreSQL, run the Data Platform or write labels; the source benchmark says nothing about Spark speed.
- Inventory and USD money are still simple; no price variation, many sellers or multiple available pools yet.
- Coverage proves the emitted source set, not that every line is terminal;
  censored lines must be excluded/kept immature by the label policy.
- Partial output is not resumed. On failure, partial output may remain; use a new directory.
- Code and Python hashes are pinned in the identity; changing the environment can change the dataset ID.
