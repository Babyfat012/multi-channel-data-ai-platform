# Source v2 — small fixture for package B

> **Pipeline v2 (order lifecycle) — designed and tested locally, not the deployed pipeline.** The pipeline running on AWS is v1 (DP1 → DP4 + ML); see [Data Platform evidence](../evidence.md). Mentions of GCP below are an older plan that was never run; the current cloud target is [AWS](../../shared/aws_deploy.md).

Local implementation of [source contract v2](source_contract_order_lifecycle.md), keeping the scope
Q52–Q61. Separate entry point: [generator/generate_order_lifecycle_fixture.py](../../../generator/generate_order_lifecycle_fixture.py).
It does not replace the v1 Batch/Stream engine and does not use the historical source/evidence roots.

```bash
.venv/bin/python -m generator.generate_order_lifecycle_fixture --output out/source-v2-ROOT-THAT-DOES-NOT-EXIST-YET
.venv/bin/pytest -q
```

The API `generate_fixture(root, FixtureConfig(...), delivery_parts=1, native_schema=1)`
only accepts a new local path; even an existing empty directory is rejected.
There is no resume/restart, Cloud IO, DB load, service startup or full-scale generation.
Creation defaults to 3 days, limited to 2–7 days; 2 SKUs, 1 Order/Channel/day,
2 lines/Order. Five Channels: Amazon, Walmart, Shopify, TikTok Shop, Shopee.
The fixture is a deterministic set of scenarios, not a business distribution for benchmarking.

## Sources and identity

`identity.json` is written before the data files: it pins config, seed, population/world
and physical outcome digest, contract/code/Python digest/version, module versions,
required/optional source profiles, renderer config and three exclusive UTC horizons.
`manifest.json` locks the file list, SHA-256/bytes/counts, authority/capability/scope,
source snapshots, coverage and diagnostics. Operational layout per the contract:
`dataset_version=.../producer=.../feed=.../export_date=.../part-00000.jsonl`.
Coverage is exported by exactly the producer that owns the feed; operational source authority is
not granted for a Channel-managed pool just because the engine knows the world.

The native Orders are **synthetic native-shaped fixture schemas**
`sim-native-v2/<channel>/<version>`, not evidence of calling real APIs or data
byte-compatible with the v1 renderer. They are frozen creation observations and make no claim of
status refresh. Native files contain only Orders fields; delivery/approval/refund/stock are exported
in separate feeds. The fixture uses USD/scale2 as published in the currency table; no FX and no
adding currencies together. Walmart is a JSONL bootstrap with columns `purchase_order_id`,
`raw_payload`, `fetched_at`; the manifest still pins the **JDBC** boundary. Nothing is loaded into a DB yet,
there is no DB/JDBC source snapshot receipt yet, and that boundary is not replaced by a file.

The operational envelope keeps revision, producer epoch/sequence, causal refs,
effective/observed business clock and recorded execution clock. Sequence follows
the observations of each producer. Native/account identity and stable units do not depend
on SKU mapping or on how many times the renderer splits a record. The business line key uses a versioned
JSON tuple with dataset/seller/Channel/account/native Order/full native line key.
The fact hash excludes the execution-only `recorded_at`; the file checksum/coverage hash covers the whole
emitted record, so two regenerations of the same fact still dedup even if execution time differs.
`require_same_world` requires the same world ID **and** physical outcome digest;
an equal seed is not enough.

## Scenarios exported and checked

- Line A: ordered3, cancel before ship1, delivered2, approve/receive1; gross330,
  discount30, ordered300, canceled100 → headline200. Product Refund100 and a separate
  shipping Refund10 in the same transaction; the refund is not subtracted from the headline.
- Line B: ordered3, two allocations/shipments 2+1, delivered on two different days;
  terminal after the last delivered portion. The delivery renderer can split records with the same unit/time.
- An initial refund knows the Order but lacks the allocation, keeping both lines A/B in the
  uncertainty scope; a correction linking to A arrives late and does not edit the old record. Refund coverage
  revision1 is complete in terms of transport but still has uncertainty; revision2 is clean
  after the correction is observed. The refund interval covers past deadline30d of
  every allocation, while the other feeds close at the follow-up horizon.
- Listing opening before the sale; a business change has effective before observed,
  a correction changes attribution; the sale binding, line ID and old pool reservation stay
  unchanged. Five Storefronts share one pool: opening200/SKU is recorded only once,
  not summed to1000. Reserve/release/ship/receive/quarantine/disposition,
  inbound promise5/receipt2/close short3 and the reconciliation snapshot are all separate feeds.
- `fixture.probes` has role `diagnostic_fixture`, separate from the complete sources:
  retry, hash conflict, release-before-reserve, missing dependency/revision,
  out-of-order stock correction, native current_quantity2 then a full snapshot missing the
  line. These are negative inputs for C/D/E; do not ingest probes as production feeds.

`inspect_records` only checks identity/revision/reference and returns ready/pending/conflict;
it does not apply stock or compute labels. `coverage_receipt` reconciles the emitted set against
expected partitions/sequences/count/digest and a strict cutoff observed<C; a missing
capability returns unavailable, missing data within a claimed capability returns partial.
Complete does not remove uncertainty or by itself prove an official negative.

The three default horizons are creation `D0,D3)`, follow-up `[D0,D90)`, observation
`[D0,D105)`. Physical fulfillment at most4d, qualification30d inclusive, allowance15d,
source observation delay not exceeding15d. Source event overflow or a configured tail
that is too short is rejected before the root is created; never truncate and then grant complete. The manifest
publishes terminal/maturity-deadline/censored counts per cohort, actual max delay,
tail bytes and runtime. Maturity counts are not label eligibility.

## Handoff artifact

New root: `out/source-v2-b-final-20261018-051930425055/`, see
[index `out/source-v2-b-final-20261018-051930425055/index.json` (local, not committed).
Three variants `default`, `render-split` (Amazon schema2/delivery split),
`approve-d31`; each variant has 15 Orders, 30 lines, 2 SKUs, all5 Channels;
total source JSONL **2,511,009 bytes**, 62 files/variant. Default/split share the same
physical world; D29→D31 share the intent digest but differ in outcome/world, used to
check qualification, not to conclude that defects changed. The `out/` output is git-ignored.
A trial root from before the delay cap was tightened is also kept as is, with no resume/restart;
only the `source-v2-b-final-...` root above is the handoff artifact of the current code.

The test [tests/unit/generator/test_generate_order_lifecycle_fixture.py](../../../tests/unit/generator/test_generate_order_lifecycle_fixture.py) went red→green slice
by slice, with assertions from the frozen emitted source and independent reconciliation of quantity/stock/money;
it does not read oracle truth to build expected labels. Next up are C/D/E/J:
Bronze/Silver/JDBC ingestion, projections/reconciliation, label v2/eligibility,
stream recovery and publication receipts. No runtime claim is made yet for these parts,
full scale, Cloud or a five-Channel benchmark for labels/stock.
