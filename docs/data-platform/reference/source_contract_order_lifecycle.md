# Source contract v2 — Orders and simulated Seller operational sources

> **Pipeline v2 (order lifecycle) — designed and tested locally, not the deployed pipeline.** The pipeline running on AWS is v1 (DP1 → DP4 + ML); see [Data Platform evidence](../evidence.md). Mentions of GCP below are an older plan that was never run; the current cloud target is [AWS](../../shared/aws_deploy.md).

Contract ID **`seller-source-v2`**. Q60/Q61 were approved by the user with
"Understood, proceed": headline sales use the quantity remaining after
cancellation before shipping, and product Refunds are shown separately; simulated
Seller operational sources are added through Bronze/Silver so that label v2 can be
observed. This is a specification to implement,
**not yet a deployed schema/code or runtime proof**. It requires no
real API connections, paid services or new Cloud permissions.

The label policy stays as defined in [return-label-contract](return_label_contract.md);
topology/queue follow expanded-scope-design.
The schemas below are rules for a **simulated source owned by the project**, not
claims about real Channel API schemas. The current state is grounded in the source
code of the renderer/model/RNG, cited in each section; proposed changes are
not described as current code behavior.

## 1. Native boundary and capability matrix

Core Orders must still cover all five Channels. Do not add actual delivery,
approval or outcome to native Orders if the renderer has no such field.
Keep the original payload, schema/version and transport separate. For Orders files, one
row is one Order containing an array of lines; Walmart still reads the source DB through JDBC, and the
boundary does not change to the bootstrap file. Bronze operational feeds have their own grain;
"one row = one Order" does not apply to movements or Refunds.

| Current renderer | Readable Order/line identity | Quantity/amount represented | Delivery/label v2 readable from this renderer |
|---|---|---|---|
| Amazon | `AmazonOrderId`/`OrderItems.OrderItemId` or `orderId`/`orderItems.orderItemId` | Ordered quantity, item amount/discount; shipped quantity is absent in both schemas | No actual delivery per allocation, approval or product Refund success yet |
| Walmart | `purchaseOrderId`, `orderLines.orderLine.lineNumber` | `orderLineQuantity`, PRODUCT charge | Has Delivered status and status time for the current state; not enough allocation/approval/refund history |
| Shopify | `order.id`, `line_items.id` | `quantity`, `current_quantity`, price, total discount | No actual delivery; fulfilled does not stand in for delivered |
| TikTok Shop Batch | `data.orders.id`, `line_items.id` | simulated quantity, original/sale price, seller discount | Delivered status/update time does not prove every allocation; no approval/refund feed in this renderer yet |
| Shopee | `order_sn`, `item_list.order_item_id` | purchased quantity and original/discounted price | Completed/update time is not actual delivery per allocation |

Primary sources in the repo: [Amazon](../../../generator/channels/amazon.py),
[Walmart](../../../generator/channels/walmart.py),
[Shopify](../../../generator/channels/shopify.py),
[TikTok Batch](../../../generator/channels/tiktok_batch.py),
[Shopee](../../../generator/channels/shopee.py). This matrix only draws conclusions about the code
that was read; **it does not conclude that the real APIs lack other capabilities**.
Native fields keep their original names/meanings; `current_quantity` is not automatically mapped
to the original quantity, and a snapshot missing a line does not by itself produce a cancellation.

The native export manifest must record `snapshot_mode` (`full_entity` or
`delta`), `snapshot_scope` (Order IDs or query/partition scope),
`snapshot_as_of`, `page_set_complete`, `schema_id`, `file_set_digest` and
`source_observed_at`. `full_entity` only allows marking a native line absent
within exactly the Order that is complete; it does not cascade-delete shipment/stock/label. A
contradiction with other facts creates a conflict that needs a correction citing the source.

## 2. Ownership and authority of additional sources

Producers use the `sim.seller.*` namespace, which clearly marks the data as simulated.
Each authority is bound to a seller/account/pool scope, a capability profile and a validity period;
two sources must never both be the authority for one measure in the same scope/time.

| Feed ID / producer | Grain and authority | Must not be inferred from the feed |
|---|---|---|
| `seller_oms.line_commitments` / `sim.seller.oms` | State revision of one line: ordered basis, cancellation before shipping, price/discount and native bridge | Does not confirm physical delivery by itself or turn the current native projection into a cancellation |
| `seller_oms.fulfillment_allocations` / `sim.seller.oms` | Binding line↔physical units↔shipment allocation↔pool; terminal quantity | Not treated as delivered when only planned/shipped |
| `carrier.delivery_events` / `sim.seller.carrier` | Actual delivery event of an allocation within the scope confirmed by the source | Promised/estimated time does not replace actual; claims no authority over pools it does not observe |
| `seller_postsales.return_events` / `sim.seller.postsales` | Request/approved/rejected/canceled per return item and allocation | Approved is not receipt/restock; a business cancellation does not erase the approved history |
| `seller_postsales.refund_events` / `sim.seller.postsales` | Transaction/component/status and money allocation backed by evidence | Success is not a physical return; shipping/goodwill is not product money by itself |
| `seller_catalog.listing_versions` / `sim.seller.catalog` | Listing state, SKU mapping, attributes and eligibility by effective/observed time | Does not create line identity; a mapping change does not move stock or old reservations |
| `seller_wms.stock_operations` / `sim.seller.wms` | Opening balance + reserve/release/ship/receive/disposition at pool/location | A snapshot is not a movement; a reservation does not increase/decrease physical on-hand |
| `seller_wms.inbound_events` / `sim.seller.wms` | Promise/ETA, partial receipts and closure of an inbound line | An unreceived promise does not increase available |
| `seller_wms.stock_snapshots` / `sim.seller.wms` | Observation for reconciliation at `effective_at` | Does not overwrite the ledger or add an adjustment to force a match |
| `source.coverage_envelopes` / producer of each feed | Observed scope, complete/partial/unavailable and exceptions | Does not replace linkage coverage; does not prove complete using hidden truth |

The first Seller-fulfilled slice has full authority for the feeds above. A
Channel-managed pool needs its own profile: it may have Orders/catalog while delivery,
stock or post-sales are unavailable. Grant a capability only when the simulator actually
exports an observable feed with source/clock/coverage. Do not automatically grant the right to
observe every Channel-held warehouse just because the world engine knows about them.

Additional sources export JSONL under
`dataset_version=<id>/producer=<producer>/feed=<feed>/export_date=<UTC-date>/`.
Each Bronze row keeps the payload unchanged plus `source_file`, `export_date`,
`ingested_at`, `run_id`; feed/schema/dataset identity is pinned by the source
manifest, never overwritten or disguised as native Orders fields. The source DB keeps the
source snapshot identity and the representation the DB returns; it does not promise the original bytes
before the JSONB source is normalized; see [source boundary](channel_sources.md).

## 3. Envelope, identity and immutable revisions

All operational records share the envelope schema below. IDs are UTF-8 strings;
long native numbers are never coerced to float. Quantities are integer EACH units in the first
slice; money is integer minor units + currency/scale. Timestamps are UTC RFC3339 with
microseconds; coarser native precision is declared, not faked as precise.

```json
{
  "schema_id": "seller-source-v2/return-event/1",
  "dataset_version": "ds-fixture-v2-001",
  "world_id": "world-fixture-001",
  "source_epoch": "sim.seller.postsales:epoch-001",
  "producer": "sim.seller.postsales",
  "feed": "seller_postsales.return_events",
  "source_event_id": "return-r17-approved-1",
  "entity_id": "return-item-r17-1",
  "entity_revision": 2,
  "source_sequence": 204,
  "operation": "lifecycle_event",
  "corrects_source_event_id": null,
  "depends_on_event_ids": ["allocation-a1-delivered-1"],
  "effective_at": "2026-01-29T10:00:00.000000Z",
  "source_observed_at": "2026-01-30T08:00:00.000000Z",
  "clock_domain": "simulation_business_utc:v2",
  "recorded_at": "2026-10-18T04:00:00.000000Z",
  "payload": {
    "seller_id": "seller-1",
    "channel": "amazon",
    "account_id": "account-us-1",
    "native_order_id": "111-1111111-1111111",
    "native_line_id": "10000000000001",
    "allocation_id": "allocation-a1",
    "return_id": "r17",
    "return_item_id": "r17-1",
    "event_type": "approved",
    "quantity": 1,
    "policy_regime_id": "approval-normal-v1"
  }
}
```

Mandatory rules:

- The entity key uses a versioned canonical tuple: dataset + seller + Channel +
  account/shop + native Order + the full native line key. Never concatenate strings
  with an unescaped separator; distinguish null from absent. SKU/mapping is not
  part of the business line ID. Buyers are never linked across Channels.
- `source_event_id` is the identity of one source fact/revision and is immutable on retry;
  uniqueness is per dataset + producer + epoch + event ID. Same ID/hash is a
  duplicate; same ID with a different payload/hash is a conflict. A retry does not create a revision.
- `entity_revision` is an integer increasing within the entity; `source_sequence` is the source
  order within the epoch, not by itself event time or a cross-producer order.
  Timestamps are not used as a tie-break to guess revisions. Gaps are kept pending.
- `operation` is one of `lifecycle_event`, `state_revision`, `correction`,
  `invalidation`. A correction/invalidation must point to the event/revision being corrected,
  with a reason and a new revision; the old record is not overwritten. A business Refund reversal
  is a new event, distinct from an invalidation that confirms an earlier success was wrong.
- `depends_on_event_ids` and operation refs keep causal relations. A release
  arriving before its reserve, or a delivery before its allocation, is kept
  `pending_reference`, not applied by the largest timestamp or by clamping.
  If still missing at the end of the horizon, it becomes a conflict/unknown for exactly that scope.
- A source without native revisions uses source snapshot ID + canonical
  payload digest + observation identity; this is an observation identity
  and must not be called a business order committed by the source.

Current code has deterministic native IDs and RNG per module/day, but
does not enforce this envelope yet; see [base](../../../generator/channels/base.py),
[RNG](../../../generator/core/rngs.py), [model](../../../generator/core/model.py).

## 4. World identity and invariance when the renderer changes

The manifest locks seed, config, catalog/buyer population, semantics/schema
versions, module versions, generator/runtime digests, source profiles and
horizons **before writing**. The dataset version includes the render/transport configuration;
a separate `world_id` pins the entire physical world. Two datasets may only be called
same-world if the manifest's physical entities/outcomes/quantity/time have identical
digests; an equal seed is not enough.

v2 substreams are keyed by stable entity ID + module semantic version + purpose,
for example `(world_id, line_id, returns-v2, intent)` and
`(world_id, return_item_id, approvals-v1, delay)`. Do not use the list of
allocation records as the number of Bernoulli trials. Generate intent/affected stable units
once at line grain; then map units onto physical delivery allocations.
Splitting/merging renderer records must not redraw intent, affected units,
approval time or money. Changing the actual delivery time can change qualification
and requires a different world/scenario. Do not use the future allocation count as an
order-time feature. A per-line p mechanism with a quantity effect must be versioned and its
prevalence checked; never silently re-apply p per unit/allocation.

The [current return engine](../../../generator/core/returns.py) only draws line-level outcomes
and uses request time; [batch exports](../../../generator/delivery/batch_exports.py)
pick snapshots by status. v2 is not automatically byte- or target-compatible with v1.
Comparing v1/v2 labels keeps a same-world scope with full linkage and reports eligibility changes;
never compare metrics across two different worlds and conclude that a model is better.

## 5. Concrete canonical schema for quantity and money

OMS line commitment state, in a `state_revision` envelope:

```json
{
  "native_order_id": "order-1",
  "native_line_id": "line-1",
  "ordered_original_qty": 3,
  "ordered_basis_qty": 3,
  "canceled_unshipped_qty": 1,
  "fulfillment_terminal": true,
  "currency": "USD",
  "minor_unit_scale": 2,
  "ordered_gross_minor": 33000,
  "ordered_discount_minor": 3000,
  "ordered_product_net_minor": 30000,
  "canceled_product_net_minor": 10000,
  "cancellation_operation_ids": ["cancel-op-1"],
  "price_basis_revision": "price-1",
  "listing_binding_id": "binding-line-1-sale"
}
```

The Q60 example in major currency units: gross330 − discount30 = ordered300;
cancel1 net100 → headline current non-canceled200. Product refund100 stays a
separate measure and does not lower the headline to100 by itself. Tax/shipping/fee/goodwill stay
separate components; this headline is never called cash, net profit or settlement.
If the cancellation quantity is unknown, the headline is unknown for the affected portion;
never infer a cancellation from a generic status or from a native quantity reduced by a Refund.

`ordered_original_qty`/amount is the original commitment; `ordered_basis_qty`/amount
is the basis after audited source corrections, without subtracting business Refunds.
Analytics keeps original and corrected basis with clear roles; a restated headline uses
the pinned basis revision. A correction3→2 when delivered3 causes a conflict unless
there is a valid dependent correction; delivered is never edited to match.

Conservation at the cutoff, against exactly the basis revision:

```text
ordered_basis_qty = canceled_unshipped_qty + not_yet_shipped_qty
                  + shipped_not_delivered_qty + delivered_qty
current_noncanceled_qty = ordered_basis_qty - canceled_unshipped_qty
headline_product_sales_minor = ordered_product_net_minor
                             - canceled_product_net_minor
```

Return received and refund quantity/amount are post-delivery measures; do not add
them as new terms in the ordered equation. Cumulative delivered is computed only
from distinct unit/allocation identities; each unit ships once
in the first slice's topology, which has no replacements. Allocation/refund/return
facts are aggregated independently to the line before joining, to prevent fan-out.

`fulfillment_terminal=true` is only trusted once the allocation/cancellation
references are resolved and the quantity equation proves nothing is left
pending. A source flag alone does not override reconciliation. A cancellation event
keeps affected units/qty, amount basis and effective/observed time; a canceled quantity
must not at the same time belong to a shipment that is still in effect.

Money rules: integer minor units per a versioned currency table; no
floats in source v2. Order-level discounts are allocated by largest remainder
with a stable line ID tie-break, recording `allocation_rule_version`; allocated +
unallocated must equal the source total. Product refund allocation is source
evidence; the same even-split formula must not be used to create label linkage. Never add
two currencies together or use `price_factor` as FX. Reconciliation keeps unmatched
amounts and discrepancies separate; amounts are never edited to pass a check.

## 6. Fulfillment, post-sales and label-building fields

Minimum payload of each feed (beyond envelope/seller/account/Channel):

| Feed | Fields required when claiming the capability | Optional, never inferred |
|---|---|---|
| Fulfillment allocation | `allocation_id`, native Order/line refs, `physical_unit_ids`, `quantity`, `shipment_id`, `bound_pool_id`, `bound_location_id`, `binding_version`, `allocation_state` (`planned/shipped/delivered/canceled`) | Carrier ref; delivered is never taken from the planned state |
| Delivery event | `delivery_event_id`, `allocation_id`, `delivered_unit_ids`, `delivered_qty`, `actual_delivered_at`, `event_type=delivered`, authority scope | Signature/attempt data out of scope |
| Return event | `return_id`, `return_item_id`, `event_type` (`requested/approved/rejected/canceled/received/dispositioned`), native refs, affected units/qty, allocation refs or an explicit unresolved scope | Reason code; physical receipt does not replace the approved time |
| Refund event | `refund_transaction_id`, `transaction_revision`, `status` (`pending/failed/success/reversed`), `component` (`product/shipping/tax/fee/goodwill`), positive `amount_minor`, currency/scale, native refs, allocation money rows or unresolved scope | Return ID; refund-only is supported |
| Invalidation/correction | `corrects_source_event_id`, reason, replacement fields/refs, new revision | A correction that does not point to the wrong fact is rejected |

Each transition has its own event ID; `effective_at` is when the transition
actually happened. Example: requestedD29/approvedD31 keeps both; approved is never edited
to D29. A Refund with mixed components exports component rows under the same transaction;
status/success time does not multiply the amount; component subtotal + unmatched equals the
transaction total. Allocation rows contain `allocation_id`, `amount_minor`,
`evidence_ref`; an amount that happens to match is never used to infer an allocation.

For a delivery event, `effective_at=actual_delivered_at`; for approval/success,
`effective_at` is the corresponding qualification timestamp. If the source event only has a
date, or a status time that does not prove the actual transition, the capability is recorded as
proxy/precision-limited and is not automatically promoted to a label-sufficient source at the boundary.

The label transform uses actual delivery per allocation, approval events with qty>0
or product refund success with amount>0, the allocation bridge, terminal quantity,
coverage envelopes and known-time revisions. It does not read `line.label`,
`return_probability`, `defect_propensity`, truth-only delivery or the engine's future
unit assignments. The ground truth oracle has its own namespace/permissions and is
used only to verify the simulator/transform, never as a source feature or as evidence
that a feed is complete. The approval version/policy regime is source metadata
for diagnostics, not by itself an order-time feature before it has been observed.

## 7. Two clocks, coverage and uncertainty

`effective_at` and `source_observed_at` belong to
`simulation_business_utc:v2`. Effective time can be earlier than observation
for late arrivals/corrections. An as-known query at C uses **observed < C**; effective state
at T uses the interval `[valid_from, valid_to)`. `recorded_at`, Bronze
`ingested_at` and actual model artifact creation use `execution_utc`; pacing
has its own manifest mapping, and execution timestamps never replace simulated
knowledge time. The same producer must not backdate observed time on replay.

A coverage envelope is also a source record with a revision/known-time:

```json
{
  "coverage_id": "cov-postsales-us-20260201-r2",
  "producer": "sim.seller.postsales",
  "feed": "seller_postsales.refund_events",
  "profile_id": "seller-operated-label-v2/1",
  "dataset_version": "ds-fixture-v2-001",
  "source_epoch": "sim.seller.postsales:epoch-001",
  "scope": {"seller_id": "seller-1", "account_id": "account-us-1",
            "channel": "amazon", "pool_ids": ["seller-shared-1"]},
  "event_interval": {"start_inclusive": "2026-01-01T00:00:00.000000Z",
                     "end_exclusive": "2026-02-01T00:00:00.000000Z"},
  "capability": "product_refund_success_with_allocation",
  "status": "complete",
  "expected_partitions": ["partition-0"],
  "completed_partitions": ["partition-0"],
  "source_sequence_ranges": [{"partition": "partition-0", "first": 1, "last": 980}],
  "source_record_count": 980,
  "record_set_sha256": "fixture-placeholder-compute-from-emitted-set",
  "unresolved_scope_refs": ["uncertainty-refund-r9"],
  "source_observed_at": "2026-02-02T08:00:00.000000Z",
  "clock_domain": "simulation_business_utc:v2",
  "revision": 2
}
```

The digest in the example is an illustrative placeholder and must be computed from the emitted set when
implemented. `complete` means the transport/source has exported the full scope according to the source
contract, not that every record can be mapped. A complete feed can still have an
unresolved refund. An envelope only takes effect after it is observed; later corrections
create a new envelope revision and recompute affected generations, without
overwriting the old as-known commitment. A sequence range with a gap, a missing partition,
an unparseable record or a producer that has not closed the interval cannot be complete.
The envelope event interval must cover past the inclusive deadline: `end_exclusive`
must be **greater than** the deadline, not merely equal to it. Complete is never granted because
30 days have passed by today or because the feed appears empty.

The **scale** source emits coverage for consecutive weeks, non-overlapping and non-cumulative,
on each operational feed; the last week ends exactly at `lifecycle_followup_end`.
Observed = max(end of week +1 day, max observed of rows in the week +1µs), and lies before
`source_observation_end`. An interval with no events still needs the producer's complete claim
with count0/digest `[]` and all partitions; complete is never inferred from receiving no rows.

The receipt selects rows with the same dataset/feed/producer/epoch, effective within the interval and
observed before the envelope; it checks the canonical digest (sorted by source_event_id), count,
sequence ranges and partitions over **exactly this set**. Ranges can be split into several segments
because sequences are assigned per producer, not per week/feed. Silver joins by UTC day bucket
before the interval predicate so that each scale row falls into only one envelope, avoiding N×number of weeks.
Contiguous envelopes can be merged to cover the label window; the final end must be past the inclusive
deadline. A gap or a partial week is not sufficient coverage. The fixture keeps the old envelopes and
bytes; see ADR-0013.

Minimum uncertainty record:

```json
{
  "uncertainty_id": "uncertainty-refund-r9",
  "source_event_ref": "refund-r9-success-1",
  "reason": "missing_allocation",
  "affected_scope": {"account_id": "account-us-1", "native_order_id": "order-1",
                     "candidate_native_line_ids": ["line-A", "line-B"]},
  "possible_effective_interval": ["2026-01-10T00:00:00.000000Z", "2026-01-11T00:00:00.000000Z"],
  "unallocated_product_amount_minor": 10000,
  "currency": "USD",
  "resolution_state": "unresolved"
}
```

If the Order is known but the line is missing, keep the candidate lines; if the line is known but the
allocation is missing, keep the candidate allocations; if the Order is missing, widen to the
account/feed/time scope proven by the envelope. Narrow only with trustworthy source
constraints or a resolving revision, never by picking on similar SKU/amount. If the scope
cannot be bounded, mark the whole relevant producer scope unavailable/unknown;
never drop the event in order to call other lines negative.

An official label needs feed completeness **and** clean linkage/uncertainty for the
relevant line scope. Report three separate denominators: feeds complete, events linkable,
lines eligible/mature/fully observable. A line with a clear positive but still
missing a required capability does not become a standard training sample; this keeps the
conservative policy already settled. Events that are certainly outside the window/non-product, with
evidence, can be excluded from the affected label scope while keeping the audit trail.

## 8. Three horizons and a configurable tail

The manifest has an exclusive interval and clock domain for all three marks:

1. `order_creation_start/end`: Orders are only created within this range. History and
   stream are slices of the Order creation range, not the date lifecycles stop.
2. `lifecycle_followup_end`: shipment/delivery/cancel/Return/Refund continue
   for existing Orders. No new Orders are created just to keep the stream active.
3. `source_observation_end`: late observations/corrections continue to be received and
   envelopes are emitted after business events; must be later than follow-up if there is delay.

First fixture profile: Order creation `[D0,D395)`, lifecycle follow-up
`[D0,D485)`, source observation `[D0,D500)`, UTC. The D marks are relative days,
not a production SLA commitment. The profile has max fulfillment lag45d,
event qualification window30d, follow-up allowance15d and source delay15d;
it includes events beyond30 to check negatives under the policy. The generator must validate
bounds or record overflow/censored, never truncate and then declare itself complete.

A stress profile with uncapped lag must keep a finite observation cutoff and
right-censored counts; a finite tail cannot prove that every lifecycle has matured.
Every profile publishes the distribution and cap used, maturity/terminal
counts per order-date cohort, unknown/censored reasons and the tail's bytes/runtime.
Example: an Order on D394 delivered D402, approvedD431, observedD434: at D395 it is
immature; after D432 it must still wait for matching observed coverage, never reading truth
to assign early. Stream pacing/execution runtime is computed separately from simulated dates.

## 9. Listing, pool bindings and stock operations

Even a "static" fixture exports the opening listing/pool mapping before the first
Order, with effective/observed time and profile. Never infer "active" just because there is one
Order. Listing version payload:

```json
{
  "listing_id": "listing-X", "native_listing_id": "native-X",
  "seller_id": "seller-1", "channel": "amazon", "account_id": "account-us-1",
  "internal_sku_id": "sku-A", "listing_state": "active",
  "valid_from": "2026-01-01T00:00:00.000000Z", "valid_to": null,
  "mapping_version": "mapping-X-1", "change_kind": "business_change",
  "category_id": "category-1", "attribute_version": "attributes-A-1",
  "eligible_pool_ids": ["seller-shared-1"], "eligibility_version": "pool-X-1"
}
```

A change on D5 reported on D7 differs from a correction confirming the old mapping was wrong: both create a
revision observedD7, but `change_kind`/effective range/reference differ.
A sale line keeps `listing_binding_id` and the mapping attribution **as known at the time of
sale**. Restated analytics has its own attribution revision; prediction never
changes an SKU A feature to B because of a correction learned later. The return rate numerator/denominator
use the same sale-time dimension role and eligibility/cutoff; a current-attribute
view must be named separately. A mapping correction does not create a new line ID/sample.

An allocation/reservation binding pins pool/location/SKU/version at operation time.
Changing the eligibility mapping does not migrate open reservations or stock.
If a correction needs reattribution, it must have explicit old/new binding revisions,
dependent reconciliation and an audit trail, never a current join. One shared Seller pool
serves several Storefronts; Channel-managed pools are separate per owner;
availability is never double-counted and Storefront eligibility is never called quota/ATP.

The stock operation payload uses `operation_id`, `operation_revision`,
`operation_type`, `pool_id`, `location_id`, `internal_sku_id`, the related
`reservation_id`/`inbound_line_id`/`allocation_id`,
`depends_on_operation_ids`, `binding_version`, `quantity` and state deltas:

| Operation | Δ on-hand | Δ reserved | Δ non-sellable | Condition |
|---|---|---|---|---|
| opening balance10 | +10 | 0 | 0 | One opening source revision for the scope |
| reserve3 | 0 | +3 | 0 | available≥3 in the ordered operation sequence |
| ship1 of the reservation | −1 | −1 | 0 | Reservation has at least1; allocation binding matches |
| release2 | 0 | −2 | 0 | Not receipt2 |
| return receive1 pending inspection | +1 | 0 | +1 | Has a linked receipt ID/unit |
| disposition restockable1 | 0 | 0 | −1 | State transition, not received again |
| inbound receipt2 | +2 | 0 | 0 | Receipt2 has an identity; promised5 is not used |

`available = on_hand - reserved - non_sellable` in the first topology; the groups
are disjoint. Reserve/ship is one logical operation emitted by the WMS authority;
OMS references the same operation and does not emit a second movement.
A correction replaces the old operation delta with the new revision when building the projection,
instead of summing all revisions. A business reversal uses a new operation with a
causal ref. Source order/causal refs decide what is applied; a missing dependency stays
pending. A snapshot that differs from the ledger creates a reconciliation record, never a negative clamp
or an automatic adjustment. Oversell/backorder/replacement/transfer network
remain outside the first slice; input outside the capability is marked unsupported/conflict.

## 10. Required/optional profiles and publication

A profile is versioned/pinned **before the build**, bound to a scope list and outputs.
Required items are never reduced after seeing a validation failure. Profiles can
coexist per account/pool; a missing capability does not make the whole system fail forever,
nor is an output silently dropped from the manifest.

| Profile | Required sources / outputs | Optional / unavailable, published |
|---|---|---|
| `core-orders-five-channel/1` | Five native Orders, native identity/schema/snapshot manifests; Bronze/Silver, Orders quantity/amount/coverage, sales headline only in scopes with full Q60 basis | Lifecycle labels/inventory are not claimed from this profile; without basis the headline is unavailable on its own, original native measures still carry provenance |
| `seller-operated-label-v2/1` | Selected core scope + OMS commitment/allocation, carrier actual delivery, post-sales Return and product Refund, envelopes/bridge/uncertainty; official labels, eligibility/unknown diagnostics | Physical receipt/reason optional for the target; missing one of the two positive paths does not prove official0/complete target |
| `seller-stock-triage/1` | OMS binding, catalog/pool history, opening/movement/reservation/inbound receipts, coverage; pool balances/reconciliation/availability queue | Reconciliation snapshot optional when the profile does not claim snapshot parity; ETA optional; unsupported Channel pools receive no stock claim |
| `channel-orders-limited/1` | Orders/catalog scope and declared capability; coverage/unsupported diagnostics | Labels/stock unavailable or provisional per capability; not counted in the all-five-Channel benchmark |

A feed optional in one profile may be required in another. A missing optional
snapshot source is not automatically0; a late/failed required feed keeps the
last-good generation for that profile and creates a failed candidate with a reason. A missing
capability by design differs from a missing partition in a claimed capability. Manifest
output rows have `status=available/unavailable/provisional`, `profile_version`,
`capability_refs`, `coverage_refs`, dataset/input snapshots/contract digests;
fields/outputs are never dropped to make the validator skip checks. UI/API/agent echo scope
and limits. Partial profile publication may only be used if the activation contract
has clearly defined the dependent bundle; generations are never mixed across profiles.

## 11. Fixtures and acceptance conditions

This is an implementation/test checklist; **none of it has run for source v2 yet**. Expected values
must be computed independently from the emitted frozen source set, not using the main transform
or oracle truth as a production source.

| Fixture | Required assertion | IR |
|---|---|---|
| Order on the last day, delivered/approved/observed after creation end; overflow lag | Horizon/tail/envelope correct; mature/censored partitioned; no fill0 | IR04 |
| Renderer adds a field/module and splits delivery records with the same units/time | Same-world outcome digest/quantity/label unchanged; seed-only comparison blocked | IR05/08 |
| Refund only knows Order A+B, then a revision maps it to A | A/B unknown before resolution, A1/B0 once every other condition is met afterwards | IR07/09 |
| Ship3 refund1, native quantity2; full snapshot missing the line | ordered/delivered3 remain; no automatic cancel/release/cascade delete | IR11 |
| Ordered300 canceled100 refund100 | Headline200 with original/canceled/refund separate; tax/shipping not mixed in | IR12/Q60 |
| Listing/pool mapping change/correction while a reservation is open | Line ID/binding stable; no stock teleport; PIT A differs from restated B | IR13/14 |
| Two Storefronts sharing pool10 | Seller total10; eligibility does not become20 or an independently promised ATP | IR15 |
| Release-before-reserve, correction arriving out of order, duplicate/hash conflict | pending/convergent projection or explicit conflict; no clamp/reapply | IR16 |
| Feed lacking a capability vs required feed missing a partition | profile limits/unavailable differ from candidate fail; requirements not changed | IR20 |
| Keep intents, only change approveD29→D31 | Label changes exactly per v2; timing diagnostic explains it, no claim that defects decreased | IR24 |

Items IR04/05/07/08/09/11/13/14/15/16/20/24 have **contract-level enforcement
in this document**; their runtime status is still open. Confirming
Q60/Q61 closes the business/source choice; it does not close the IRs through implementation.
Concrete dependencies, source profile manifests, physical tables and byte/runtime
measurements must be pinned/proven in G0–G2 before full scale.

## Basis and limitations

The quantity/identity design is based on counterexamples with source code from the
cross-domain audit; clock, uncertainty and
same-world are based on the temporal audit.
Issue/priority mapping follows the IR register.
The field policy keeps [label v2](return_label_contract.md) unchanged and does not change the
target to physical return or to a return request before approval. The real APIs
in [channel-sources](channel_sources.md) are separate research with their own confidence
levels/limitations; this contract does not re-verify them or use unverified assumptions
to upgrade local capabilities. No new data/API/Cloud was
executed in the work of writing this contract.
