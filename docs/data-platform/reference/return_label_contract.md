# Return Label v2 contract — Q56/Q57

The policy is settled. ID: **`return-or-refund-line-v2`**.
ADR-0010 records the trade-offs;
this file specifies the expected behavior for writing sources/transforms/validation.
The current generator code and label bridge have not been replaced yet.

## Unit of measure and conditions

- One sample is one Order Line, identified by dataset + Channel/account namespace +
  full Order/line identity; multiple units/shipments do not multiply the number of samples.
- Each delivered portion has an allocation ID, a delivered quantity and an observed actual delivery
  time. A Return/Refund event must link to exactly the line and
  allocation being evaluated; the allocation is never guessed from the most recent delivery.
- A line becomes eligible for the maturity check only when its delivered quantity is >0 and the remaining part
  has been delivered or cancelled. A line fully cancelled before delivery has no label; a line with a
  still-unsettled part is immature. A partial cancel does not create a fake delivery date.
- An approved Return request with a valid goods quantity >0, or a successful product
  Refund with amount >0 and a trusted allocation, is a positive event. A raw request
  not yet accepted, a rejected request, a pending/failed Refund, a shipping-only Refund, or a
  fee/goodwill amount not identified as a product refund does not by itself create a positive.
- Event time is **when the Return request was accepted** or **when the product
  Refund succeeded**; request creation/pending time is never used instead.
  Physical receipt of the Return, inspection and restock are separate events.

## Return ≠ Refund — three cases and how the label handles them (settled)

In real life, Return (goods come back) and Refund (money goes back) are **two independent events**:

| Case | Goods back? | Money back? | Label | `positive_kind` |
|---|---|---|---|---|
| Return + Refund | Yes | Yes | 1 | `return_and_refund` |
| Refund-only (buyer keeps the goods) | No | Yes (product) | 1 | `refund_only` |
| Return-only (exchange, voucher, rejected after inspection) | Yes | No | 1 | `return_only` |
| Neither within the window | — | — | 0 | null |

- The label is **deliberately combined**: it measures "the Order Line had a post-sale problem", not just "money was lost". Return-only is still positive (goods coming back
  still cost shipping/handling and are a quality signal).
- A **partial** product Refund (amount > 0) is still positive; goodwill/fee/shipping are not. A % threshold (if needed) would be
  a new label version, not a change to v2.
- `positive_kind` has a value only when `label = 1`; it is computed over positive events **within the window** and **visible before
  the cutoff** for the same line. Matching is by line + window, not yet by `return_id`; a stricter match will be considered only
  once every Channel provides that link. The label value does not change ⇒ still `return-or-refund-line-v2`.
- The feature is renamed `return_or_refund_rate_90d` so the name does not suggest "returns only".

## Two clocks and the window

For allocation a delivered at d[a], its window is:

```text
d[a] <= qualifying_event_time <= d[a] + 30 × 24 hours
```

All timestamps are UTC with the precision of the source contract; they are never reduced to a
calendar-day difference. Use the inclusive bound at exactly 30 days, with fixtures just
before/exactly at/after the deadline. A snapshot query uses an exclusive cutoff C: it selects only
source records with `source_observed_at < C`. Date ranges are [start, end).
To include an event exactly at the cutoff, use a later snapshot/cutoff.
An adapter with an inclusive timestamp condition must map this bound explicitly
according to the source precision; never assume the backend's PIT join uses the same bound.

`maturity_end = max(d[a] + 30 days)` over the actually delivered portions considered.
A line enters train/eval or the official rate only when fulfillment is terminal,
`C > maturity_end`, the terminal information was known before C, and the observed feeds
have coverage/completeness at least through every relevant deadline. Coverage must
point to the source's manifest/watermark/observation envelope; the simulator's hidden truth
must not be used as a feature to prove a feed is complete.

Positives and negatives wait for the same conditions. The Live View can show an
approved Return early, but an early positive alone does not enter the standard denominator.
If the source cannot prove coverage, the unknown/provisional state
must be explicit; 30 wall-clock days are never taken as data completeness.

When a line is eligible, mature, and has sufficient coverage and linkage:

```text
label = 1 if there is at least one qualifying event within its allocation window
label = 0 if there is no such event
cohort_date = UTC date of the last delivery of the actually delivered portion
```

Not yet due → immature; no delivery → ineligible; conflict/missing
linkage/feed → unknown. This state is kept separate from the nullable label value;
unknown is never encoded as 0. A line with a clear positive but still missing required
data still does not enter the standard train/eval set under a "complete" label.

## Cancellation, reversal and correction

The v2 target is **a valid event has occurred** within the window, not the final
state of the post-sale process. So a request that was accepted and then
cancelled through the normal process still records the historical approved event; a refund
success followed by a money reversal is two different events, and the reversal does not erase the
fact that a refund happened. Lifecycle/analytics show the later state as well.

Conversely, a source correction confirming that an approval/success was never valid,
or correcting delivery/allocation/time, must invalidate/recompute the corresponding event/label.
If the source does not distinguish a business reversal from a data correction,
keep unknown/conflict and do not infer. This is how the historical-event criterion
of Q57 is implemented together with the Q36 restatement; it is not turned into a physical-return label.

A correction may be used only from the time it is observed: a new generation recomputes,
old generations stay unchanged per retention. An old as-of feature/training set must not
see a correction known after its cutoff. A new snapshot may move mature
back to immature/unknown if delivery/coverage is corrected; an old label is not kept just
because it was once materialized.

## History, coverage and follow-up

### Label history and uncertainty must follow the source

A single `label_available_at` does not replace the full revision history. Each revision
needs the label policy version, state/value, source snapshot/event references,
observation validity, terminal/coverage evidence and the reason for change. Keep the
materialized training dataset immutable: the feature as-of at order placement is distinct from the
label observation cutoff before training; outcome evaluation has its own cutoff.
Do not filter labels by the feature cutoff and thereby drop all valid future outcomes.

An unmatched product Refund/Return must keep its **affected scope**: if only the Order
is known, keep the lines that may be related; if the line is known, keep the allocations
that may still be related. Never inner join away the unmatched records and then prove
the remaining lines negative. Feed completeness differs from linkage coverage; a missing Order
identity can create a larger uncertainty region according to the source envelope. Never
guess an allocation because amounts happen to match. The settled conservative unknown rule
is applied before concluding an official label0/1.

Sources of actual delivery/approval/refund/allocation need explicit capability/authority.
Q61 settled the simulated Seller operational sources per the
[source contract](source_contract_order_lifecycle.md) and the issue log;
promised delivery is never used in place of
actual, and hidden truth is never turned into a Seller-observable source.

### Consecutive coverage

Each required kind needs the union of `complete` envelopes, with `complete` receipts,
an `evidence_ref`, no unresolved scope and observed before the cutoff, continuously covering
`[min(d[a]), max(d[a])+30 days]`. Sort intervals by start and extend end whenever start
does not exceed the covered end. The final end must be **greater than** maturity because end is exclusive.
A single envelope covering everything is a special case; any gap, however small, is unknown. Revisions and
corrections are applied as-known like events; a superseded old claim is not used.

Scale uses consecutive weeks `[start+7k, min(start+7(k+1), followup_end))`;
no single envelope is required to cover the whole window. Each
envelope's digest/count/sequence checks only rows of the right feed/producer/epoch/dataset, effective within the interval,
observed before the envelope. A sufficient receipt does not remove uncertainty about linkage.

### The follow-up lifecycle does not stop when Order generation stops

Separate `order_creation_end`, `lifecycle_followup_end` and
`source_observation_end`. A line placed near the last day may be delivered late,
qualify later still and be observed even later; the end of 30 days of stream does not by itself
give maturity/coverage. The tail must be long enough per the scenario/lateness contract, or
the line is recorded as immature/censored/unknown with an explicit numerator/denominator. Do not generate new
Orders just to extend follow-up for a chosen cohort.

The generator must not redraw an independent outcome just because one allocation is split
into two records of the same physical world; OR-ing two Bernoulli p0.1 gives 0.19,
which is not equivalent to the original p0.1. The outcome/quantity mechanism must be explicitly versioned and
must not leak the future shipment count/approval delay into the feature at order placement.

## Label fixtures with independent expected output

D0 is a UTC timestamp, C is after every maturity in the case unless stated otherwise; feeds
are complete and quantities terminal, linkage correct unless the case states something is missing.

| Case | Facts | Expected |
|---|---|---|
| L01 | Delivered D0 and D20; approved Return of the D20 portion at D45 | 1; cohort D20; maturity_end D50 |
| L02 | Delivered D0 and D20; approved Return of the D0 portion at D45; no other event | 0; outside the D0 portion's window, even though after the last delivery |
| L03 | Delivered D0; request D29 but accepted D31 | 0; event qualification at D31 |
| L04 | Delivered D0; product Refund success exactly at D30 | 1 if the source was observed before C and coverage is sufficient |
| L05 | Delivered D0; pending D28, failed D29, success D32; no Return | 0; pending is not success |
| L06 | Shipping-only Refund D10; no Return/product Refund | 0 |
| L07 | 3 units: 1 delivered at D0, 2 cancelled; no positive event, C>D30 | 0; one sample, delivered1/canceled2, cohort D0 |
| L08 | 1 delivered at D0, 2 still pending; approved Return D5 | immature; not in the standard set yet |
| L09 | Two allocations with different deadlines; Refund maps to the line but the allocation is missing | unknown; do not guess the window |
| L10 | Approved D29 but known only at D35; C=D31 and feed not complete | unknown/provisional; not called an official 0 |
| L11 | Case L10, C=D40, full coverage | 1; event time within the window, known-time respected |
| L12 | Approved D10, then buyer cancels the request D12; no approval correction | 1; target is the historical approved event |
| L13 | Approved D10, source correction at D40 confirms it was wrong data | The generation before the correction keeps the as-known result; the generation after recomputes 0 if eligible |
| L14 | Whole line cancelled before delivery | ineligible, label null |
| L15 | No event, maturity_end=D30 and C exactly D30 | immature; the snapshot does not yet cover the final bound |
| L16 | Order with A/B, product Refund success within the window but only the Order ID is known | A/B unknown if both may still be related; once resolved to A, recompute A1/B0 when other conditions hold |
| L17 | Input revisions in turn prove label0, a late qualifying event makes it 1, a delivery correction makes maturity insufficient | Querying each observation snapshot returns 0/1/immature correctly; the old training dataset keeps its checksum |
| L18 | Earlier weeks sufficient; the last two weeks `[D21,D28)` and `[D28,D35)` are contiguous, deadline D30 | Coverage sufficient; label follows the events |
| L19 | Like L18 but missing week `[D14,D21)` of one required kind | unknown |
| L20 | Like L18 but one week of one kind has partial status/receipt | unknown |

The cases above have pure Python regressions; L01–L17 keep their expected values.

## Migration, API and acceptance

The version must appear on the label dataset, feature/training manifest,
MLflow run/model metadata, serving compatibility and metric definitions.
A v1 model may be kept as old evidence; a v2 tag is never written onto a v1 artifact.
A v1/v2 comparison must include a table of eligibility/cohort/positive changes with reasons on
the same identity/dataset; a metric for a different target is not to be read as a better model.

The generator currently uses requests and a single Order-level delivered_at. V2 needs
sufficiently observed shipment/approval/refund sources and a bridge per line/allocation; it is not
enough to add a `label_version` column to the current label. Labels/outcomes are used only for
train/eval and mature analytics, never as features at order placement.

S19/S20 together with S27/S28 check this contract; L01–L17 are expected cases to be
implemented later, with no runtime test run passing yet. Coverage/freshness values,
source schemas, physical tables and dependency pins still have to complete G0–G2.
