# Data & AI Platform

The core definitions of Seller, SKU, Listing, Storefront, Order Line and Return
Label live in the [Data & AI Platform terminology](docs/shared/terminology.md). The entries below
add vocabulary for the Seller-side analytics extension.

## Language

**Data Platform**:
The part of the system that provides normalized sales data, with quality checks and lineage, for analytics and as input to the ML Platform and LLM Platform.
_Avoid_: DE, Data Engineering (as the name of a part of the system)

**ML Platform**:
The part of the system that manages features and the lifecycle of predictive models, from training to serving and monitoring.
_Avoid_: MLE, Feature + ML platform

**LLM Platform**:
The part of the system that provides conversation and Seller assistance through LLMs, document retrieval, and agents that use data and models.
_Avoid_: LLM/Agent platform, layer 3

**SKU Insight**:
Analytical information about how a SKU is doing, helping the Seller notice results
and notable changes when looking at that SKU.
_Avoid_: Product recommendation, return prediction

**Seller Recommendation**:
A suggested action for the Seller based on related data or business information;
distinct from product recommendations for buyers.
_Avoid_: Product recommendation, root-cause conclusion

**Ordered Value**:
Merchandise value after discounts, in the original currency. Headline sales subtract
cancellations before shipping; ordered, canceled and product Refund are separate measures.
Ordered 300 / canceled 100 / Refund 100 gives headline 200. The metric does not confirm money was collected.
_Avoid_: Profit, realized revenue, paid GMV

**Return**:
The buyer **ships goods back** to the Seller (request accepted). Usually comes with a refund, but not always:
exchanges, vouchers/store credit, or returned goods rejected on inspection produce a Return without a Refund.
_Avoid_: Treating Return as a synonym for refund

**Refund**:
**Money goes back** to the buyer. It can come with a Return, or be **refund-only** (returnless refund: the buyer keeps the goods).
Only Refunds of the product component (`component = product`) affect the label; shipping/tax/fee/goodwill do not.
_Avoid_: Assuming every Refund involves goods being shipped back

**Three post-sale cases** (`positive_kind` of label v2):
`return_and_refund` (goods back + money back), `refund_only` (money back, buyer keeps goods), `return_only` (goods back,
no money back). Return and Refund are **two independent events**; "together" means the same Order Line and both within
the 30-day window, without requiring a `return_id` join (not every Channel sends that link).

**Historical Return-or-Refund rate** (`return_or_refund_rate_90d`; formerly `return_rate_90d`):
Share of Order Lines with a positive v2 label among those whose delivery/cancellation is settled, with every
delivered portion mature and full coverage at the cutoff. Always reported with numerator/denominator, the cohort
(by last delivery of the delivered portion) and the label version. It counts refund-only, so it is **not** the rate of
goods physically shipped back; filter by `positive_kind` to separate them.
_Avoid_: Predicted return probability, rate over all newly placed orders, "return rate" (missing the word Refund)

**Return Risk Score**:
A ranking signal that tells the Seller which SKU/Listing to check first, inferred
at order time conditional on the order being delivered. It is a ranking with supporting evidence,
not an unconditional probability for every new order.
_Avoid_: Return probability, certain forecast

**Live View**:
Recent activity derived from the Stream (new orders, Order Status changes, late
Returns), kept separate from official daily figures.
_Avoid_: Official figures, closed report

**Official Daily Figures**:
Numbers closed per complete day, labeled with the data timestamp and the publication
version. When a valid correction arrives, a new publication updates the current numbers and
keeps the old version for traceability.
_Avoid_: Live, real-time

**Publication Version**:
One published, validated set of data containing every required output of the same
refresh; serving requests pin exactly one version so numbers never mix.
_Avoid_: Snapshot, deployment, release

**Dataset Version**:
One fixed run of the data simulator (catalog, cutoff, configuration, history),
identified so that proofs, features and models point to exactly one reproducible dataset.
_Avoid_: Daily snapshot, hidden data version

**Simulated Seller operational sources**:
OMS/WMS/carrier/post-sales feeds exported separately by the simulator, with
authority, native identity links and observation time; they flow through Bronze/Silver.
They do not give every Channel/pool full coverage.
_Avoid_: Real Channel API, ground truth used directly as a feature

**Inventory Pool**:
A group of the Seller's physical stock used to supply one or more Storefronts.
Several Storefronts sharing a pool does not increase its physical stock.
_Avoid_: Channel stock, total stock per Storefront

**Location**:
A place that holds or handles goods for an Inventory Pool, such as a Seller warehouse or a
Channel-operated warehouse.
_Avoid_: Storefront, Marketplace, buyer address

**Reservation**:
The quantity of a SKU in a pool held for a specific demand and not yet
released or shipped.
_Avoid_: Sold, shipped, physical stock decrease

**Inbound**:
Quantity expected to arrive at a pool/location but not yet confirmed as received.
_Avoid_: Available-to-sell stock, received goods

**SKU Review Queue**:
A per-Storefront list of SKUs the Seller should look at, with reason, coverage
and drill-down evidence. The three groups (return risk, inventory/Listing
availability and sales swings) have their own priority order and never take sales actions automatically.
_Avoid_: Order cancellation list, products recommended to buyers, loss probability
