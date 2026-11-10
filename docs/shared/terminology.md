# Data & AI Platform Multi-channel Seller Data Platform

A Seller-side data platform that collects sales data from several external e-commerce platforms (Amazon, TikTok Shop, Shopify, Walmart, Shopee) into one lakehouse. All data is simulated, with Amazon as the reference model.

The source model extends to partial lifecycle feeds, Listing history,
a small inventory model and review queues. Inventory terms are defined in
[GLOSSARY.md](../../GLOSSARY.md). Label v2, the pool topology and the
three-group queue are settled. Current definitions follow the [label contract](../data-platform/reference/return_label_contract.md)
and the extension design; older code/proofs still follow the
original contract until they are migrated with a new version.

## Language

**Channel**:
An external sales platform the system collects data from (Amazon, TikTok Shop, Shopify, Walmart, Shopee).
_Avoid_: Platform, marketplace site, source, store

**Marketplace**:
A country-specific storefront inside a Channel that sells by country, e.g. Amazon DE. Not every Channel has one (TikTok Shop does not sell by Marketplace).
_Avoid_: Site, region, store, country

**Storefront**:
A concrete sales presence of the Seller on a Channel: the Channel plus, if the Channel has one, the Marketplace. Examples: Amazon DE, TikTok Shop, a Shopify store.
_Avoid_: Shop, store, site

**Seller**:
The single merchant that owns every Storefront the system serves.
_Avoid_: Vendor, merchant, tenant, brand

### Products

**SKU**:
The product code assigned by the Seller; the canonical cross-Channel key.
_Avoid_: Product, item, ASIN, variant

**Listing**:
The offer of one SKU on one Storefront, with its own price and status (Amazon US has an ASIN, Shopify has a variant ID). One SKU has many Listings.
_Avoid_: Offer, product page

### Orders

**Order**:
The unit a Channel hands to the Seller to fulfill (Amazon `AmazonOrderId`, Walmart `purchaseOrderId`, a Shopify, TikTok Shop or Shopee order). Contains one or more Order Lines.
_Avoid_: Purchase, transaction, customer order

**Order Line**:
One product with a specific quantity inside an Order.
_Avoid_: Item, line item, order item

**Order Status**:
The canonical, Channel-independent status of an Order: `pending_payment` → `awaiting_shipment` → `shipped` → `delivered` → `completed`, plus two side branches `cancelled` and `returned`. The Channel's native status is kept separately and mapped to the canonical status.
_Avoid_: State, fulfillment status, financial status

**Return**:
The buyer sends back an Order Line after delivery.
_Avoid_: RMA

**Refund**:
Money returned to the buyer for an Order Line, with or without a Return.
_Avoid_: Chargeback, reimbursement

**Fulfillment Method**:
Who is responsible for delivering an Order Line: the Seller or the Channel (like Amazon FBM and FBA). Applies to every Channel.
_Avoid_: Shipping type, logistics mode

### Prediction labels

**Return Label**:
The v2 label of an Order Line: positive if there is an accepted Return request or a successful product Refund within 30 days of delivery of the related portion. Official positives/negatives both wait for delivery/cancellation to settle, for maturity and for coverage; undelivered lines have no label, and missing data is unknown.
_Avoid_: Churn, target

**Delivered At**:
The time a portion of goods was actually delivered to the buyer; an Order Line delivered in several shipments has several such timestamps. Label v2 evaluates a window per delivered portion, while the line cohort uses the last delivery date of the delivered portion once the remaining quantity is delivered/cancelled; source coverage must be published explicitly.
_Avoid_: Delivery date, arrival date

**SKU Defect Propensity**:
A SKU's tendency to be returned because of the product itself (too small, defective, not as described), the same across every Storefront. Only the generator knows it; it never appears as a data column and can only be inferred from Return history.
_Avoid_: Quality score, defect rate

### Data timeline

**Cutoff**:
"Today" in the simulated world. Everything before the Cutoff belongs to the Batch, everything after belongs to the Stream.
_Avoid_: Snapshot date, split date

**Export Date**:
The date a Channel exports a batch of data to the Seller. Data is partitioned by this date rather than by Order creation date, because overlapping exports and schema changes are tied to the export date. One Order can appear in several Export Dates.
_Avoid_: Load date, ingestion date, partition date

**Sale Event**:
A promotion that abruptly shifts Order volume, discount levels and SKU distribution on one or a few Storefronts, e.g. Amazon Prime Day. A source of covariate drift.
_Avoid_: Promotion, campaign, flash sale

**Batch**:
The history of every Channel up to the Cutoff, each Channel in its native format, received once.
_Avoid_: Backfill, historical load

**Stream**:
The sequence of events after the Cutoff in the same simulated world as the Batch: new Orders, Order Status changes for Orders already in the Batch, and late Returns.
_Avoid_: Replay, live feed

### Data layers (Medallion)

**Bronze**:
A faithful copy of the data a Channel delivers to the Seller, only converted to a columnar format with technical columns added (Export Date, source file name, load time). No column renames, no schema merging, no deduplication. Several schema variants of the same Orders feed within a Channel live together; key naming belongs to Silver. Other feeds were added later, so lineage must distinguish both Channel and feed (Orders, Shipment, Return, Inventory, Listing); do not read the old Orders rule as "each Channel has only one table". Tables in this layer use the `raw_` prefix.
_Avoid_: Raw, landing, staging

**Silver**:
Data cleaned and mapped to the system's common language: schema normalized per feed, statuses mapped and duplicates removed by identity/version. Current projections are kept distinct from the observation history used for as-of queries; grain follows the entity or event (Order, Order Line, Shipment, Return, Inventory...), and history is never deleted just to keep the latest row. Tables in this layer use the `stg_` prefix.
_Avoid_: Cleansed, conformed, curated

**Gold**:
Tables that answer one specific business question, pre-aggregated or pre-joined: e.g. revenue per Storefront per day, Return rate per SKU, or the feature table for the Return Label. End users and models read this layer, not Silver. Tables use a role prefix: `dim_` for dimensions, `fact_` for facts, `feat_` for features, `obt_` for single-purpose one-big-tables.
_Avoid_: Mart, serving layer, aggregate
