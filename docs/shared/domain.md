# Business domain overview

This document explains the project's business domain in detail: the terminology, the prediction problem and the data problems the generator must simulate. The README only summarizes it.

## Contents

1. [The big picture](#1-the-big-picture)
2. [Key nouns](#2-key-nouns)
3. [Products](#3-products)
4. [Orders](#4-orders)
5. [The prediction problem](#5-the-prediction-problem)
6. [How data flows in](#6-how-data-flows-in)
7. [Money and time rules](#7-money-and-time-rules)
8. [Data problems the generator must produce](#8-data-problems-the-generator-must-produce)
9. [Amazon-specific extensions](#9-amazon-specific-extensions)
10. [Assumptions and limitations](#10-assumptions-and-limitations)

## 1. The big picture

The project does not build a marketplace like Amazon. It builds **the data system of a single Seller** who sells on several marketplaces at once. This Seller needs to bring data from 5 marketplaces into one place for analysis and prediction.

Running example: the company **ACME** sells hoodies on Amazon, TikTok Shop, Shopify, Walmart and Shopee. Each marketplace returns data in a different shape, and the system consolidates all of it.

All data is **synthetic**. Amazon is the most deeply modeled reference; the other four Channels are modeled more thinly.

## 2. Key nouns

| Term | Meaning | Example with ACME |
|---|---|---|
| **Seller** | The single seller the system serves | ACME |
| **Channel** | An external platform | Amazon, TikTok Shop, Shopify, Walmart, Shopee |
| **Marketplace** | A country-specific store within a Channel (only Channels that sell per country have one) | Amazon DE |
| **Storefront** | "The specific place where ACME sells" = Channel + Marketplace, if any | Amazon DE, TikTok Shop, ACME's Shopify store |

There are 7 Storefronts: Amazon US/UK/DE, TikTok Shop US, Shopify (one store), Walmart US, Shopee VN.

A Marketplace only exists on Channels that sell per country. TikTok Shop has none, so the term **Storefront** is needed to cover every case.

## 3. Products

- **SKU** is a code ACME assigns itself, for example `HOODIE-BLK-M`. It is the common code across all marketplaces.
- **Listing** is the offer of that SKU on one Storefront. Amazon US has its own ASIN, Shopify has its own variant ID, and prices differ too.

One SKU has many Listings. The SKU is what lets us recognize "the same hoodie being sold in 5 places". Listing price and status change over time, which is where the SCD2 dimension sits.

## 4. Orders

- **Order** is an order the marketplace hands to ACME to fulfill.
- **Order Line** is one product line in an order, for example "2 black hoodies, size M".
- **Order Status** is the canonical set of states: `pending_payment` → `awaiting_shipment` → `shipped` → `delivered` → `completed`, plus the side branches `cancelled` and `returned`.

Each marketplace uses different status names (TikTok Shop has `AWAITING_COLLECTION`, Shopify separates payment and fulfillment status). We keep the original status in Silver and have a mapping table to the canonical status.

Two related terms: **Return** is when the buyer sends back an Order Line after delivery; **Refund** is money returned to the buyer (with or without a Return).

## 5. The prediction problem

The ML question: *"Will this Order Line be Returned or Refunded within 30 days of delivery?"*

| Situation | Handling |
|---|---|
| When is the prediction made? | As soon as the order is created |
| Cancelled before delivery | Not counted, since it was never delivered |
| Bought 2, returned 1 | Counted as yes (positive label) |
| Delivered only 10 days ago | Label not known yet, excluded from the training set |

**Why not predict "will the customer buy again"?** Amazon and TikTok Shop hide buyer identity, Shopify can be null for guest checkout, and only Walmart returns it fully. So there is no `customer_id` that can be shared across the 5 Channels.

That is why the Online Feature Store looks up by **`sku_id` + `storefront_id`**, for example "90-day return rate of the black hoodie on Amazon DE".

Labels arrive 30 days later. This is exactly the situation of "having to monitor drift before the ground truth is available".

## 6. How data flows in

- **Batch**: 180 days of history for all 5 Channels, each Channel in its own format, placed in MinIO/PostgreSQL to be pulled into Bronze.
- **Stream**: order status change and return events, flowing through Kafka and Flink. Each marketplace "breaks" in its own way:
  - TikTok Shop and Shopify (webhooks): often send duplicates.
  - Amazon (notifications): often arrive late.
  - Walmart and Shopee (assumed polling): arrive in batches.

The Seller side has **no clickstream** (views, carts), since that is the marketplace's internal data.

## 7. Money and time rules

- Original amounts are kept with their original currency code, plus a column converted to **USD** at the daily exchange rate.
- Shopify has two currencies per order (the store currency and the currency the customer paid in); both are kept.
- Times are stored in **UTC**, separating `event_time` (when the event happened) and `ingested_at` (when the system received it). The gap between the two is late arrival.

## 8. Data problems the generator must produce

| Problem | Details |
|---|---|
| **Skew** | Orders by Storefront: Amazon US 45%, TikTok Shop US 15%, Amazon UK 12%, Amazon DE 10%, Shopify 8%, Walmart US 7%, Shopee VN 3%. A few SKUs account for most of the sales (Zipf). 8 categories with different return rates, apparel about 22%, household goods about 4% |
| **High cardinality** | 2,000 SKUs are kept. High cardinality is in the Order IDs (about 720 thousand), Order Line IDs (over 1 million), tracking numbers and per-Channel buyer IDs |
| **Schema evolution** | From a fixed date, Amazon order data switches from the Orders v0 model to Orders 2026-01-01: renamed fields, changed structure, and new fields, so older partitions are null |
| **Offline duplicates** | About 2% of rows are duplicated due to overlapping export days, including exact duplicates and newer corrected versions |
| **Data drift** | (1) Covariate: a Prime Day-style sale abruptly changes the discount level and SKU distribution on the Amazon Storefronts. (2) Concept: TikTok livestreams almost double the apparel return rate while the features still look normal |
| **Duplicates, bursts, late arrivals (stream)** | Depending on each Channel's characteristics, as in section 6 |

All numbers are generator config and can be increased when running on the cloud. For the detailed design see [generator-design.md](../data-platform/reference/generator_design.md).

## 9. Amazon-specific extensions

Amazon is modeled more deeply than the other Channels:

- **Fulfillment Method** (who ships: the Seller or the Channel, as in FBA/FBM), applied to all 5 Channels because it strongly affects the return rate.
- Amazon's own **Return reason codes**.

Fees, settlements, advertising and FBA inventory are not covered at this stage and can be added later.

## 10. Assumptions and limitations

- **Verification level of each Channel's data shape**: orders and returns for all 5 Channels have been checked against official documentation. Some details are still *assumptions* (for example the actual delivery date for Amazon and Shopee, and Amazon notifications). For the full list, with sources and confidence levels, see [channel-sources.md](../data-platform/reference/channel_sources.md).
- **Fully synthetic data**: distributions (Storefront shares, return rates by category) are generator parameters, not real figures.
- **No clickstream**: view and cart data is the marketplace's internal data, outside the Seller's point of view.
