# Sources and confidence levels for each Channel's data shape

This document records which documentation the generator relies on to model each Channel's native data shape, which fields are verified and which are only assumptions. Research was done on 2026-09-21.

## Table of contents

1. [How to read confidence levels](#1-how-to-read-confidence-levels)
2. [Amazon](#2-amazon)
3. [Shopify](#3-shopify)
4. [Walmart](#4-walmart)
5. [TikTok Shop](#5-tiktok-shop)
6. [Shopee](#6-shopee)
7. [Consequences for the generator](#7-consequences-for-the-generator)

## 1. How to read confidence levels

| Level | Meaning |
|---|---|
| **High** | Read directly from the Channel's official documentation page or model during the research session |
| **Medium** | From a secondary source (open-source SDK, blog, search result summary), not yet cross-checked against the official page |
| **Low** | Inferred or recalled from before, with no source in the research session. Not used for modeling until verified |

Principle: the generator only models fields at High or Medium level. Every Medium field is marked `assumption` in the config and in the manifest.

## 2. Amazon

**Orders v0.** Source: the official model `ordersV0.json` in the repo `amzn/selling-partner-api-models`. Level: **High**.

- Order: `AmazonOrderId` (3-7-7 format), `PurchaseDate`, `LastUpdateDate`, `OrderStatus`, `FulfillmentChannel` (`AFN` or `MFN`), `NumberOfItemsShipped`, `NumberOfItemsUnshipped`, `BuyerInfo`, `ShippingAddress`, `IsReplacementOrder`, `IsPrime`, `IsBusinessOrder`.
- `OrderStatus`: `PendingAvailability`, `Pending`, `Unshipped`, `PartiallyShipped`, `Shipped`, `InvoiceUnconfirmed`, `Canceled`, `Unfulfillable`.
- OrderItem: `OrderItemId`, `ASIN`, `SellerSKU`, `Title`, `QuantityOrdered`, `QuantityShipped`, `ItemPrice`, `ItemTax`, `PromotionDiscount`.

**Orders 2026-01-01** (new version, same repo). Level: **High**.

- Two operations: `searchOrders` and `getOrder`. Identifiers and times: `orderId`, `createdTime`, `lastUpdatedTime`.
- Fulfillment statuses: `PENDING_AVAILABILITY`, `PENDING`, `UNSHIPPED`, `PARTIALLY_SHIPPED`, `SHIPPED`, `CANCELLED`, `UNFULFILLABLE`.
- Has `fulfillment.shipByWindow`, `fulfillment.deliverByWindow` (estimated windows), `packages`, `orderItems[].product` (ASIN, SKU), `proceeds`, `promotion`, `cancellation`.

**Used for schema evolution.** The 2026 shape has **no new fields** — every key traces back to a v0 key, only renamed, nested, retyped or dropped. Renamed: `AmazonOrderId` to `orderId`, `PurchaseDate` to `createdTime`, `LastUpdateDate` to `lastUpdatedTime`, and `OrderTotal` to `proceeds.grandTotal` (one more level of nesting). Changed structure and value spelling: `OrderStatus: "Shipped"` to `fulfillment.status: "SHIPPED"`. Changed type: `IsPrime` (bool) to the array `programs`. Dropped keys (v0 only): `NumberOfItemsShipped`, `NumberOfItemsUnshipped`, `IsBusinessOrder`, `IsReplacementOrder`, `OrderItems[].QuantityShipped`. The version switch date in the generator is chosen arbitrarily, not Amazon's real deprecation schedule (that schedule is not verified).

**Notification.** The generator assumes Amazon sends order status change notifications and that notifications often arrive late. The payload and delivery mechanism were not read from official documentation. Level: **Low**, recorded as an assumption.

**Important gap:** both Orders versions only have an **estimated** delivery date, no **actual** delivery date and no "delivered" status. Other sources were searched:

- **Seller-fulfilled orders (MFN):** `EarliestDeliveryDate` and `LatestDeliveryDate` in Orders, which are estimates. Level: **Medium** (discussion in the official repo, a maintainer confirmed "not the actual delivery date").
- **FBA orders:** the report `GET_AMAZON_FULFILLED_SHIPMENTS_DATA_GENERAL` has `shipment-date`, `estimated-arrival-date`, `carrier`, `tracking-number`. Level: **High** (official Reports page), the date is still an estimate. This report is restricted, needs a Restricted Data Token, and covers at most 1 month per request.
- **Actual tracking:** `getTracking` only applies to packages bought through Amazon Buy Shipping, kept for at most 45 days. Level: **Medium** (search results). Not applicable to FBA or to Sellers using their own external carrier.

Conclusion: SP-API does not provide the actual delivery date for every order. See section 7 for how the generator handles this.

**Order reports.** Source: the official FBA Reports page. Level: **High**. `GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL` (orders updated within a date range, used for order tracking) and `GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL` (orders placed within a date range, all statuses). The "by last update" variant is the natural source for overlapping exports in the offline duplicate scenario.

**Returns report.** Source: the official Returns Reports page. Level: **High**.

- `GET_FLAT_FILE_RETURNS_DATA_BY_RETURN_DATE` (TSV): `Order ID`, `Order date`, `Return request date`, `Amazon RMA ID`, `ASIN`, `Merchant SKU`, `Item Name`, `Return quantity`, `Return Reason`, `Refunded Amount`. At most 60 days per request. There is an equivalent XML version (`GET_XML_RETURNS_DATA_BY_RETURN_DATE`) that adds `return_reason_code` and `label_details` (`tracking_id`, `return_carrier`, `label_cost`).
- `GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA`: returned goods that have arrived at the FBA warehouse, with `order-id`, `reason`, `disposition`. Updated daily. The full column list is not available yet.
- There are also reports for Seller Fulfilled Prime (`GET_CSV_MFN_PRIME_RETURNS_REPORT`, `GET_XML_MFN_PRIME_RETURNS_REPORT`), not used by the project.
- Both main sources have the return request date but not the date the goods reach the Seller, so `Return request date` is the label's timestamp.
- The generator uses the flat-file report for MFN orders and the FBA Returns report for FBA orders. This split is based on inference: the flat-file report has an RMA and a prepaid return label, which is characteristic of Seller-fulfilled orders. Level: **Low**, recorded as an assumption.

## 3. Shopify

**Order.** Source: the official Admin REST API documentation page, Order resource. Level: **High**.

- Order: `id`, `name`, `email`, `created_at`, `updated_at`, `closed_at`, `cancelled_at`, `cancel_reason`, `customer`, `shipping_address`, `total_price`, `currency`, `presentment_currency`.
- `financial_status`: `pending`, `authorized`, `partially_paid`, `paid`, `partially_refunded`, `refunded`, `voided`.
- `fulfillment_status`: `fulfilled`, `partial`, `not_eligible`, `restocked`, `null`.
- Line item: `id`, `sku`, `variant_id`, `product_id`, `quantity`, `current_quantity`, `price`, `price_set`, `total_discount`, `fulfillment_status`.
- Refund: `id`, `order_id`, `created_at`, `processed_at`, `refund_line_items`, `transactions`.

**REST is legacy.** Source: Shopify's official changelog, via search results. Level: **High** for the fact, **Medium** for the details. The REST Admin API has been marked legacy since 2024-10-01. Since 2025-04-01 every new public app must use GraphQL. Existing apps may still use REST; no migration timeline was found. The 2026 status is not verified.

There is no "delivered" in Order. Delivery status lives in a separate fulfillment object. The generator assumes Order has a `fulfillments` list carrying the delivery status. The field names were not read from official documentation, level **Low**.

Duplicate webhooks: there are community discussions about Shopify calling a webhook several times for one order. Level **Medium**. The identifier of each webhook delivery (for dedup) is not verified, level **Low**.

**Buyer.** `customer` can be null for guest checkouts.

**Batch renderer in the repo.** `generator/channels/shopify.py` emits one
REST `order` response per JSONL line, including `line_items` and `customer=null` for
guests. It keeps the pre-discount `price` and each line's `total_discount`;
`fulfillment_status` only says the goods were fulfilled and does not imply an actual delivery
date. The REST shape was chosen for the existing simulated dataset; when connecting a real Shopify
app, the GraphQL requirement for the API version in use must be revisited.
The small source export in `out/channels-small` has gone through DP1 Bronze with 910 responses.

Cross-check against the [Shopify Order resource](https://shopify.dev/docs/api/admin-rest/latest/resources/order).

## 4. Walmart

**Order.** Source: the official Walmart Marketplace documentation pages (Get all orders, Order management event). Level: **High**.

- Order: `purchaseOrderId`, `customerOrderId`, `customerEmailId`, `orderDate` (Unix epoch), `buyerId`, `isGuest`, `orderType` (`REGULAR`, `REPLACEMENT`, `PREORDER`), `shippingInfo`.
- Order line: `orderLines.orderLine[]` with `lineNumber`, `item` (`productName`, `sku`, ...), `charges` (`chargeType`: `PRODUCT` or `SHIPPING`, `chargeAmount`: `{currency, amount}`, `tax`), `orderLineQuantity`, `orderLineStatuses`, `statusDate`.
- Status: `Created`, `Acknowledged`, `Shipped`, `Delivered`, `Cancelled`.
- Webhook `ORDER_UPDATES`: `source.eventType`, `source.eventTime` (ISO 8601 UTC), `source.eventId` (idempotency key). Sub-events: `PO_CREATED`, `PO_LINE_AUTOCANCELLED`, `INTENT_TO_CANCEL`, `FRAUD_CANCEL`.

**Return.** Source: the official documentation page "Retrieve return order details". Level: **High** for field names, **Medium** for enums (the page only has a sample).

- `returnOrderId`, `customerOrderId`, `returnOrderLines` (SKU, reason, refund status, tracking), `status`, `totalRefundAmount`, `returnOrderDate`, `returnByDate`, `refundMode`, `returnType` (e.g. `REPLACEMENT`).
- Status values seen in the sample: `REFUND_INITIATED`, `REFUND_COMPLETED`, `NOT_SHIPPED`, `COMPLETED`. No complete list yet.

Adjustment from the old design: Walmart has both webhooks and polling. Using polling for Walmart is a **design choice** of the project, not a limitation of the Channel.

### Trust boundary: Walmart's source is PostgreSQL, not the JSONL files

**a) The Walmart data path.** Walmart data goes through three hops, each held by a different party. Telling the three hops apart is what tells you where the real "source" is.

```
  JSONL files                PostgreSQL                  Spark                  Bronze
  /data/raw/pg-load/         walmart.orders              (read via JDBC)        raw_walmart_orders
  walmart.orders/            (another department's DB)                          (lakehouse.bronze,
  export_date=*/             purchase_order_id,                                 Iceberg)
  part-*.jsonl               raw_payload JSONB, fetched_at

        └───── hop ① ─────►┘         └───── hop ② ─────►┘        └───── hop ③ ─────►┘
        generator load-postgres      Spark reads, read-only     Spark writes to Iceberg
```

- **Hop ①** — `generator load-postgres` loads the JSONL files into the table `walmart.orders`. This loads data into another department's DB and runs only once when building the source.
- **Hop ②** — Spark reads `walmart.orders` via JDBC. The data team **only reads**; it does not own or modify the table.
- **Hop ③** — Spark writes the Bronze table `raw_walmart_orders` on Iceberg. This is what the system owns.

**b) Which hop is the real source, and why.** The real source is **`walmart.orders` in PostgreSQL** — the boundary sits right between hop ① and hop ②. Reason: PostgreSQL is **another department's DB**; the data team **does not own** it and **only reads** from it. The JSONL files are just the vehicle for loading that DB. Since it does not own the DB, the data team cannot change how the source stores data, so **the trust boundary stops at PostgreSQL**, not at the JSONL files.

**c) Observable consequence.** The column `raw_payload` is declared `JSONB`, so when PostgreSQL stores it, the JSON string is normalized: **the original key order** (`purchaseOrderId`, `customerOrderId`, ...) **becomes alphabetical order** (`buyerId`, `isGuest`, `orderDate`, ...), and **a space is added after each colon**. **No value is lost** — only key order and whitespace change; key/value content stays the same. The JSON standard **does not define key order**, so both spellings are the same JSON.

**d) Why this is NOT a bug.** Bronze copies exactly what the source holds. For Amazon the source is a **file**, so it copies it **byte for byte**. For Walmart the source is **PostgreSQL**, so it copies exactly what PostgreSQL returns, including when JSONB has reordered the keys. The two look different because **the sources are different**, not because one source is wrong.

## 5. TikTok Shop

Source: the official documentation pages at `partner.tiktokshop.com`, read through a browser (the direct download tool returned an empty page). Level: **High** unless stated otherwise.

**Order statuses (API).** `UNPAID`, `ON_HOLD`, `AWAITING_SHIPMENT`, `PARTIALLY_SHIPPING`, `AWAITING_COLLECTION`, `IN_TRANSIT`, `DELIVERED`, `COMPLETED`, `CANCELLED`. Notable meanings: `ON_HOLD` is the period in which the buyer can still cancel without seller approval; `COMPLETED` is when returns or refunds are no longer possible.

**Get Order List** (`POST /order/202309/orders/search`). Filters by `order_status`, `create_time_ge`/`create_time_lt`, `update_time_ge`/`update_time_lt` (Unix timestamp, seconds), `shipping_type` (`TIKTOK` or `SELLER`, like Fulfillment Method), `buyer_user_id`, `is_buyer_request_cancel`. Paginated with `page_token`, at most 100 per page, sorted by `create_time` or `update_time`.

**Get Order Detail** (`GET /order/202309/orders`). The field names below were read from the **202309** version page (217 fields). The 202507 version page also exists but its response section could not be opened, so the differences between the two versions are unknown. The generator uses 202309. Field names in `data.orders[]`:
- Order level: `id`, `status`, `user_id`, `create_time`, `paid_time`, `update_time`, `rts_time`, `rts_sla_time`, `tts_sla_time`, `delivery_sla_time`, `shipping_due_time`, `cancel_reason`, `cancellation_initiator` (`SELLER` or `BUYER`), `fulfillment_type`, `delivery_type`, `shipping_provider`, `shipping_provider_id`, `tracking_number`, `buyer_message`, `seller_note`, `is_sample_order`, `warehouse_id`, `payment_method_name`, `recipient_address` (has `postal_code`, `region_code`; absent when `UNPAID` or `ON_HOLD`), `packages[]`, `line_items[]`.
- `payment`: `currency`, `sub_total`, `shipping_fee`, `seller_discount`, `platform_discount`, `total_amount`, `original_total_product_price`, `tax`, `product_tax`, `shipping_fee_tax`, ...
- `line_items[]`: `id`, `sku_id`, `product_id`, `seller_sku`, `product_name`, `sku_name`, `original_price`, `sale_price`, `seller_discount`, `platform_discount`, `display_status`, `cancel_user`, `cancel_reason`, `package_id`, `package_status`, `tracking_number`, `shipping_provider_name`, `is_gift`, `item_tax`, `sku_type`, `combined_listing_skus`.
- No actual delivery date field was found at order or order line level (only SLA timestamps). `packages` and webhooks need further checking.

**Webhook.** Envelope: `type` (number), `tts_notification_id`, `shop_id`, `timestamp` (seconds), `data`.
- Type 1 `Order status change`: `data` contains `order_id`, `order_status`, `is_on_hold_order`, `update_time`. `order_status` values in the webhook: `UNPAID`, `ON_HOLD`, `AWAITING_SHIPMENT`, `AWAITING_COLLECTION`, `CANCEL`, `IN_TRANSIT`, `DELIVERED`, `COMPLETED`.
- Type 12 `Return status change`: `data` contains `order_id`, `return_role` (`BUYER`, `SELLER`, `SYSTEM`), `return_type` (`REFUND`, `REPLACEMENT`, `RETURN_AND_REFUND`), `return_status`, `return_id`, `create_time`, `update_time`. Observed `return_status` values: `RETURN_OR_REFUND_REQUEST_PENDING`, `REFUND_OR_RETURN_REQUEST_REJECT`, `AWAITING_BUYER_SHIP`, `BUYER_SHIPPED_ITEM`, `RETURN_OR_REFUND_REQUEST_CANCEL`, `RETURN_OR_REFUND_REQUEST_COMPLETE`, `RETURN_OR_REFUND_REQUEST_SUCCESS`, and the `REPLACEMENT_REQUEST_*` group.
- There are also separate webhooks: `Cancellation status change` (11), `Package update` (4), `Aftersales Request Status Update` (64), `RMA Status Update` (65), `Refund Success` (67). Details not read yet.

**Inconsistency between API and webhook:** the cancelled status is `CANCELLED` in the API but `CANCEL` in the webhook. This detail should be kept in the generator, because it forces the status mapping layer in Silver to handle both.

**Webhook delivery.** Level: **Medium**, from Hookdeck's guide (secondary source). At-least-once, 4 retries (2 minutes, 30 minutes, 3 hours, 12 hours), deduplicated by `tts_notification_id`.

**Search Returns** (`POST /return_refund/202602/returns/search`). The Return source for the Batch. Filters by `return_ids`, `order_ids`, `buyer_user_ids`, `return_types`, `return_status`, `create_time_ge`/`create_time_lt`, `update_time_ge`/`update_time_lt`.
- `data.return_orders[]`: `order_id`, `return_id`, `return_type`, `return_status`, `arbitration_status`, `role`, `return_reason`, `return_reason_text`, `return_method`, `return_tracking_number`, `return_provider_name`, `can_buyer_keep_item`, `is_quick_refund`, `create_time`, `update_time`, `refund_amount`, `partial_refund`, `return_line_items[]`.
- `return_line_items[]`: `return_line_item_id`, `order_line_item_id`, `sku_id`, `sku_name`, `product_name`, `seller_sku`, `refund_amount`. It has `order_line_item_id`, so the label can be attached to the exact Order Line.
- `return_type`: `REFUND`, `RETURN_AND_REFUND`, `REPLACEMENT`, `EXCHANGE`.
- `return_status`: `RETURN_OR_REFUND_REQUEST_PENDING`, `REFUND_OR_RETURN_REQUEST_REJECT`, `AWAITING_BUYER_SHIP`, `BUYER_SHIPPED_ITEM`, `REJECT_RECEIVE_PACKAGE`, `RETURN_OR_REFUND_REQUEST_SUCCESS`, `RETURN_OR_REFUND_REQUEST_CANCEL`, `RETURN_OR_REFUND_REQUEST_COMPLETE`, `AWAITING_BUYER_RESPONSE`, `PARTIAL_REFUND`, the `REPLACEMENT_REQUEST_*` and `EXCHANGE_REQUEST_*` groups.

**API versions.** Get Order Detail has two versions (202309 and 202507) but the differences could not be read, so TikTok Shop is **not** used for the schema evolution scenario. That scenario uses Amazon Orders v0 and Orders 2026-01-01, both of which have official models.

**Buyer.** The order-level `user_id` is a buyer ID scoped to TikTok Shop and cannot be used across Channels.

**Batch renderer in the repo.** `generator/channels/tiktok_batch.py` renders
Get Order Detail 202309 as a `data.orders` response containing one Order per
JSONL line. The same Order shares its ID with the existing TikTok Shop webhook. The snapshot
keeps the API status `CANCELLED`; the webhook keeps the text `CANCEL`. `payment.seller_discount`
aggregates all discounts from the simulation core, `platform_discount=0` because the core does not yet distinguish
which party pays the discount. `line_items.quantity` is an assumption to cross-check when extending to
the real API. The small source export is in `out/channels-small`; DP1 has loaded and checked
1,810 responses into Bronze. Silver is described in `silver-five-channels.md`.

Official page used to cross-check statuses and time filter fields:
[Get Order List 202309](https://partner.tiktokshop.com/docv2/page/get-order-list-202309).

## 6. Shopee

Source: the official documentation pages at `open.shopee.com`, read through a browser. Level: **High** for the readable items below.

**Order statuses** (from `get_order_list`): `UNPAID`, `READY_TO_SHIP`, `PROCESSED`, `SHIPPED`, `COMPLETED`, `IN_CANCEL`, `CANCELLED`, `INVOICE_PENDING`. The two statuses `IN_CANCEL` and `INVOICE_PENDING` are not in the list of the open-source SDK used in the previous research round, so that SDK is insufficient.

**`get_order_detail`** (`response.order_list[]`):
- Order level: `order_sn`, `region`, `currency`, `cod`, `total_amount`, `order_status`, `shipping_carrier`, `payment_method`, `estimated_shipping_fee`, `actual_shipping_fee`, `message_to_seller`, `create_time`, `update_time`, `days_to_ship`, `ship_by_date`, `buyer_user_id`, `buyer_username`, `note`, `pay_time`, `dropshipper`, `split_up`, `buyer_cancel_reason`, `cancel_by` (`buyer`, `seller`, `system` or `Ops`), `cancel_reason`, `recipient_address` (`zipcode`, `state`, `city`, ...), `package_list`, `item_list`.
- `item_list[]`: `item_id`, `item_name`, `item_sku`, `model_id`, `model_name`, `model_sku`, `model_quantity_purchased`, `model_original_price`, `model_discounted_price`, `order_item_id`, `promotion_type`, `promotion_id`, `wholesale`, `add_on_deal`, `flash_sale`.
- Times are Unix timestamps. No actual delivery date was found at order level in the fields read.

**`get_return_list`** (Returns module, official page). Level: **High** for field names:
- `return` (list) has `return_sn`, `reason`, `text_reason`, `refund_amount`, `currency`, `create_time`, `update_time`, `status`, `due_date`, `tracking_number`, `dispute_reason`, `needs_logistics`, `amount_before_discount`, `user` (`username`, `email`), `item[]` (`model_id`, `name`, `amount`, `images`), with a `more` flag for pagination.
- Filters include `status` (e.g. `REQUESTED`), `negotiation_status` (e.g. `TERMINATED`), `seller_proof_status` (e.g. `PENDING`).
- The values of `status` are in the "Data Definition - ReturnStatus" section, which could not be read. Only `REQUESTED` and `CANCELLED` are confirmed as valid values. The list `REQUESTED`, `PROCESSING`, `ACCEPTED`, `COMPLETED`, `CANCELLED` comes from a secondary source (level **Medium**).

**Shopee push (webhook): not needed.** The project's design assumes Shopee arrives via polling (see [Domain.md](../../shared/domain.md)), so the push payload is not needed for the generator. Push code 3 (order status update) has only been seen in developer discussions, level **Low**, not used.

**Batch renderer in the repo.** `generator/channels/shopee.py` emits
`response.order_list` with one Order per JSONL line, with `item_list`, VND prices
as integers and `order_item_id` for joining the ground truth. Because the Order schema has no
actual delivery date, the renderer projects both core statuses `DELIVERED` and `COMPLETED` onto
`COMPLETED`; `update_time` is only when the current status changed and is not implicitly
`delivered_at`.

Shopee's small source export is in `out/channels-small`; DP1 has loaded and checked
354 responses into Bronze.

## 7. Consequences for the generator

1. **All 5 Channels have official sources for orders and returns.** Still missing: Shopee's full `ReturnStatus`, TikTok Shop's `packages` and cancel/refund webhooks, the differences between TikTok Shop's two Get Order Detail versions, the full columns of the FBA reports, Walmart's return status list, Amazon notifications, Shopify fulfillment and webhook IDs. These items are recorded as assumptions when used.
2. **The actual delivery date is not available for Amazon, Shopee, Shopify, TikTok Shop** in the fields read. The Return Label needs the delivery date, so the general rule is: the simulation core always has `delivered_at`, and each renderer exposes it the way the real Channel does:
   - TikTok Shop and Walmart: status `DELIVERED` (with `update_time` or `statusDate`).
   - Shopify: fulfillment event.
   - Amazon: only estimated dates (`estimated-arrival-date` for FBA, `EarliestDeliveryDate`/`LatestDeliveryDate` for MFN). The generator uses this estimated date as the observable timestamp, and the real `delivered_at` exists only in the core's ground truth. This is an **assumption**: the Amazon label uses the core's actual delivery date to compute the 30-day window, while features and the pipeline only see the estimated date.
   - Shopee: only `update_time` when moving to `COMPLETED`, treated as a proxy similar to Amazon, recorded as an assumption.
3. **Return has a dedicated API or report on every Channel.** Amazon: the flat-file Returns report (MFN) and the FBA Returns report. Walmart: the return orders API. TikTok Shop: Search Returns (Batch) and webhook type 12 (Stream). Shopee: `get_return_list`. Shopify: the refund object in Order (no return object yet).
4. **Per-Channel buyer IDs** (Walmart `buyerId`, Shopify `customer.id`, TikTok Shop `user_id`, Shopee `buyer_user_id`) are only meaningful within each Channel. Amazon has no stable buyer ID.
5. **Status name inconsistencies between API and webhook** of the same Channel (TikTok Shop `CANCELLED` and `CANCEL`) are kept in the generator.
6. **The Walmart webhook has `eventId`**, so Walmart duplicates can be modeled via `eventId` if Walmart is later switched from polling to webhooks.
7. **Extending the renderers does not change the old Channels' measurements.** TikTok Shop, Shopify and
   Shopee use their own per-Order export RNG streams. Amazon/Walmart still use the
   old stream from the 100 GB run; the small manifest after adding all three Channels has the same
   `rows`, `unique_keys`, `rows_by_placement` and `bytes` for the two old sources as
   `out/small/run_manifest.json`. The file count differs because the old version wrote files
   before the resume mechanism existed.
