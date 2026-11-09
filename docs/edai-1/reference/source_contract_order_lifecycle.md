# Contract nguồn v2 — Orders và nguồn vận hành Seller giả lập

> **Pipeline v2 (order lifecycle) — thiết kế và thử ở local, không phải pipeline đã nộp.** Pipeline chạy trên AWS là v1 (DP1 → DP4 + ML), xem [Data Platform evidence](../evidence.md). Các chỗ nhắc GCP dưới đây là kế hoạch cũ, chưa chạy; đích cloud hiện hành là [AWS](../../shared/aws_deploy.md).

Contract ID **`seller-source-v2`**. Q60/Q61 được người dùng
chấp thuận bằng “Hiểu, xử lý”: headline sales dùng phần còn lại sau hủy
trước ship, Refund tiền hàng hiển thị riêng; bổ sung nguồn vận hành Seller
giả lập qua Bronze/Silver để quan sát nhãn v2. Đây là đặc tả để implement,
**chưa phải schema/code đã triển khai hoặc proof runtime**. Không yêu cầu
kết nối API thật, dịch vụ trả phí hoặc quyền Cloud mới.

Policy nhãn giữ nguyên [return-label-contract](return_label_contract.md);
topology/queue theo expanded-scope-design.
Các schema dưới đây là quy định của **nguồn giả lập do dự án sở hữu**, không
phải lời khẳng định schema API Channel thật. Căn cứ hiện trạng là source
code renderer/model/RNG, được dẫn tại từng phần; các thay đổi đề nghị không
được mô tả như hành vi code hiện tại.

## 1. Ranh giới native và ma trận capability

Core Orders vẫn phải có đủ năm Channel. Không thêm actual delivery,
approval hoặc outcome vào native Orders nếu renderer không có trường đó.
Giữ payload gốc, schema/version và transport riêng. Với file Orders, một
dòng là một Order chứa mảng lines; Walmart vẫn đọc DB nguồn qua JDBC, không
đổi ranh giới sang file bootstrap. Bronze operational feed có grain riêng,
không áp “một dòng = một Order” lên movement hoặc Refund.

| Renderer hiện tại | Order/line identity đọc được | Quantity/amount đã thể hiện | Delivery/nhãn v2 đọc được từ renderer này |
|---|---|---|---|
| Amazon | `AmazonOrderId`/`OrderItems.OrderItemId` hoặc `orderId`/`orderItems.orderItemId` | Ordered quantity, item amount/discount; shipped quantity không có ở cả hai schema | Chưa có actual delivery theo allocation, approval và product Refund success |
| Walmart | `purchaseOrderId`, `orderLines.orderLine.lineNumber` | `orderLineQuantity`, PRODUCT charge | Có trạng thái Delivered và status time ở trạng thái hiện tại; chưa đủ lịch sử allocation/approval/refund |
| Shopify | `order.id`, `line_items.id` | `quantity`, `current_quantity`, price, total discount | Không có actual delivery; fulfilled không thay delivered |
| TikTok Shop Batch | `data.orders.id`, `line_items.id` | quantity giả lập, original/sale price, seller discount | Delivered status/update time không chứng minh mọi allocation; chưa có approval/refund feed ở renderer này |
| Shopee | `order_sn`, `item_list.order_item_id` | purchased quantity và giá nguyên/giảm | Completed/update time không phải actual delivery theo allocation |

Nguồn primary trong repo: [Amazon](../../../generator/channels/amazon.py),
[Walmart](../../../generator/channels/walmart.py),
[Shopify](../../../generator/channels/shopify.py),
[TikTok Batch](../../../generator/channels/tiktok_batch.py),
[Shopee](../../../generator/channels/shopee.py). Matrix này chỉ kết luận về code
đã đọc, **không kết luận các API thật không có những capability khác**.
Native fields giữ tên/ý nghĩa gốc; `current_quantity` không tự được map
thành lượng ban đầu, và snapshot mất line không tự phát sinh cancellation.

Native export manifest bắt buộc ghi `snapshot_mode` (`full_entity` hoặc
`delta`), `snapshot_scope` (Order IDs hoặc query/partition scope),
`snapshot_as_of`, `page_set_complete`, `schema_id`, `file_set_digest` và
`source_observed_at`. `full_entity` chỉ cho phép đánh dấu native line absent
trong đúng Order đã complete; không cascade xóa shipment/stock/label. Mâu
thuẫn với facts khác tạo conflict cần correction có dẫn chiếu nguồn.

## 2. Ownership và authority nguồn bổ sung

Producer dùng namespace `sim.seller.*`, hiển thị rõ là dữ liệu giả lập.
Mỗi authority gắn scope seller/account/pool, capability profile và thời hạn;
không để hai nguồn cùng là authority một measure trong cùng scope/time.

| Feed ID / producer | Grain và authority | Không được suy từ feed |
|---|---|---|
| `seller_oms.line_commitments` / `sim.seller.oms` | Revision trạng thái một line: ordered basis, cancel trước ship, giá/discount và native bridge | Không tự xác nhận physical delivery hoặc đổi current native projection thành cancellation |
| `seller_oms.fulfillment_allocations` / `sim.seller.oms` | Binding line↔physical units↔shipment allocation↔pool; quantity terminal | Không tự coi đã giao khi chỉ planned/shipped |
| `carrier.delivery_events` / `sim.seller.carrier` | Actual delivery event của allocation trong scope được nguồn xác nhận | Promised/estimated time không thay actual; không nhận quyền trên pool không quan sát |
| `seller_postsales.return_events` / `sim.seller.postsales` | Request/approved/rejected/canceled theo return item và allocation | Approved không là receipt/restock; canceled nghiệp vụ không xóa lịch sử approved |
| `seller_postsales.refund_events` / `sim.seller.postsales` | Transaction/component/status và allocation tiền có chứng cứ | Success không là physical return; shipping/goodwill không tự là tiền hàng |
| `seller_catalog.listing_versions` / `sim.seller.catalog` | Listing state, SKU mapping, attributes và eligibility theo effective/observed time | Không tạo identity line; mapping change không chuyển stock hoặc reservation cũ |
| `seller_wms.stock_operations` / `sim.seller.wms` | Opening balance + reserve/release/ship/receive/disposition ở pool/location | Snapshot không phải movement; reservation không tăng/giảm physical on-hand |
| `seller_wms.inbound_events` / `sim.seller.wms` | Promise/ETA, receipt từng phần và closure của inbound line | Promise chưa nhận không tăng available |
| `seller_wms.stock_snapshots` / `sim.seller.wms` | Quan sát để đối soát tại `effective_at` | Không ghi đè ledger hoặc thêm adjustment để ép khớp |
| `source.coverage_envelopes` / producer của từng feed | Phạm vi đã quan sát, complete/partial/unavailable và exceptions | Không thay linkage coverage; không chứng minh complete bằng truth ẩn |

Seller-fulfilled slice đầu có authority đầy đủ cho các feed trên. Pool
Channel-managed cần profile riêng: có thể có Orders/catalog mà delivery,
stock hoặc post-sales unavailable. Chỉ cấp capability khi simulator thực
sự xuất feed quan sát được, có nguồn/clock/coverage. Không tự tạo quyền
quan sát mọi kho do Channel giữ chỉ vì world engine biết chúng.

Nguồn bổ sung xuất JSONL dưới
`dataset_version=<id>/producer=<producer>/feed=<feed>/export_date=<UTC-date>/`.
Mỗi dòng Bronze giữ nguyên payload + `source_file`, `export_date`,
`ingested_at`, `run_id`; feed/schema/dataset identity được pin bằng source
manifest, không ghi đè hoặc giả thành fields native Orders. DB nguồn giữ
source snapshot identity và representation DB trả về, không hứa byte gốc
trước khi nguồn JSONB chuẩn hóa; xem [ranh giới nguồn](channel_sources.md).

## 3. Envelope, identity và revision bất biến

Tất cả operational records có schema envelope sau. ID là UTF-8 string;
không ép native số dài sang float. Số lượng là integer unit EACH trong lát
đầu; tiền là integer minor units + currency/scale. Timestamp là UTC RFC3339
microsecond; native precision thô hơn được khai báo, không chế độ chính xác.

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

Rules bắt buộc:

- Entity key dùng tuple canonical có version: dataset + seller + Channel +
  account/shop + native Order + đầy đủ native line key. Không ghép chuỗi
  bằng dấu phân cách không escape; phân biệt null/absent. SKU/mapping không
  tham gia business line ID. Buyer không được nối xuyên Channel.
- `source_event_id` là identity một fact/revision nguồn, bất biến khi retry;
  uniqueness theo dataset + producer + epoch + event ID. Cùng ID/hash là
  duplicate; cùng ID khác payload/hash là conflict. Retry không tạo revision.
- `entity_revision` integer tăng trong entity; `source_sequence` thứ tự
  nguồn trong epoch, không tự là event-time hoặc thứ tự xuyên producer.
  Timestamp không làm tie-break để đoán revision. Gap được giữ pending.
- `operation` thuộc `lifecycle_event`, `state_revision`, `correction`,
  `invalidation`. Correction/invalidation phải trỏ event/revision bị sửa,
  có reason và revision mới; record cũ không overwrite. Refund reversal
  nghiệp vụ là event mới, khác invalidation xác nhận success cũ sai.
- `depends_on_event_ids` và operation refs giữ quan hệ nhân quả. Release
  đến trước reserve hoặc delivery trước allocation tới được giữ
  `pending_reference`, không apply bằng timestamp lớn nhất hoặc clamp.
  Nếu hết horizon vẫn thiếu thì conflict/unknown đúng scope.
- Nguồn không có revision native dùng source snapshot ID + canonical
  payload digest + observation identity; đây là identity observation,
  không được gọi là thứ tự nghiệp vụ nguồn đã cam kết.

Current code có deterministic native IDs và RNG theo module/ngày, nhưng
chưa thực thi envelope này; xem [base](../../../generator/channels/base.py),
[RNG](../../../generator/core/rngs.py), [model](../../../generator/core/model.py).

## 4. World identity và bất biến khi thay renderer

Manifest khóa seed, config, catalog/buyer population, semantics/schema
versions, module versions, generator/runtime digests, source profiles và
horizons **trước ghi**. Dataset version bao gồm cấu hình render/transport;
`world_id` riêng pin nguyên physical world. Hai dataset chỉ được gọi là
same-world nếu manifest physical entities/outcomes/quantity/time có digest
giống nhau; seed bằng nhau không đủ.

Substream v2 theo stable entity ID + module semantic version + purpose,
ví dụ `(world_id, line_id, returns-v2, intent)` và
`(world_id, return_item_id, approvals-v1, delay)`. Không dùng danh sách
allocation record làm số lần Bernoulli. Sinh intent/affected stable units
một lần ở grain line; sau đó map units vào physical delivery allocations.
Split/gộp renderer records không được rút lại intent, affected units,
approval time hoặc money. Thay thời gian giao thật có thể đổi qualification
và phải có world/scenario khác. Không dùng future allocation count làm
feature lúc đặt. Cơ chế p theo line có quantity effect phải version và kiểm
prevalence, không âm thầm áp lại p cho mỗi unit/allocation.

[Return engine hiện tại](../../../generator/core/returns.py) chỉ rút outcome cấp
line và dùng request time; [batch exports](../../../generator/delivery/batch_exports.py)
chọn snapshot theo status. V2 không tự tương thích byte hoặc target v1.
So sánh nhãn v1/v2 giữ same-world scope đủ linkage và báo thay eligibility;
không so metric trên hai world khác nhau rồi kết luận model tốt hơn.

## 5. Schema canonical cụ thể của lượng và tiền

OMS line commitment state, trong envelope `state_revision`:

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

Ví dụ Q60 dùng đơn vị tiền chính: gross330 − discount30 = ordered300;
hủy1 net100 → headline current non-canceled200. Product refund100 giữ
measure riêng, không tự hạ headline thành100. Tax/shipping/fee/goodwill giữ
components riêng; không gọi headline này là cash, net profit hay settlement.
Nếu quantity cancellation chưa biết thì headline unknown cho phần affected,
không suy cancel từ status chung hoặc native quantity giảm do Refund.

`ordered_original_qty`/amount là cam kết ban đầu; `ordered_basis_qty`/amount
là cơ sở sau source corrections có audit, không trừ Refund nghiệp vụ.
Analytics giữ original và corrected basis có role rõ; headline restated dùng
basis revision đã pin. Một correction3→2 khi delivered3 gây conflict trừ khi
có dependent correction hợp lệ, không sửa delivered cho khớp.

Conservation tại cutoff, theo đúng basis revision:

```text
ordered_basis_qty = canceled_unshipped_qty + not_yet_shipped_qty
                  + shipped_not_delivered_qty + delivered_qty
current_noncanceled_qty = ordered_basis_qty - canceled_unshipped_qty
headline_product_sales_minor = ordered_product_net_minor
                             - canceled_product_net_minor
```

Return received và refund quantity/amount là measures sau giao; không cộng
chúng như thành phần mới trong phương trình ordered. Chỉ tính delivered
cumulative từ unit/allocation identities không trùng; mỗi unit xuất một lần
trong topology không có replacement của lát đầu. Allocation/refund/return
facts aggregate độc lập về line trước join để chống fan-out.

`fulfillment_terminal=true` chỉ được tin khi các allocation/cancellation
references đã resolve và phương trình quantity chứng minh không còn lượng
treo. Cờ từ nguồn một mình không vượt reconciliation. Cancellation event
giữ affected units/qty, amount basis và effective/observed time; lượng hủy
không được đồng thời thuộc một shipment còn hiệu lực.

Money rules: integer minor-unit theo currency table được version; không
float trong source v2. Discount Order-level phân bổ bằng largest remainder
với tie-break stable line ID, ghi `allocation_rule_version`; allocated +
unallocated phải bằng tổng nguồn. Product refund allocation là chứng cứ
nguồn, không dùng cùng công thức chia đều để tạo linkage nhãn. Không cộng
hai currencies hoặc dùng `price_factor` làm FX. Reconciliation giữ riêng
amount unmatched và sai số; không sửa amount để vượt check.

## 6. Fulfillment, post-sales và fields dựng nhãn

Payload tối thiểu của từng feed (ngoài envelope/seller/account/Channel):

| Feed | Fields bắt buộc khi claim capability | Optional, không tự suy |
|---|---|---|
| Fulfillment allocation | `allocation_id`, native Order/line refs, `physical_unit_ids`, `quantity`, `shipment_id`, `bound_pool_id`, `bound_location_id`, `binding_version`, `allocation_state` (`planned/shipped/delivered/canceled`) | Carrier ref; delivered không lấy từ state planned |
| Delivery event | `delivery_event_id`, `allocation_id`, `delivered_unit_ids`, `delivered_qty`, `actual_delivered_at`, `event_type=delivered`, authority scope | Signature/attempt data ngoài scope |
| Return event | `return_id`, `return_item_id`, `event_type` (`requested/approved/rejected/canceled/received/dispositioned`), native refs, affected units/qty, allocation refs hoặc explicit unresolved scope | Reason code; physical receipt không thay approved time |
| Refund event | `refund_transaction_id`, `transaction_revision`, `status` (`pending/failed/success/reversed`), `component` (`product/shipping/tax/fee/goodwill`), positive `amount_minor`, currency/scale, native refs, allocation money rows hoặc unresolved scope | Return ID; refund-only được hỗ trợ |
| Invalidation/correction | `corrects_source_event_id`, reason, replacement fields/refs, new revision | Không nhận correction không trỏ fact sai |

Mỗi lần transition có event ID riêng; `effective_at` là lúc transition
thực sự xảy ra. Ví dụ requestedD29/approvedD31 giữ cả hai, không sửa approved
thành D29. Refund mixed components xuất component rows dưới cùng transaction;
status/success time không nhân amount; subtotal components + unmatched bằng
transaction tổng. Allocation rows gồm `allocation_id`, `amount_minor`,
`evidence_ref`, không dùng amount tình cờ khớp để suy allocation.

Với delivery event, `effective_at=actual_delivered_at`; với approval/success,
`effective_at` là timestamp qualification tương ứng. Nếu event nguồn chỉ có
ngày hoặc status-time không chứng minh actual transition, capability ghi
proxy/precision-limited và không tự nâng thành nguồn đủ nhãn ở boundary.

Label transform dùng actual delivery theo allocation, approval event qty>0
hoặc product refund success amount>0, allocation bridge, terminal quantity,
coverage envelopes và revision known-time. Không đọc `line.label`,
`return_probability`, `defect_propensity`, truth-only delivery hoặc future
unit assignments của engine. Ground truth oracle có namespace/quyền riêng,
chỉ dùng kiểm chứng simulator/transform, không làm source feature hoặc bằng
chứng feed complete. Version/policy regime của approval là metadata nguồn
để diagnostics, không tự là feature order-time trước khi đã quan sát.

## 7. Hai đồng hồ, coverage và uncertainty

`effective_at` và `source_observed_at` thuộc
`simulation_business_utc:v2`. Effective time có thể lùi so với observation
khi late/correction. Query as-known C dùng **observed < C**; effective state
ở T dùng interval `[valid_from, valid_to)`. `recorded_at`, Bronze
`ingested_at` và actual model artifact creation dùng `execution_utc`; pacing
có mapping manifest riêng, không dùng execution timestamp thay simulated
knowledge time. Cùng producer không được tự backdate observed khi replay.

Coverage envelope cũng là record nguồn có revision/known-time:

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

Digest trong ví dụ là placeholder minh họa, phải tính từ emitted set khi
implement. `complete` nghĩa transport/source đã xuất đủ phạm vi theo source
contract, không nghĩa mọi record map được. Một feed complete vẫn có
unresolved refund. Envelope chỉ có hiệu lực sau observed; các corrections
về sau tạo envelope revision mới và recompute affected generations, không
ghi đè lời cam kết as-known cũ. Sequence range mà có gap, missing partition,
unparseable record hoặc producer chưa close interval không được complete.
Envelope event interval phải bao phủ qua deadline inclusive: `end_exclusive`
phải **lớn hơn** deadline, không chỉ bằng deadline. Không cấp complete bằng
việc hôm nay đã qua 30 ngày hoặc thấy feed trống.

Nguồn **scale** phát coverage mỗi tuần nối tiếp, không chồng và không cộng dồn,
trên mỗi operational feed; tuần cuối kết thúc đúng `lifecycle_followup_end`.
Observed = max(cuối tuần +1 ngày, max observed dòng trong tuần +1µs), nằm trước
`source_observation_end`. Interval rỗng sự kiện vẫn cần claim complete của producer
với count0/digest `[]`, đủ partition; không tự suy complete từ việc không nhận dòng.

Receipt chọn dòng cùng dataset/feed/producer/epoch, effective trong interval và
observed trước envelope; kiểm digest canonical (sắp source_event_id), count,
sequence ranges và partition trên **đúng tập này**. Ranges có thể tách nhiều đoạn
vì sequence cấp producer, không cấp tuần/feed. Silver join bằng bucket ngày UTC
trước predicate interval để mỗi dòng scale chỉ vào một envelope, tránh N×số tuần.
Các envelope nối liền có thể hợp lại để phủ cửa sổ nhãn; end cuối phải quá deadline
inclusive. Khoảng hở hoặc tuần partial không đủ coverage. Fixture giữ envelope và
bytes cũ; xem ADR-0013.

Uncertainty record tối thiểu:

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

Biết Order nhưng thiếu line thì giữ candidate lines, biết line nhưng thiếu
allocation thì giữ candidate allocations; thiếu Order thì mở rộng đến scope
account/feed/time do envelope chứng minh. Chỉ thu hẹp bằng constraints
nguồn đáng tin hoặc revision giải quyết, không chọn theo SKU/amount gần
giống. Nếu không giới hạn scope thì đánh unavailable/unknown toàn scope
producer relevant, không bỏ event để gọi những line khác là negative.

Nhãn official cần feed completeness **và** linkage/uncertainty sạch cho
scope line liên quan. Báo ba mẫu số riêng: feeds complete, events linkable,
lines eligible/mature/fully observable. Line có positive rõ nhưng còn
missing required capability không thành mẫu training chuẩn; đây giữ policy
bảo thủ đã chốt. Những event chắc chắn outside window/nonproduct có chứng
cứ có thể được loại khỏi affected label scope, vẫn giữ audit.

## 8. Ba horizon và tail có cấu hình

Manifest có interval exclusive và clock domain cho cả ba mốc:

1. `order_creation_start/end`: chỉ tạo Order trong khoảng này. History và
   stream là slices của khoảng tạo Order, không phải ngày dừng lifecycle.
2. `lifecycle_followup_end`: tiếp tục shipment/delivery/cancel/Return/Refund
   của Order đã có. Không tạo Order mới chỉ để giữ stream hoạt động.
3. `source_observation_end`: tiếp tục nhận late observations/corrections và
   phát envelopes sau business events; phải lớn hơn follow-up nếu có delay.

Fixture profile đầu: Order creation `[D0,D395)`, lifecycle follow-up
`[D0,D485)`, source observation `[D0,D500)`, UTC. Các mốc D là relative ngày,
không là cam kết production SLA. Profile có max fulfillment lag45d,
event qualification cửa sổ30d, follow-up allowance15d và source delay15d;
gồm events beyond30 để kiểm negative đúng policy. Generator phải validate
bounds hoặc ghi overflow/censored, không truncate rồi tự tuyên bố complete.

Stress profile cho lag không có trần phải giữ finite observation cutoff và
right-censored counts; không thể chứng minh mọi lifecycle chín bằng tail
hữu hạn. Mọi profile công bố phân bố và trần được dùng, maturity/terminal
counts theo order-date cohort, reasons unknown/censored và bytes/runtime
của tail. Ví dụ Order D394 giao D402, approvedD431, observedD434: ở D395
immature; sau D432 vẫn phải chờ coverage quan sát phù hợp, không đọc truth
để gán sớm. Stream pacing/execution runtime tính riêng khỏi simulated dates.

## 9. Listing, pool bindings và stock operations

Ngay cả fixture “static” vẫn xuất opening listing/pool mapping trước Order
đầu, có effective/observed time và profile. Không suy “active” chỉ vì có một
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

Change D5 báo D7 khác correction xác nhận mapping cũ sai: cả hai tạo
revision observedD7, nhưng `change_kind`/effective range/reference khác.
Sale line giữ `listing_binding_id` và attribution mapping **as-known lúc
bán**. Analytics restated có attribution revision riêng; prediction không
đổi feature SKU A thành B vì correction biết sau đó. Tử/mẫu return rate
dùng cùng sale-time dimension role, eligibility/cutoff; current-attribute
view phải đặt tên riêng. Mapping correction không làm line ID/sample mới.

Allocation/reservation binding pin pool/location/SKU/version lúc operation.
Thay eligibility mapping không migrate reservation hoặc stock đang mở.
Nếu correction cần reattribute phải có explicit old/new binding revisions,
dependent reconciliation và audit, không dùng join current. Một pool Seller
shared dùng cho nhiều Storefront; Channel-managed pools riêng theo owner,
không cộng availability lặp và không gọi Storefront eligibility là quota/ATP.

Stock operation payload dùng `operation_id`, `operation_revision`,
`operation_type`, `pool_id`, `location_id`, `internal_sku_id`,
`reservation_id`/`inbound_line_id`/`allocation_id` liên quan,
`depends_on_operation_ids`, `binding_version`, `quantity` và state deltas:

| Operation | Δ on-hand | Δ reserved | Δ non-sellable | Điều kiện |
|---|---|---|---|---|
| opening balance10 | +10 | 0 | 0 | Một opening source revision cho scope |
| reserve3 | 0 | +3 | 0 | available≥3 trong ordered operation sequence |
| ship1 của reservation | −1 | −1 | 0 | Reservation có ít nhất1; allocation binding khớp |
| release2 | 0 | −2 | 0 | Không phải receipt2 |
| return receive1 pending inspection | +1 | 0 | +1 | Có receipt ID/unit liên kết |
| disposition restockable1 | 0 | 0 | −1 | Chuyển trạng thái, không nhận lần nữa |
| inbound receipt2 | +2 | 0 | 0 | Receipt2 có identity, không dùng promised5 |

`available = on_hand - reserved - non_sellable` trong topology đầu; các nhóm
không giao nhau. Reserve/ship là một operation logic được WMS authority
phát ra; OMS reference cùng operation, không phát movement thứ hai.
Correction thay operation delta cũ bằng revision mới khi dựng projection,
không cộng toàn bộ revisions. Reversal nghiệp vụ dùng operation mới có
causal ref. Source order/causal refs quyết định apply; mất dependency giữ
pending. Snapshot khác ledger tạo reconciliation record, không clamp âm
hoặc tạo adjustment tự động. Oversell/backorder/replacement/transfer network
vẫn ngoài slice đầu; input ngoài capability được đánh unsupported/conflict.

## 10. Profiles required/optional và publication

Profile được version/pin **trước build**, gắn danh sách scope và outputs.
Không giảm required sau khi thấy validation fail. Profiles có khả năng
coexist theo account/pool, không biến missing capability thành fail mãi cho
toàn bộ hệ thống, cũng không silently bỏ output khỏi manifest.

| Profile | Required sources / outputs | Optional / unavailable được công bố |
|---|---|---|
| `core-orders-five-channel/1` | Năm native Orders, native identity/schema/snapshot manifests; Bronze/Silver, quantity/amount/coverage Orders, sales headline chỉ ở scope đủ Q60 basis | Lifecycle labels/inventory không claim từ profile này; thiếu basis thì headline unavailable riêng, original native measures vẫn có provenance |
| `seller-operated-label-v2/1` | Core scope được chọn + OMS commitment/allocation, carrier actual delivery, post-sales Return và product Refund, envelopes/bridge/uncertainty; labels official, eligibility/unknown diagnostics | Physical receipt/reason optional cho target; thiếu một trong hai positive paths không chứng minh official0/complete target |
| `seller-stock-triage/1` | OMS binding, catalog/pool history, opening/movement/reservation/inbound receipts, coverage; pool balances/reconciliation/availability queue | Snapshot đối soát optional khi profile không claim snapshot parity; ETA optional; unsupported Channel pools không nhận stock claim |
| `channel-orders-limited/1` | Orders/catalog scope và capability declared; coverage/unsupported diagnostics | Nhãn/stock unavailable hoặc provisional đúng capability; không tính vào benchmark toàn năm Channel |

Feed optional trong một profile có thể required ở profile khác. Một nguồn
snapshot optional thiếu thì không tự là0; một required feed trễ/failed giữ
last-good generation cho profile đó, tạo candidate failed có reason. Missing
capability thiết kế khác thiếu partition trong capability đã claim. Manifest
output rows có `status=available/unavailable/provisional`, `profile_version`,
`capability_refs`, `coverage_refs`, dataset/input snapshots/contract digests;
không drop field/output để làm validator bỏ kiểm. UI/API/agent echo scope
và limits. Partial profile publication chỉ được dùng nếu activation contract
đã định nghĩa bundle phụ thuộc rõ, không tự trộn generation giữa profile.

## 11. Fixture và điều kiện nghiệm thu

Đây là checklist implement/test, **tất cả chưa chạy cho source v2**. Expected
phải tính độc lập từ emitted frozen source set, không dùng transform chính
hoặc oracle truth như source production.

| Fixture | Assertion bắt buộc | IR |
|---|---|---|
| Order ngày cuối, giao/approve/observe sau creation end; overflow lag | Horizon/tail/envelope đúng; mature/censored phân hoạch; không fill0 | IR04 |
| Renderer thêm field/module và split delivery records cùng units/time | Same-world outcome digest/quantity/label không đổi; seed-only comparison bị chặn | IR05/08 |
| Refund chỉ có Order A+B rồi revision map về A | A/B unknown trước resolution, A1/B0 khi mọi điều kiện khác đủ sau đó | IR07/09 |
| Ship3 refund1, native quantity2; full snapshot mất line | ordered/delivered3 còn; không tự cancel/release/cascade delete | IR11 |
| Ordered300 canceled100 refund100 | Headline200 và original/canceled/refund riêng; tax/shipping không trộn | IR12/Q60 |
| Listing/pool mapping thay/correction lúc reservation mở | Line ID/binding ổn định; no stock teleport; PIT A khác restated B | IR13/14 |
| Hai Storefront cùng pool10 | Seller total10, eligibility không thành20 hoặc ATP hứa độc lập | IR15 |
| Release-before-reserve, correction đến đảo thứ tự, duplicate/hash conflict | pending/convergent projection hoặc explicit conflict; không clamp/reapply | IR16 |
| Feed thiếu capability vs feed required thiếu partition | profile limits/unavailable khác candidate fail; không thay requirement | IR20 |
| Giữ intents, chỉ đổi approveD29→D31 | Nhãn thay đúng v2; diagnostic timing giải thích, không tuyên bố defect giảm | IR24 |

Các mục IR04/05/07/08/09/11/13/14/15/16/20/24 đã có **cách thực thi ở mức
contract trong tài liệu này**, còn trạng thái runtime mở. Việc xác nhận
Q60/Q61 đóng lựa chọn nghiệp vụ/nguồn; không đóng các IR bằng implementation.
Dependencies cụ thể, source profile manifests, physical tables và số đo
bytes/runtime phải được pin/chứng minh trong G0–G2 trước full-scale.

## Căn cứ và giới hạn

Thiết kế quantity/identity dựa các phản ví dụ có nguồn code trong
audit cross-domain; clock, uncertainty và
same-world dựa audit temporal.
Mapping vấn đề/ưu tiên theo IR register.
Field policy giữ nguyên [label v2](return_label_contract.md), không đổi
target thành physical return hoặc return request trước duyệt. Các API thật
trong [channel-sources](channel_sources.md) là nghiên cứu riêng có mức tin
cậy/giới hạn; contract này không xác nhận lại chúng hoặc dùng giả định chưa
kiểm chứng để nâng capability local. Không có dữ liệu/API/Cloud mới được
thực thi trong công việc viết contract.
