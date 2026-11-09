# Contract run, publication và serving — thiết kế triển khai bộ nhỏ

Đây là thiết kế kỹ thuật trong Q1–Q61 đã được người dùng
chấp thuận; Q60 dùng sales current non-canceled, Q61 bổ sung feed Seller có
provenance. Không đổi target `return-or-refund-line-v2`, quyền sản phẩm hoặc
phạm vi Cloud. Chưa triển khai protocol này và chưa có POC đạt. Những ví dụ
ID/hash dưới đây là giá trị minh họa, không phải artifact runtime.

Tài liệu cụ thể hóa ADR-0007,
readiness, [label contract](return_label_contract.md)
và IR02/06/10/17/18/19/20/22/23/25 trong
sổ rủi ro. Sửa resume/CDC/validator riêng không
đủ đóng các IR này. Không lấy kiểm link hoặc JSON parse làm proof pipeline.

## 1. Bất biến và lựa chọn backend local

Mỗi build đọc một input vector đã đóng băng; mỗi lần ghi lại dùng namespace
attempt mới. Candidate chỉ được đọc công khai sau khi output bắt buộc đã
seal, đối soát và con trỏ activation được đổi trong một transaction control.
Một activation liên kết **data generation + serving bundle** tương thích;
chúng có vòng đời riêng. Training dataset của model bất biến, không cần cùng
ID với data generation mới phục vụ model đó. Một turn dùng đúng một context.

Chọn PostgreSQL 15 đang có trong [Compose](../../../docker-compose.yml) làm control
store, schema `platform_control` trong DB riêng của ứng dụng; không dùng DB catalog
Lakekeeper hoặc DB Airflow làm bảng nghiệp vụ. Bộ nhỏ có thể dùng cùng
PostgreSQL server nhưng roles và DB phải riêng. Chưa tạo DB/schema tại đây.
Dùng object store hiện có để giữ immutable JSON/report/artifact. Không thêm
service điều phối mới. Đây là lựa chọn triển khai local để thử; không khẳng
định độ sẵn sàng, RPO/RTO hoặc khả năng scale Cloud đã được chứng minh.

| Store | Định danh phải pin | Quy tắc ghi/đọc |
|---|---|---|
| Iceberg | Catalog/table UUID + metadata location + snapshot ID + schema digest | Đọc `snapshot-id`; không đọc current rồi chỉ gắn ID vào response |
| File/object | Bucket/key + object version nếu có + SHA-256 + bytes | File set exact trong manifest, không glob lại lúc retry |
| PostgreSQL outputs | Schema/table hoặc partition generation-specific + schema/key/content digest | Loader ghi candidate riêng, seal xong runtime chỉ có SELECT; không JDBC overwrite bảng đang serve |
| Feast/Redis | Generation-specific project/namespace + registry artifact digest + materialization receipt | Mỗi generation có online namespace riêng; không ghi đè project đang pin |
| Model/RAG | Image digest, weights/preprocessor/embedding/corpus/index digests | Endpoint startup chứng minh artifact thực nạp; alias/tag không làm identity |

Namespace output mẫu: `out/<epoch>/<run>/<attempt>/...`,
`pub_<generation>_<attempt>.sales_daily`, Feast project `platform_<generation>_<attempt>`; manifest trỏ attempt đã chọn,
không để hai retry ghi chung project.
POC phải kiểm bản Feast đã pin thực sự tách Redis keys theo project và registry;
không dựa vào tên project trên config để suy có isolation. Nếu adapter không
hỗ trợ thì chọn namespace vật lý khác và ghi vào manifest trước nghiệm thu;
không tự thêm cột generation vào entity làm thay nghĩa feature. Adapter offline
phải đọc exact Iceberg snapshot. Nếu không làm được, xuất immutable Parquet từ
snapshot đó rồi dùng file manifest exact và đối soát parity; Iceberg vẫn là
lineage gốc, thay đường đọc phải báo rõ trong proof Feast.

## 2. Manifest bất biến và frozen input vector

Envelope JSON bắt buộc có `schema_version`, identity, timestamps UTC, clock,
contract/config/code digests và references. Dùng JSON canonical UTF-8 theo quy
ước project đã pin rồi SHA-256; không hash pretty-print tùy ý. Tất cả digest
thực phải có thuật toán và 64 hex digits; `h_*` bên dưới là placeholders.
Không đặt credential, URL có token hoặc payload nhạy cảm vào manifest.

```json
{
  "schema_version": "platform.run-input.v1",
  "run_id": "r_018",
  "dataset_version": "d_world_07",
  "world_digest": "h_world",
  "clock_id": "sim_world_07",
  "knowledge_cutoff_exclusive": "2026-07-01T00:00:00Z",
  "business_window": {"start": "2026-01-02T00:00:00Z", "end": "2026-07-01T00:00:00Z"},
  "view_mode": "as_known",
  "source_profile_digest": "h_sources",
  "output_profile_digest": "h_profile",
  "code_tree_digest": "h_code",
  "dependency_lock_digest": "h_lock",
  "config_digest": "h_config",
  "metric_contract_digest": "h_current_noncanceled",
  "label_contract": "return-or-refund-line-v2",
  "label_contract_digest": "h_label_v2",
  "inputs": [
    {
      "role": "amazon_orders_bronze",
      "kind": "iceberg",
      "catalog": "platform",
      "table": "bronze.amazon_orders",
      "table_uuid": "uuid_a",
      "snapshot_id": "81234001",
      "metadata_location": "s3://lake/metadata/00017.json",
      "schema_digest": "h_schema_a",
      "dataset_manifest_digest": "h_source_manifest",
      "coverage_receipt_digest": "h_orders_coverage"
    },
    {
      "role": "seller_delivery",
      "kind": "file_set",
      "manifest_uri": "s3://raw/d_world_07/delivery/manifest.json",
      "manifest_digest": "h_delivery_files",
      "producer": "simulated_seller_wms",
      "authority": "actual_delivery",
      "coverage_receipt_digest": "h_delivery_coverage"
    }
  ],
  "resolved_at_wall": "2026-10-18T08:00:00Z"
}
```

Đây chỉ là hai entries để minh họa shape. Resolver phải mở rộng **toàn bộ**
feed/Channel/dependency theo source profile: Orders năm Channel, mappings,
Listing/inventory/lifecycle/refund/coverage được dùng. Một entry không đủ cho
cả Q61. Coverage receipt gồm scope Channel/account/pool, khoảng event được
bao phủ, observation cutoff, producer/authority, missing/conflicts/linkage,
completeness definition và bằng chứng upstream. `unsupported` khác `empty`.

Resolver được chạy một lần khi tạo run; manifest ghi xong mới cấp build token.
Nó có thể resolve snapshots không cùng commit timestamp, nhưng phải kiểm cùng
world/dataset, cutoff và source coverage. Bronze chứa observation sau cutoff
thì transform vẫn lọc `source_observed_at < cutoff`. Exact snapshot không tự
chứng minh known-time đúng. Mutable JDBC source phải được export dưới một
consistent snapshot vào immutable staging trước khi đóng manifest; parallel
JDBC connections không mặc nhiên dùng chung DB snapshot. CDC entry cần source
incarnation, topic ID/partition set và start-inclusive/end-exclusive offsets
cụ thể từ protocol CDC; không thay bằng event-time hoặc từ khóa `latest`.

Task nhận `--input-manifest <uri> --input-digest <sha256> --attempt-id ...`.
DAG không lấy lại Airflow Variables để resolve nguồn/config giữa tasks.
Validator nhận manifest đó cộng exact output receipts. Khi upstream tạo
output mới cho downstream cùng run, bổ sung stage manifest mới có parent digest
và exact snapshot của stage trước; không sửa input manifest gốc. Retry dùng
cùng manifest, attempt khác và namespace khác. Snapshot/file mất hoặc digest
không khớp: fail `INPUT_UNAVAILABLE`/`INPUT_CONFLICT`, không fallback latest.

DAG [DP2](../../../dags/dp2_silver.py), [DP3](../../../dags/dp3_features.py) và
[label bridge](../../../dags/dp3_return_labels.py) hiện truyền table names/run ID;
chưa có interface này. `max_active_runs=1` của từng DAG không tạo snapshot
đa bảng hoặc khóa giữa các DAG. Iceberg hỗ trợ đọc snapshot riêng từng bảng,
không tự tạo snapshot liên bảng.
[Nguồn Iceberg 1.7.2](https://iceberg.apache.org/docs/1.7.2/spark-queries/).

## 3. Output profile được khóa trước build

Profile có scope và capabilities tường minh, được hash vào input manifest.
Không cho candidate tự bỏ output sau khi thấy lỗi. Mỗi scope có required
outputs riêng; optional không làm mất số liệu chính hoặc biến thiếu thành0.

```json
{
  "schema_version": "platform.output-profile.v1",
  "profile_id": "seller-demo-v1",
  "scope": {"channels": ["amazon", "walmart", "tiktok_shop", "shopify", "shopee"]},
  "required": ["sales_daily_pg", "listing_inventory_pg", "coverage_pg"],
  "capability_requirements": [
    {"capability": "return_label_v2", "scope_manifest_digest": "h_label_scope", "outputs": ["label_history_offline", "features_offline", "features_online"]},
    {"capability": "sku_risk_queue", "scope_manifest_digest": "h_risk_scope", "outputs": ["risk_queue_pg", "risk_model_compatibility"]}
  ],
  "optional": ["customer_repeat_supported_scope"],
  "validation_suite_digest": "h_expected_suite",
  "coverage_policy_digest": "h_coverage_policy"
}
```

Scope manifests liệt kê từng namespace đủ/thiếu capability và lý do; chúng
được chốt từ G0, không tuyên bố label/model phủ năm Channel nếu thực tế chỉ đủ
một phần. Seller feed Q61 có authority/bridge riêng, không làm mọi pool có
coverage như nhau. Required capability trong scope đã khai báo mà bị mất
feed/receipt thì candidate fail, giữ activation tốt trước. Scope ngoài
capability hiển thị `unsupported`/`unknown`; không cắt dòng lặng lẽ. Optional
thiếu phải có receipt `unavailable` và consumer không được suy0. Đổi requirement
hay scope tạo profile revision mới, audit và kiểm chứng theo contract tương ứng.

Mỗi output receipt phải có role/status, input/stage digest, physical identity,
key/schema/content digest, logical row count, quantity/amount theo currency,
coverage, freshness, validation report digest và seal evidence. Zero rows hợp
lệ phải được chứng minh bằng input/coverage; zero rows không thay missing.
Content digest dùng key order + canonical value encoding đã pin (bao gồm null,
decimal/time precision), shard digests + root; count đơn độc không đủ oracle.
Validator đối chiếu logical rows/key/value ở snapshot exact với fixture/source
oracle độc lập, không dùng physical file counts làm số dòng nghiệp vụ.

## 4. Label revisions, training và đúng đồng hồ

Lưu history append-only theo line identity đầy đủ + target version; không ghi
đè0 bằng1 rồi mất khả năng truy vấn kiến thức cũ. Row revision tối thiểu:

```json
{
  "line_key": ["d_world_07", "amazon", "shop_1", "order_10", "line_2"],
  "label_contract": "return-or-refund-line-v2",
  "revision_id": "lr_003",
  "revision_sequence": 3,
  "known_at": "2026-06-15T09:00:00Z",
  "state": "immature",
  "label": null,
  "maturity_end": "2026-07-01T12:00:00Z",
  "cohort_date": "2026-06-01",
  "reason": "delivery_correction",
  "source_refs_digest": "h_corrected_delivery_refs",
  "coverage_receipt_digest": "h_coverage_c3",
  "replaces_revision": "lr_002",
  "computed_at_wall": "2026-10-18T09:00:00Z"
}
```

`known_at` là thời điểm đủ bằng chứng nguồn/coverage/maturity theo clock nghiệp
vụ đã khai báo, không tự lấy thời gian delivery hoặc thời gian chạy replay.
Nếu maturity mới chỉ qua trong wall-clock mà không có completeness proof thì
không tạo label0 chính thức. Với cutoff exclusive C, chọn revision mới nhất
có `known_at < C`; tie cùng known_at phải dùng source revision/order đã có
contract hoặc báo conflict, không dùng thứ tự Spark tùy ý. Khoảng hiệu lực
suy ra của revision ở t trước revision ở t2 là **t < C <= t2**, giữ đúng biên
exclusive của label contract. Có thể lưu `next_known_at` dẫn xuất, không sửa
nội dung revision cũ. Bổ sung state change vì maturity/coverage dù không có
event dương mới. Historical queries đọc source snapshot đủ chứa revisions cần
thiết, và filter đúng cutoff; không query bảng current rồi hồi tưởng label cũ.

Fixture bắt buộc: C1 biết0, C2 late event thành1, C3 delivery correction thành
immature; training dataset chốt C1 giữ checksum cũ. Refund unmatched giữ affected
scope trước eligibility, không làm mất uncertainty bằng inner join. Maturity,
positive/negative và boundaries vẫn theo L01–L17, không định nghĩa target mới.

Training manifest phải riêng với publication manifest:

```json
{
  "schema_version": "platform.training-dataset.v1",
  "training_dataset_id": "td_fold_03",
  "world_digest": "h_world",
  "clock_id": "sim_world_07",
  "feature_definition_digest": "h_feature_v3",
  "feature_schema_digest": "h_ordered_units_defaults",
  "label_contract": "return-or-refund-line-v2",
  "label_contract_digest": "h_label_v2",
  "training_information_cutoff_exclusive": "2026-04-01T00:00:00Z",
  "sample_manifest_digest": "h_sample_keys_feature_cutoffs_label_revisions",
  "input_manifest_digests": ["h_training_inputs"],
  "eligibility_and_split_digest": "h_fold_03",
  "created_at_wall": "2026-10-18T09:30:00Z"
}
```

Mỗi sample lưu prediction/order-time, feature cutoff riêng, label revision
chọn trước training information cutoff, eligibility/coverage và fold ID. Future
outcome sau feature cutoff có thể dùng làm label **nếu đã biết trước training
cutoff**; không buộc label đã biết lúc đặt Order. Toàn bộ preprocessing,
feature selection, calibration, threshold và tuning chỉ được fit từ dữ liệu
được phép ở cutoff; test tương lai chỉ mở sau khi khóa các lựa chọn đó.

Forward-at-order evaluation cần cùng `clock_id` và chuỗi thời gian:

```text
mọi training/tuning information observed < training_information_cutoff
training_information_cutoff <= model_ready_at <= sample_prediction_at
feature information observed < sample_feature_cutoff (không sau prediction)
outcome_evaluation_cutoff > maturity + đủ coverage của sample
```

Model học đến D60 không hợp lệ để dự đoán Order D10 dù giao D65; case này phải
bị loại khỏi forward-at-order proof. Fold dùng thời gian prediction, không
chỉ ngày delivery. Với sample có timestamp trùng cutoff thì giữ strict source
boundary trong contract, không tự inclusive. Temporal holdout phải báo số mẫu
bị loại, immature/unknown, scope coverage, baseline và target version.

Tách hai chế độ evidence: `actual_deployment` dùng availability/ready thật
trong wall-clock; `simulated_replay` dùng clock mô phỏng và declared simulated
ready time + training latency policy đã khóa trước fold. Replay vẫn ghi actual
artifact creation/training timestamps; không backdate chúng thành bằng chứng
đã deploy trong quá khứ. Nếu chưa mô hình hóa model-ready theo clock mô phỏng,
chỉ gọi đó retrospective evaluation, chưa đủ forward-at-order proof. Không
so timestamp wall 2026-10 với ngày mô phỏng 2026-04 như cùng đồng hồ.

## 5. Data generation, serving bundle và activation

Generation bất biến ghi frozen input digest, output profile digest, output
receipts, cutoff/view/capability, validation report và wall timestamps.
`generation_id` là UUID hoặc ID opaque mới; không dùng thứ tự chuỗi ID đo độ
mới. Bundle độc lập pin model/RAG/tool artifacts:

```json
{
  "schema_version": "platform.serving-bundle.v1",
  "bundle_id": "b_07",
  "model": {
    "image_digest": "h_image",
    "weights_digest": "h_weights",
    "preprocessor_digest": "h_preprocessor",
    "training_dataset_digest": "h_td_fold_03",
    "feature_definition_digest": "h_feature_v3",
    "feature_schema_digest": "h_ordered_units_defaults",
    "label_contract": "return-or-refund-line-v2",
    "label_contract_digest": "h_label_v2",
    "score_semantics_digest": "h_probability_or_uncalibrated_contract"
  },
  "agent": {
    "prompt_digest": "h_prompt",
    "tool_schema_digest": "h_tools",
    "permission_policy_digest": "h_permissions",
    "corpus_digest": "h_corpus",
    "embedding_digest": "h_embedding",
    "index_digest": "h_index"
  },
  "compatibility_policy_digest": "h_compatibility"
}
```

Compatibility là so sánh contract có kiểm tra: ordered feature names, dtype,
units, categorical vocab/default/missing policy, definition digest, label
policy, output semantics và capability; không chỉ số cột. Generation không
cần có training dataset ID giống model. Bundle mới có thể dùng generation cũ
nếu tương thích; data mới không bắt buộc retrain. Model v1/proof cũ giữ target
v1; không đổi metadata thành v2 để dùng chung tên endpoint.

Startup endpoint kiểm image thực chạy, weights/preprocessor thực load, trả
observed bundle digest. Gateway chỉ route endpoint đã attest đúng bundle;
rolling pods khác bundle có route riêng. Chọn A/B arm một lần trong context,
không chọn lại giữa tool calls; mỗi arm phải tương thích cùng target/scope.
Rollback activation chọn cả generation/bundle đã xác minh, audit rõ rollback;
không tự route mutable `production` alias. Kết quả inference echo observed
model/bundle digest, không copy tên mong muốn từ gateway.

## 6. Control tables, fencing và atomic publish

Các bảng logic tối thiểu, triển khai migration riêng khi làm POC:

| Bảng | Khóa/ràng buộc chính |
|---|---|
| `environment` | singleton/environment ID, recovery epoch UUID, mode recovering/ready, policy digest |
| `publication_lane` | lane, epoch, next fence, lease owner/token/expiry; số thứ tự desired release do scheduler cấp |
| `run`/`attempt` | run input digest bất biến; attempt UUID + epoch + fence + status + namespace |
| `artifact`/`artifact_reference` | identity + digest + namespace + ready/deleting/deleted; refs tới parent/child artifacts |
| `generation`/`bundle` | IDs + content digests; insert-only content, lifecycle state tách riêng |
| `activation` | append-only activation UUID, generation digest, bundle digest, intent sequence, publisher epoch/fence, reason |
| `active_pointer` | lane + epoch + pointer version + activation ID |
| `reader_lease` | lease ID + activation/dependency references + epoch + expiry + subject/scope hash |
| `publication_event` | idempotency key + candidate digest + outcome + pointer version; unique key |

Control writes chỉ qua service/procedures, loader/agent không có UPDATE quyền
trực tiếp vào pointer/epoch. Dùng DB clock cho lease; process clock không quyết
định ai còn lease. Lock order cố định: environment → lane/pointer → artifact
refs/lease, tránh deadlock. PostgreSQL row locks tồn tại tới transaction end;
`SELECT ... FOR UPDATE` ngăn writer khác đổi cùng row nhưng không tự bảo vệ
Redis/Iceberg. [PostgreSQL 15 locking](https://www.postgresql.org/docs/15/explicit-locking.html).

Protocol:

1. Scheduler cấp intent sequence và input manifest; acquire lane lease trong
   transaction, tăng fence mỗi lần cấp/reacquire. Worker nhận `(epoch,fence,
   owner,expiry)`; renew có điều kiện đúng token, epoch và chưa hết hạn.
2. Attempt ghi namespace riêng. Worker cũ không được cùng namespace attempt
   mới. Trước seal: kết thúc mọi producer transaction/job; thu hồi loader
   credentials, dừng/đóng sessions đang ghi, khóa quyền sửa namespace. Với PG,
   service owner giữ schema, loader không phải owner; revoke không đủ nếu
   transaction cũ còn chạy. Với Redis, generation-specific credentials/prefix,
   kiểm revoke/disconnect thực tế; không tin lease flag trên ứng dụng là đủ.
   Nếu credential isolation/revocation hoặc producer shutdown không được chứng
   minh trên backend đã pin thì không seal/publish, không thay bằng sleep chờ.
3. Seal và validate exact output identities/digests. Chỉ candidate đã hoàn
   tất producer shutdown + seal evidence mới thành `VALIDATED`. Retry có token
   cũ không thể write/publish vào accepted namespace; cần POC credential và
   storage enforcement để chứng minh, không chỉ kiểm app-side.
4. Publisher transaction khóa environment/lane/pointer; kiểm environment ready,
   epoch/fence/owner/lease còn hạn bằng DB clock, required outputs/profile và
   dependency closure đều sealed/validated/not-deleting, compatibility bundle,
   expected pointer version và intent còn hợp lệ. Không gọi remote sink trong
   lúc giữ DB transaction dài; remote receipts đã được kiểm và namespace seal.
5. Kiểm lại expiry bằng `clock_timestamp()` ngay trước CAS trong transaction
   đang giữ lane lock; không dùng timestamp cố định lúc transaction bắt đầu.
   Insert activation/event, đổi pointer bằng CAS `WHERE epoch=:e AND
   pointer_version=:expected`; tăng pointer version, commit cùng transaction.
   CAS phải đổi đúng một row. Sai epoch/token/expiry/intention/CAS: rollback,
   không tự retry với latest pointer để vượt một publication mới hơn.
6. ACK mất: tra unique publication idempotency key. Cùng candidate đã commit
   trả kết quả cũ; cùng key khác digest là conflict. Nếu activation đã bị thay
   sau đó thì trả outcome lịch sử, không publish lại để kéo pointer lùi.

Pseudocode này mô tả service transaction, không phải migration chạy được:

```sql
BEGIN;
-- lock + check environment, lane lease, dependencies, compatibility first
SELECT * FROM platform_control.active_pointer WHERE lane = :lane FOR UPDATE;
-- application checks expected version, intent and exact candidate digest
INSERT INTO platform_control.activation (...) VALUES (...);
UPDATE platform_control.active_pointer
SET activation_id = :activation, pointer_version = pointer_version + 1
WHERE lane = :lane AND epoch = :epoch AND pointer_version = :expected;
-- assert affected_rows = 1; insert event with UNIQUE idempotency key
COMMIT;
```

CAS không tự ngăn một candidate cũ acquire token mới rồi publish đè dữ liệu
mới. Scheduler giữ desired intent sequence riêng: candidate chỉ được publish
nếu chưa superseded và cutoff không lùi trừ **explicit rollback intent** đã
kiểm dependency/compatibility. Mỗi release manifest ghi action/reason, không
sửa generation cũ. Giữ publication intent và active pointer version riêng.

Nếu required sink lỗi, pointer cũ giữ nguyên và freshness hiển thị đúng; nếu
store của activation đang đọc mất thật, request fail unavailable, không đọc
store latest khác. Profile có thể trả partial capability đã công bố từ trước,
nhưng không hạ requirement trong failure handling. Atomic pointer chỉ bảo đảm
visibility boundary, không chữa input sai hoặc output sai.

## 7. Turn, pagination, cache và historical routes

Gateway/server tạo context sau khi auth, không nhận scope/cutoff do LLM tự
mở rộng. Resolve pointer và tạo reader lease/dependency refs trong **cùng
transaction khóa control**; nếu GC đã đánh deleting thì pin fail. Context:

```json
{
  "context_id": "ctx_1",
  "epoch": "recovery_uuid_9",
  "activation_id": "a_12",
  "generation_digest": "h_g12",
  "bundle_digest": "h_b07",
  "lease_id": "lease_1",
  "view_mode": "as_known",
  "business_window": ["2026-06-01T00:00:00Z", "2026-07-01T00:00:00Z"],
  "knowledge_cutoff_exclusive": "2026-07-01T00:00:00Z",
  "scope_hash": "h_authorized_account_storefront_sku_currency_filters",
  "metric_contract_digest": "h_current_noncanceled",
  "deadline_wall": "2026-10-18T10:01:00Z",
  "budget_id": "budget_ctx_1"
}
```

Coordinator truyền context handle ký bởi server tới agents/tools/model/RAG;
LLM chỉ chọn tool/arguments trong schema và scope được phép. Tool results
echo context/generation/bundle/view/cutoff/coverage. Mismatch làm cả nhánh
abstain/retry cùng context, không merge evidence khác version. Turn mới resolve
activation mới và revalidate evidence cũ; lịch sử hội thoại không là nguồn số
liệu hiện tại. So hai generation là explicit comparison với hai context pin
và result tách từng vế, không tự cộng chúng.

Cursor ký/MAC gồm context, scope/query digest, sort keys + unique tie-break,
last key, expiry, auth scope digest. Page kế và drilldown giữ nguyên context;
lease hết hạn phải trả `CONTEXT_EXPIRED` và bắt đầu lại có chỉ báo rõ, không
fallback current. Không giữ lease trong mọi trang browser vô hạn; server
renew trong tổng thời hạn context được cấu hình, expired cursor không hồi sinh
lease đã GC. Token expiry phải không vượt thời gian server thực sự giữ pin.

Cache key bao gồm canonical full query + auth scope + epoch + activation,
generation/bundle/metric/tool schema digests + view/knowledge cutoff/business
window/currency/filter + A/B arm. Cache hit vẫn kiểm lease/context/authorization
hiện hành; cached answer không vượt quyền mới bị thu hồi. Có dữ liệu sensitivity
policy thì redact trước cache/log. Không dùng SKU đơn lẻ làm cache key.

API tách business window khỏi knowledge cutoff và view:

- `official`: đọc generation đã publish theo scope/window materialized.
- `as_known`: đọc historical source/history pin đủ để chỉ biết trước C.
- `restated`: cùng business window nhưng dùng correction biết tại cutoff C2
  được nêu rõ, không gọi đó thông tin đã biết tại cutoff C1.
- `live`: identity/freshness riêng, không join ngầm vào daily official answer.

Online Feast/Redis chỉ phục vụ generation/cutoff thực materialize. Historical
request cần offline route exact snapshot hoặc generation historical retained,
query bounded và manifest riêng; chưa materialize/hết retention thì trả
`HISTORY_UNAVAILABLE`/`UNSUPPORTED_VIEW`, không gửi version param xuống latest
online lookup. Mọi route phải kiểm capability/profile đúng yêu cầu query.

## 8. Retention, GC và phục hồi epoch sạch

Duy trì dependency graph cho generation, model training dataset, bundle,
validation/evidence và reader leases. Ref lâu dài dùng cho model/evidence còn
được retention policy bảo vệ; reader refs có expiry/renew. Không chỉ bảo vệ
active pointer rồi xóa training snapshots mà model cũ vẫn cần tái lập.

GC hai bước: trong transaction khóa artifact/control, xác nhận không còn
active/retained dependency hoặc lease hợp lệ và đánh `DELETING`; từ đó cấm
lease/ref mới. Sau đó xóa từng sink với idempotency/tombstone, ghi receipt rồi
`DELETED`. Crash giữa bước có thể retry; artifact thiếu không quay lại READY.
Lease renew đua GC phải dùng cùng lock/state check: renew sau deleting fail,
request đang mất lease phải dừng trước dùng dữ liệu tiếp. Không tự resolve
latest hoặc nhận kết quả backend muộn sau context expired.

Iceberg expiry/orphan cleanup chỉ chạy từ allowlist GC đã tính dependency
closure; cần giữ snapshot/tag refs phù hợp bản catalog/runtime. Orphan cleanup
phải bảo vệ file của writer đang chạy và ngưỡng tuổi theo policy, không quét
candidate chưa commit như rác. Redis expiry không được cắt ngắn generation
đang pin; nếu TTL adapter có thể expire feature trước retention thì phải
adjust/rebuild/validate hoặc đánh namespace unavailable. Offline TTL, online
TTL và API staleness là ba ràng buộc riêng.
[Nguồn Iceberg maintenance](https://iceberg.apache.org/docs/1.7.2/maintenance/).

Recovery procedure local trước G4:

1. Đóng ingress serving/writes và dừng scheduler/workers; revoke/rotate writer
   và service credentials cũ, terminate connections. Không chỉ đặt ready=false
   trong bản DB sắp bị restore. Bootstrap ngoài backup được operator/local
   script điều khiển; không cho old process tự bootstrap epoch.
2. Restore backup set/catalog/PG/artifacts. Sinh **epoch UUID mới sau restore**,
   khác epoch backup; rotate context signing credentials để token cũ mất hiệu
   lực. Fence counters có thể được restore về thấp nhưng epoch mới vẫn khác.
3. Kiểm exact dependency closure của activation dự định serve, including
   content/schema/coverage/compatibility và endpoint attestation. Redis có thể
   rebuild từ offline exact generation rồi validate; không coi marker cũ là
   bằng chứng materialize còn tồn tại. Rebuild thay namespace physical cần
   receipt/deployment activation mới tham chiếu generation content digest cũ;
   không sửa receipt immutable hoặc tái dùng tên ID cho content khác.
4. Writer mới chỉ nhận credential/token epoch mới. Reset leases cũ, kiểm GC và
   orphan candidates; không revive reader token từ backup. Khi mọi required
   deps hợp lệ thì đổi environment ready trong control transaction và mở
   ingress. Nếu không có activation hợp lệ, giữ unavailable; rollback sang bản
   giữ được phải là recovery activation rõ ràng, không gọi nó bản mới nhất.

Health process sống khác readiness. Readiness cần epoch hiện hành, control
reachable, required namespaces/receipts verified, model bundle attested, scope
coverage và freshness theo policy. Fail Redis chỉ làm unavailable capability
có dependency đó nếu output profile đã cho phép partial serving; không trả0.
RPO/RTO, backup cadence, TTL/lease/sweep interval và retention durations phải
được cấu hình/benchmark trước G4, chưa tự coi đạt từ thiết kế này.

## 9. Budgets và thực thi nhiều hop

Một request có tổng work budget do gateway cấp, dùng ledger server-side
atomic theo context. Agents không được tự cấp budget mới; mỗi nested call/
retry fan-out phải reserve từ cùng ledger trước dispatch, reconcile actual
sau call; quá hạn/thiếu quota thì không dispatch tiếp. Budget không chỉ là
HTTP count: giới hạn total tool attempts, query time, rows scanned/returned,
bytes, model tokens, concurrent tasks và total deadline. SQL LIMIT chỉ chặn
rows trả về, không chặn scan; từ chối unbounded window/tenant/filter trước
query, dùng indexed/materialized views, statement timeout và query layout
benchmark. Nếu engine không có hard byte-scan quota, ghi rõ giới hạn là
estimated + thời gian/concurrency, không tuyên bố hard bytes enforcement.

Policy demo để bắt đầu POC, chưa phải SLA/ngưỡng nghiệm thu đã đo:
`answer deadline 45s`, `tool attempts total 8` kể cả retry, `concurrency 2`,
`SQL statement 5s`, `returned rows/tool 500`, `response bytes/tool 1 MiB`,
`metric window <=180 ngày`; token/corpus/query-scan caps là config required
phải chốt theo bản model/engine trước POC tương ứng. Không lấy defaults này
thay benchmark CPU hoặc ngân sách Cloud; policy digest nằm trong bundle/input.
Retry chỉ lỗi transient còn budget/deadline; invalid scope, compatibility,
expired context, missing history hoặc unknown coverage không retry sang latest.

Roles tách publisher, source loader, serving query và model runtime. Agent
read-only chỉ gọi tool templates/allowlist views theo account; không raw SQL,
control-plane tables, arbitrary URL, filesystem hoặc secret endpoint. Serving
role không sở hữu tables, không CREATE/WRITE và không bypass scope filter;
service auth xác minh audience/subject/authorization ở mỗi boundary, không
passthrough token tùy ý. Read-only credentials vẫn cần query bounds.

Cancellation propagate gateway → coordinator → child tool → DB/model handle;
DB có query ID để cancel thực tế, không chỉ ngừng await. Spark/historical job
có job group/cancel và runtime cap; queued tasks giữ shared deadline. Backend
không hỗ trợ cancel phải cô lập worker với hard timeout/process limit trước
expose endpoint. Late response không được emit vào answer đã timeout. Cleanup
lease thực hiện sau cancel/finalize; trace ghi attempt/query IDs và actual
work/token usage, redact dữ liệu/credentials. PostgreSQL có `statement_timeout`
và `lock_timeout`; app vẫn phải gắn chúng vào query/session đúng scope.
[Nguồn PostgreSQL 15](https://www.postgresql.org/docs/15/runtime-config-client.html).

## 10. POC cần đạt và điều kiện đóng IR

Mỗi probe lưu input/output digests, fixture expected độc lập, environment/deps,
trace/actual work, cleanup state và result. Chưa có probe dưới đây chạy đạt.

| IR | Probe bắt buộc | Gate / điều kiện đóng |
|---|---|---|
| IR02 | Replace Bronze/sửa Variables giữa tasks và retry; upstream snapshot expire | G1/G2: build/validate exact manifest hoặc fail rõ, không trộn input |
| IR06 |0→1→immature tại ba cutoffs, same-time source conflict, maturity boundary | G2: historical selection đúng, training dataset cũ không đổi digest |
| IR10 | Cùng business window với as-known/restated; yêu cầu historical chưa materialize | G2/G3: kết quả đúng cutoff hoặc unsupported, không latest fallback |
| IR17 | Publish giữa tools/pages, retry/cache khác scope/currency, expired pin | G3: một turn/context đúng version; expiry kết thúc rõ; không leak quyền |
| IR18 | Weights sai, reorder units/schema, mixed rolling pods, wrong target | G2/G3: chặn trước inference; trace artifact thực; rollback đúng bundle |
| IR19 | Restore PG/catalog/Redis lệch thời điểm, old writer sống lại, old cached token | G4: epoch/credentials fence thật, dependency verified trước ready |
| IR20 | Required output thiếu; optional unavailable; capability subset năm Channel | G2/G3: không đổi profile để pass, không unknown→0, scope báo đúng |
| IR22 | Rule/config/dependency/prompt đổi giữa build/validate/serve; mutable alias | G0–G4: digest mismatch bị chặn hoặc tạo revision/activation mới |
| IR23 | Order D10, train info D60, delivery D65; replay vs actual timestamps | G2: không tính sample đó vào forward-at-order proof; fold clocks rõ |
| IR25 | Prompt injection/URL, huge scan, nested retry, concurrency/cancel | G3/G4: role/scope đúng, shared budgets enforce, backend work thực dừng/bounded |

POC publication còn phải ép hai writer/CAS conflict/ACK mất, stale intent,
worker đã mất lease nhưng còn ghi, pin đua GC, Redis TTL và seal enforcement,
Iceberg reference retention, compatibility và parity trên đúng phiên bản
Feast/Spark/catalog đã pin. Bảng trên ánh xạ vào S/G hiện có, không tuyên bố
thêm một con số scenario đã được triển khai. Kết quả fail phải giữ issue mở,
không đóng bằng việc có tài liệu này. Đặc biệt source/schema/capability mới,
backend physical sizing, dependency pins, hard query-scan/token caps và G4
retention/RPO/RTO vẫn cần evidence trước full-scale.
