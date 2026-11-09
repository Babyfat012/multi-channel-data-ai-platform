# Contract Return Label v2 — Q56/Q57

Đã chốt policy. ID: **`return-or-refund-line-v2`**.
ADR-0010 ghi trade-off;
file này quy định expected behavior để viết nguồn/transform/validation.
Chưa thay code generator hay label bridge hiện tại.

## Đơn vị đo và điều kiện

- Một mẫu là một Order Line theo dataset + namespace Channel/account +
  đầy đủ Order/line identity; nhiều units/shipment không nhân số mẫu.
- Mỗi phần giao có allocation ID, delivered quantity và actual delivery
  time quan sát được. Một event Return/Refund phải liên kết đúng line và
  allocation được tính; không đoán allocation theo lần giao gần nhất.
- Một line có quantity đã giao >0 và phần còn lại đã giao hoặc hủy mới đủ
  điều kiện xét maturity. Line hủy hết trước giao không có label; line còn
  phần chưa chốt là immature. Partial cancel không tạo ngày giao giả.
- Approved Return request có quantity hàng hợp lệ >0 hoặc successful product
  Refund có amount >0 và allocation tin cậy là sự kiện dương. Raw request
  chưa được chấp nhận, rejected, pending/failed Refund, shipping-only hoặc
  fee/goodwill chưa được định danh là product refund không tự tạo positive.
- Event time dùng **lúc Return request được chấp nhận** hoặc **lúc product
  Refund thành công**; không dùng request creation/pending time thay thế.
  Việc nhận Return vật lý, kiểm hàng và restock là các sự kiện riêng.

## Return ≠ Refund — ba ca và cách nhãn xử lý (chốt)

Ngoài đời Return (hàng gửi về) và Refund (tiền về) là **hai sự kiện độc lập**:

| Ca | Hàng về? | Tiền về? | Nhãn | `positive_kind` |
|---|---|---|---|---|
| Return + Refund | Có | Có | 1 | `return_and_refund` |
| Refund-only (khách giữ hàng) | Không | Có (product) | 1 | `refund_only` |
| Return-only (đổi hàng, voucher, bị từ chối sau kiểm) | Có | Không | 1 | `return_only` |
| Không có cả hai trong cửa sổ | — | — | 0 | null |

- Nhãn **cố ý gộp**: đo "Order Line gặp sự cố hậu mãi", không chỉ "mất tiền". Return-only vẫn dương (hàng về
  vẫn tốn vận chuyển/xử lý và là tín hiệu chất lượng).
- Refund sản phẩm **một phần** (amount > 0) vẫn dương; goodwill/fee/shipping không. Ngưỡng % (nếu cần) sẽ là
  phiên bản nhãn mới, không sửa v2.
- `positive_kind` chỉ có giá trị khi `label = 1`; tính trên sự kiện dương **trong cửa sổ** và **thấy được trước
  cutoff** của cùng line. Ghép theo line + cửa sổ, chưa theo `return_id`; khi mọi Channel có liên kết đó mới cân
  nhắc ghép chặt hơn. Không đổi giá trị nhãn ⇒ vẫn `return-or-refund-line-v2`.
- Feature đổi tên `return_or_refund_rate_90d` để tên không gợi ý "chỉ trả hàng".

## Hai đồng hồ và cửa sổ

Với allocation a giao tại d[a], cửa sổ của nó là:

```text
d[a] <= qualifying_event_time <= d[a] + 30 × 24 giờ
```

Tất cả timestamp UTC với precision của contract nguồn; không rút thành
chênh lệch ngày lịch. Dùng biên bao gồm đúng hạn 30 ngày, có fixture ngay
trước/đúng/sau hạn. Query snapshot dùng cutoff C dạng exclusive: chỉ chọn
bản nguồn có `source_observed_at < C`. Khoảng ngày là [start, end).
Muốn bao gồm sự kiện đúng cutoff phải dùng snapshot/cutoff sau nó.
Adapter có điều kiện timestamp inclusive phải ánh xạ biên này tường minh
theo precision nguồn; không mặc định PIT join của backend có cùng biên.

`maturity_end = max(d[a] + 30 ngày)` trên các phần thực giao được xét.
Line chỉ vào train/eval hoặc tỷ lệ chính thức khi fulfillment đã terminal,
`C > maturity_end`, thông tin terminal đã biết trước C, và feeds quan sát
có coverage/completeness ít nhất qua mọi deadline liên quan. Coverage phải
trỏ manifest/watermark/observation envelope của nguồn; không dùng truth ẩn
của simulator làm feature để chứng minh feed hoàn chỉnh.

Positive và negative cùng chờ điều kiện này. Live view có thể hiển thị một
approved Return sớm nhưng không đưa riêng positive sớm vào mẫu số chuẩn.
Nếu nguồn không thể chứng minh coverage thì trạng thái unknown/provisional
phải thể hiện rõ; không tự lấy 30 ngày wall-clock làm đầy đủ dữ liệu.

Khi line eligible, mature, đủ coverage và linkage:

```text
label = 1 nếu có ít nhất một qualifying event trong cửa sổ allocation của nó
label = 0 nếu không có event như vậy
cohort_date = ngày UTC của lần giao cuối phần thực giao
```

Chưa đủ hạn → immature; không có delivery → ineligible; conflict/thiếu
linkage/feed → unknown. State này nằm riêng với giá trị label nullable,
không mã hóa unknown thành 0. Line có positive rõ nhưng còn thiếu dữ liệu
bắt buộc vẫn không đi vào tập train/eval chuẩn dưới nhãn “đầy đủ”.

## Cancellation, reversal và correction

Target v2 là **đã có sự kiện hợp lệ** trong cửa sổ, không phải trạng thái
cuối cùng của quy trình hậu mãi. Vì vậy request đã được chấp nhận rồi được
hủy theo quy trình thông thường vẫn ghi nhận lịch sử approved event; refund
success rồi có money reversal là hai event khác nhau và không tự xóa sự
kiện từng hoàn tiền. Lifecycle/analytics thể hiện cả trạng thái sau đó.

Ngược lại, correction của nguồn xác nhận approval/success chưa từng hợp lệ,
hoặc sửa delivery/allocation/time, phải invalid/recompute event/label tương
ứng. Nếu nguồn không phân biệt reversal nghiệp vụ với correction dữ liệu,
giữ unknown/conflict, không tự suy. Đây là cách thực thi tiêu chí historical
event của Q57 cùng restatement Q36; không suy thành nhãn physical-return.

Correction chỉ được dùng kể từ lúc quan sát: generation mới recompute,
generation cũ giữ nguyên theo retention. Feature/training as-of cũ không
được thấy correction biết sau cutoff. Snapshot mới có thể chuyển mature
về immature/unknown nếu delivery/coverage được sửa; không giữ label cũ chỉ
vì đã từng materialize.

## Lịch sử, coverage và follow-up

### Lịch sử nhãn và uncertainty phải theo nguồn

Một `label_available_at` không thay toàn bộ lịch sử revision. Mỗi revision
cần label policy version, state/value, source snapshot/event references,
observation validity, terminal/coverage evidence và reason thay đổi. Giữ
materialized training dataset bất biến: feature as-of lúc đặt đơn khác
label observation cutoff trước training; outcome evaluation có cutoff riêng.
Không lọc label bằng cutoff feature rồi loại hết outcome tương lai hợp lệ.

Unmatched product Refund/Return phải giữ **affected scope**: biết tới Order
thì giữ các line có thể liên quan; biết tới line thì giữ các allocations
còn khả năng liên quan. Không inner join làm mất unmatched rồi chứng minh
các line còn lại âm. Feed completeness khác linkage coverage; thiếu Order
identity có thể gây vùng uncertainty lớn hơn theo envelope nguồn. Không
đoán allocation bằng việc số tiền tình cờ trùng. Quy tắc unknown bảo thủ
đã chốt được áp dụng trước khi kết luận label0/1 chính thức.

Nguồn actual delivery/approval/refund/allocation cần capability/authority
tường minh. Q61 đã chốt nguồn Seller operational giả lập theo
[source contract](source_contract_order_lifecycle.md) và sổ vấn đề;
không lấy promised delivery thay
actual hoặc biến truth ẩn thành nguồn Seller quan sát được.

### Coverage nối tiếp

Mỗi kind bắt buộc cần hợp của các envelope `complete`, receipt `complete`, có
`evidence_ref`, không unresolved scope và đã quan sát trước cutoff, phủ liên tục
`[min(d[a]), max(d[a])+30 ngày]`. Sắp interval theo start, mở rộng end khi start
không vượt end đã phủ. End cuối phải **lớn hơn** maturity vì end là exclusive.
Envelope đơn phủ kín là trường hợp riêng; gap dù nhỏ cũng unknown. Revision và
correction theo as-known như event, không dùng claim cũ đã bị thay thế.

Scale dùng tuần nối tiếp `[start+7k, min(start+7(k+1), followup_end))`;
không đòi từng envelope riêng lẻ phủ toàn cửa sổ. Digest/count/sequence của mỗi
envelope chỉ kiểm dòng đúng feed/producer/epoch/dataset, effective trong interval,
observed trước envelope. Receipt đủ không xóa uncertainty về linkage.

### Vòng đời theo dõi không dừng cùng lúc sinh Order

Tách `order_creation_end`, `lifecycle_followup_end` và
`source_observation_end`. Một line đặt gần ngày cuối có thể giao muộn,
qualification sau đó và observed còn muộn hơn; hết 30 ngày stream không tự
đủ maturity/coverage. Tail phải đủ theo scenario/lateness contract, hoặc
line được ghi immature/censored/unknown có tử/mẫu rõ. Không sinh thêm Order
mới chỉ để kéo dài follow-up cho cohort đã chọn.

Generator không được rút outcome độc lập lại chỉ vì tách một allocation
thành hai record của cùng physical world; OR hai Bernoulli p0.1 cho0.19
không tương đương p0.1 ban đầu. Cơ chế outcome/quantity phải version rõ và
không lộ future shipment count/approval delay vào feature lúc đặt đơn.

## Fixture nhãn có expected output độc lập

D0 là timestamp UTC, C sau mọi maturity trong case trừ khi ghi khác; feeds
đầy đủ và quantity terminal, linkage đúng nếu case không nêu thiếu.

| Case | Dữ kiện | Expected |
|---|---|---|
| L01 | Giao D0 và D20; approved Return của phần D20 ở D45 | 1; cohort D20; maturity_end D50 |
| L02 | Giao D0 và D20; approved Return của phần D0 ở D45; không event khác | 0; quá cửa sổ của phần D0, dù nằm sau lần giao cuối |
| L03 | Giao D0; request D29 nhưng được chấp nhận D31 | 0; event qualification ở D31 |
| L04 | Giao D0; product Refund success đúng D30 | 1 nếu source đã được quan sát trước C và coverage đủ |
| L05 | Giao D0; pending D28, failed D29, success D32; không Return | 0; pending không phải success |
| L06 | Shipping-only Refund D10; không Return/product Refund | 0 |
| L07 | 3 units: giao1 tại D0, hủy2; không event dương, C>D30 | 0; một mẫu, delivered1/canceled2, cohort D0 |
| L08 | Giao1 tại D0, còn2 đang treo; approved Return D5 | immature; chưa vào tập chuẩn |
| L09 | Hai allocations khác deadline; Refund map line nhưng thiếu allocation | unknown; không đoán cửa sổ |
| L10 | Approved D29 nhưng chỉ biết D35; C=D31 và feed chưa complete | unknown/provisional; không gọi 0 chính thức |
| L11 | Case L10, C=D40, coverage đầy đủ | 1; event time trong cửa sổ, known-time được tôn trọng |
| L12 | Approved D10 rồi buyer hủy request D12; không correction approval | 1; target historical approved event |
| L13 | Approved D10 bị source correction D40 xác nhận là dữ liệu sai | Generation trước correction giữ kết quả as-known; generation sau recompute 0 nếu đủ điều kiện |
| L14 | Hủy toàn line trước giao | ineligible, label null |
| L15 | Không event, maturity_end=D30 và C đúng D30 | immature; snapshot chưa bao phủ biên cuối |
| L16 | Order có A/B, product Refund success trong cửa sổ nhưng chỉ có Order ID | A/B unknown nếu đều còn khả năng liên quan; resolve về A thì recompute A1/B0 khi điều kiện khác đủ |
| L17 | Input revision lần lượt chứng minh label0, late qualifying event thành1, correction delivery làm chưa đủ maturity | Query từng observation snapshot trả0/1/immature đúng; training dataset cũ không đổi checksum |
| L18 | Các tuần trước đủ; hai tuần cuối `[D21,D28)` và `[D28,D35)` nối liền, deadline D30 | Đủ coverage; label theo sự kiện |
| L19 | Như L18 nhưng thiếu tuần `[D14,D21)` của một kind bắt buộc | unknown |
| L20 | Như L18 nhưng một tuần của một kind status/receipt partial | unknown |

Các ca trên có regression Python thuần; L01–L17 giữ nguyên expected.

## Migration, API và nghiệm thu

Version phải xuất hiện trên label dataset, feature/training manifest,
MLflow run/model metadata, serving compatibility và metric definition.
Model v1 có thể được giữ làm proof cũ; không ghi lại tag v2 cho artifact v1.
So sánh v1/v2 phải có bảng đổi eligibility/cohort/positive và lý do trên
cùng identity/dataset; không diễn giải metric khác target là model tốt hơn.

Generator hiện dùng request và một delivered_at cấp Order. V2 cần nguồn
shipment/approval/refund đủ quan sát và bridge theo line/allocation; không
chỉ thêm cột `label_version` vào nhãn hiện tại. Label/outcome chỉ dùng để
train/eval và analytics đủ hạn, không được làm feature lúc đặt đơn.

S19/S20 cùng S27/S28 kiểm contract này; L01–L17 là expected cases để triển
khai sau, chưa có runtime test được chạy đạt. Các giá trị coverage/freshness,
source schema, physical tables và dependency pins còn phải hoàn thành G0–G2.
