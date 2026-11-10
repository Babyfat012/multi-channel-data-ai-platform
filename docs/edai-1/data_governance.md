# Data Governance trên AWS

Trang này ứng với EDAI-1, dòng 28–33 của rubric: lineage (dữ liệu đi từ đâu tới đâu) và validation (kiểm tra chất
lượng dữ liệu) của ba pipeline DP1/DP2/DP3.

Code / cấu hình: [Manifest AWS](../evidence/aws/datahub-validation-aws-20261029.json).

Các kết quả dưới đây là của các lần chạy AWS đã ghi lại; trang này không khẳng định dịch vụ hiện vẫn đang bật.

## Trên AWS

DataHub 1.7.0.1 chạy bằng Compose trên EC2. Lần publish metadata của sáu DAG ghi vào DataHub 22 dataset, 30 job và 16
assertion (luật kiểm tra chất lượng), tất cả đều PASS. Thông tin được đưa vào bằng hai script:

- [Publisher](../../scripts/run/publish_order_snapshots_datahub.py) khai lineage dựa trên input/output của từng DAG.
- [Builder validation](../../scripts/evidence/build_order_snapshots_datahub_validation.py) lấy kết quả kiểm tra từ log
  của các task Airflow đã chạy thành công.

[Manifest AWS](../evidence/aws/datahub-validation-aws-20261029.json) ghi run_id, task_id, rule và số đo của từng check.
Ảnh tab Quality chỉ cho thấy assertion có dấu xanh. Muốn biết run_id và số chi tiết thì đọc manifest; không suy ra
từ nhãn “hours ago” trên giao diện.

| DAG / run_id | Bảng và nhiệm vụ | Assertion đã ghi |
|---|---|---|
| `dp1_bronze` / `manual__2026-10-27T14:58:12+00:00` | Task `ingest_amazon` đọc payload từ S3 và ghi vào `bronze.raw_amazon_orders_dp1`. Bốn bảng raw còn lại giữ dữ liệu nguồn của từng kênh | `validate_amazon`: số dòng nguồn bằng số dòng Bronze, đều 108.248.563; 0 dòng invalid. Mỗi kênh có check riêng |
| `dp2_silver` / `manual__2026-10-27T16:05:58+00:00` | Task `build_gold_order_lines` gộp năm bảng `silver.stg_*_order_lines_dp2` thành `gold.fact_order_lines_dp2`. Bảng `gold.dim_amazon_order_dp2` giữ lịch sử theo kiểu SCD2 (lưu cả bản hiện hành và các bản cũ) | `validate_gold_order_lines`: Gold có 227.200.303 dòng, bằng Silver; 0 dòng invalid, 0 khóa trùng. Validator của bảng dimension kiểm tra khóa nối với fact và mỗi đơn chỉ có một bản hiện hành |
| `dp3_features` / `manual__2026-10-28T00:05:21.167441+00:00` | Task `build_return_features` đọc `gold.fact_order_lines_dp2` và ghi `gold.feat_order_line_return_dp3`: feature tại thời điểm đặt Order | `validate_return_features`: 227.200.303 dòng, bằng Gold. Số dòng thiếu (missing), thừa (extra), sai event time, trùng khóa và invalid đều bằng 0 |

Code pipeline và vai trò từng task: [Airflow AWS](pipeline_orchestration.md#trên-aws).

### DP1: nguồn và Bronze

![DataHub lọc AWS S3: bốn dataset nguồn Amazon, Shopee, Shopify và TikTok Shop ở source-exports](../evidence/aws/datahub/s3-sources.png)

![DataHub Bronze Amazon: nguồn S3 qua ingest_amazon tới raw_amazon_orders_dp1 và các task downstream](../evidence/aws/datahub/dp1-lineage.png)

![DataHub Quality raw_amazon_orders_dp1: assertion dp1_bronze.validate_amazon có dấu xanh và Passing (1)](../evidence/aws/datahub/dp1-validation.png)

### DP2: Silver và Gold

![DataHub Gold fact: năm nhánh Bronze qua task Silver, hợp vào build_gold_order_lines rồi fact_order_lines_dp2](../evidence/aws/datahub/dp2-lineage.png)

![DataHub Quality fact_order_lines_dp2: assertion dp2_silver.validate_gold_order_lines có dấu xanh](../evidence/aws/datahub/dp2-validation.png)

### DP3: feature

![DataHub: fact_order_lines tới build_return_features, feat_order_line_return rồi validate_return_features](../evidence/aws/datahub/dp3-lineage.png)

![DataHub Quality feat_order_line_return_dp3: assertion dp3_features.validate_return_features có dấu xanh](../evidence/aws/datahub/dp3-validation.png)

### CDC, nhãn và warehouse

[DAG CDC](../../dags/dp1_cdc.py), run `manual__2026-10-28T10:51:33.422898+00:00`: task `ingest_walmart_cdc` đọc thay
đổi từ Kafka và ghi vào `bronze.raw_walmart_cdc_v2`. Validator kiểm 3 offset; không có tin thiếu, thừa hay sai payload.
[Probe và giải thích CDC](reference/cdc.md#trên-aws).

![DataHub CDC: Kafka orders qua ingest_walmart_cdc tới raw_walmart_cdc_v2 và validate_walmart_cdc](../evidence/aws/datahub/dp1-cdc-lineage.png)

![DataHub Quality raw_walmart_cdc_v2: assertion dp1_cdc.validate_walmart_cdc có dấu xanh](../evidence/aws/datahub/dp1-cdc-validation.png)

[DAG nhãn](../../dags/dp3_return_labels.py), run `manual__2026-10-28T16:18:15.667507+00:00`:
task `build_and_validate_return_labels` nối Gold với đáp án do generator sinh, ghi ra `gold.fact_return_labels_dp3`
gồm 179.436.428 dòng; theo manifest, 0 dòng trùng và 0 dòng invalid. Nhãn được để ở bảng riêng, tách khỏi feature,
để tránh leakage (mô hình vô tình nhìn thấy đáp án khi train).

![DataHub: năm nhánh Silver qua Gold fact và task build_and_validate tới fact_return_labels_dp3](../evidence/aws/datahub/dp3-return-labels-lineage.png)

[DAG warehouse](../../dags/dp4_warehouse.py), run `manual__2026-10-29T08:34:06.602356+00:00`:
task `export_and_index_warehouse` chép 30 ngày dữ liệu Gold sang PostgreSQL `warehouse.fact_order_lines_30d_dp2`.
Trước khi tạo index, assertion đối soát 37.045.741 dòng: đủ năm Channel, số dòng mỗi bên khớp nhau.

![DataHub: năm bảng Silver qua Gold fact và export_and_index_warehouse tới warehouse.fact_order_lines_30d_dp2](../evidence/aws/datahub/dp4-warehouse-lineage.png)

![DataHub Quality warehouse.fact_order_lines_30d_dp2: assertion dp4_warehouse.export_and_index_warehouse có dấu xanh](../evidence/aws/datahub/dp4-warehouse-validation.png)

Giới hạn:

- Lineage được khai trong publisher, chưa tự suy ra từ Spark execution plan.
- Metadata và assertion ở đây là lịch sử của các run có trong manifest; không khẳng định mọi run sau này đều tự
  publish.
- Sidebar của UI đang lỗi, nhưng các trang dataset/Lineage/Quality trong ảnh vẫn mở được.

<details>
<summary>Tham khảo: thiết kế, hướng dẫn phát triển và ghi nhận trước triển khai AWS</summary>

Các ghi nhận local giữ nguyên môi trường, không dùng thay bằng chứng cloud.

## DataHub — lineage và data validation của DP1/DP2/DP3

## Phạm vi bằng chứng

DataHub GMS `v1.7.0.1` đã nhận **21 dataset** (22 sau khi thêm bảng CDC v2), **29 Airflow job** và **15 assertion**
từ `scripts/run/publish_order_snapshots_datahub.py`. Sáu dataset nguồn gồm JSONL Amazon,
ba nguồn JSONL Channel nhỏ, bảng PostgreSQL `walmart.orders` và topic Kafka CDC;
15 dataset đích là các bảng Iceberg Bronze/Silver/Gold. Metadata nối nguồn →
Bronze → Silver → Gold → feature bằng input/output của job Airflow và cạnh
dataset lineage. Nhánh CDC nối topic Kafka với bảng Bronze riêng.

Các assertion đọc kết quả **đã chạy** từ log Airflow trong volume `platform_airflow-data`;
không chạy lại Spark khi publish. `scripts/evidence/build_order_snapshots_datahub_validation.py`
chỉ nhận attempt có dòng `Marking task as SUCCESS`, trích số đo và thời điểm,
đối chiếu điều kiện PASS rồi ghi
[`evidence/datahub-validation.json`](../evidence/datahub-validation.json). Mỗi assertion
trên DataHub giữ `run_id` và metrics gốc trong kết quả SUCCESS. Đây là bản ghi
bằng chứng của các run đã hoàn tất, chưa phải lịch giám sát validation tự động.

| Pipeline | Dataset trên ảnh | Upstream thể hiện trên Lineage | Validate SUCCESS |
|---|---|---|---|
| DP1 | `lakehouse.bronze.raw_amazon_orders_dp1` | JSONL Amazon → `ingest_amazon` → Bronze | Nguồn = Bronze **5.827.514**, lỗi **0**, run `manual__2026-10-13T15:30:09+00:00` |
| DP2 | `lakehouse.gold.fact_order_lines_dp2` | Năm bảng Silver → `build_gold_order_lines` → Gold | Silver = Gold **10.194.358**, lỗi **0**, khóa trùng **0**, run `manual__2026-10-14T03:53:52+00:00` |
| DP3 | `lakehouse.gold.feat_order_line_return_dp3` | Gold fact → `build_return_features` → feature | Gold = feature **10.194.358**, lỗi **0**, khóa trùng **0**, run `manual__2026-10-14T04:11:15+00:00` |

Ảnh được chụp từ tab `Lineage` và `Quality` của UI thật. Tab Quality hiển thị
`AIRFLOW SPARK VALIDATE` và `1 Passing` cho từng dataset đại diện.

### DP1

![Nguồn Amazon tới Bronze trong DataHub Lineage](../images/datahub/dp1-lineage.png)

![Assertion DP1 hiển thị Passing trong DataHub Quality](../images/datahub/dp1-validation.png)

### DP2

![Năm bảng Silver tới Gold trong DataHub Lineage](../images/datahub/dp2-lineage.png)

![Assertion DP2 hiển thị Passing trong DataHub Quality](../images/datahub/dp2-validation.png)

### DP3

![Gold fact tới feature trong DataHub Lineage](../images/datahub/dp3-lineage.png)

![Assertion DP3 hiển thị Passing trong DataHub Quality](../images/datahub/dp3-validation.png)

## Bằng chứng chạy lại

Code CDC (bảng mới `raw_walmart_cdc_v2`, boundary theo run,
source epoch) và validator feature (so tập khóa, event time) đã đổi. Hai
check này được chạy lại bằng code hiện tại; mười ba check còn lại giữ run cũ vì
code của chúng không đổi.

| Check | Run mới | Kết quả |
|---|---|---|
| DP1 CDC `lakehouse.bronze.raw_walmart_cdc_v2` | `manual__2026-10-18T05:14:10+00:00` | Kafka = Bronze = offset duy nhất **4**; thiếu, thừa, sai payload, lỗi đều **0** ([chi tiết](reference/cdc.md#kết-quả-cdc-v2)) |
| DP3 `lakehouse.gold.feat_order_line_return_dp3` | `manual__2026-10-18T05:24:29+00:00` | Gold = feature **10.194.358**; thiếu, thừa, sai event time, khóa trùng, lỗi đều **0** |

Lần DP3 đầu tiên trong ngày (`manual__2026-10-18T05:16:26+00:00`) **thất bại**
vì executor hết heap khi chỉ bật một Spark worker. Lệnh `createOrReplace` bị
hủy nên bảng feature cũ vẫn nguyên. Bật đủ hai worker như cấu hình gốc thì run
thứ hai thành công. Bản chạy lại vẫn là DP3 cũ trên Gold cũ, chưa phải
feature/nhãn theo nguồn v2.

Manifest mới là [`datahub-validation-v2.json`](../evidence/datahub-validation-v2.json),
tạo bằng `--run` và `--cdc-table`; mỗi check ghi `log_format` (`v1`, `cdc-v2`,
`feature-v2`). Manifest cũ [`datahub-validation.json`](../evidence/datahub-validation.json)
giữ nguyên. Check trên bảng v2 có assertion URN riêng (hậu tố `-v2`), nên kết
quả của bảng CDC v1 vẫn là lịch sử riêng; bảng v1 được ghi là legacy trong
mô tả dataset.

Ảnh Quality chỉ hiện `1 Passing`, không hiện `run_id`. Vì vậy kết quả assertion
được đọc thẳng từ GraphQL của DataHub và lưu ở
[`datahub-assertions-v2-20261018.json`](../evidence/datahub-assertions-v2-20261018.json):
check DP3 có hai lần SUCCESS, lần mới nhất là run sau khi đổi code; check CDC v2 có
run sau khi đổi code; check CDC v1 vẫn là run trước đó.

![CDC v2: topic Kafka → ingest_walmart_cdc → raw_walmart_cdc_v2 → validate](../images/datahub/v2/dp1-cdc-lineage.png)

![Assertion CDC v2 hiển thị Passing](../images/datahub/v2/dp1-cdc-validation.png)

![Gold fact tới feature, sau lần chạy lại DP3](../images/datahub/v2/dp3-lineage.png)

![Assertion DP3 hiển thị Passing, kết quả mới nhất là run sau khi đổi code](../images/datahub/v2/dp3-validation.png)

Tái tạo bộ v2 (sau khi các run trên đã có trong volume Airflow):

```bash
docker run --rm -v platform_airflow-data:/logs:ro -v "$PWD":/work -w /work \
  python:3.12-slim python scripts/evidence/build_order_snapshots_datahub_validation.py \
  --logs-root /logs/logs --output docs/evidence/datahub-validation-v2.json \
  --run dp1_cdc=manual__2026-10-18T05:14:10+00:00 \
  --run dp3_features=manual__2026-10-18T05:24:29+00:00 \
  --cdc-table lakehouse.bronze.raw_walmart_cdc_v2
"$HOME/.local/share/uv/tools/acryl-datahub/bin/python" \
  scripts/run/publish_order_snapshots_datahub.py --server http://127.0.0.1:8089 \
  --evidence docs/evidence/datahub-validation-v2.json
.venv/bin/python scripts/evidence/capture_order_snapshots_datahub_evidence.py --profile v2
```

## Cách tái tạo trên máy đã chuẩn bị DataHub quickstart

Image `mysql:8.2` tại lần pull này có `/bin/bash` 0 byte; image frontend
`v1.7.0.1` có nhiều Play JAR 0 byte. File
[`docker/datahub-compose.override.yml`](../../docker/datahub-compose.override.yml)
dùng MySQL `8.0` và frontend `v1.6.0.2` đã kiểm tra chạy được cùng GMS
`v1.7.0.1`. Đây là cấu hình phục vụ proof cục bộ, cần đồng bộ phiên bản khi triển
khai dài hạn.

```bash
DATAHUB_VERSION=v1.7.0.1 UI_INGESTION_DEFAULT_CLI_VERSION=1.7.0.14 \
DATAHUB_MAPPED_GMS_PORT=8089 DATAHUB_MAPPED_FRONTEND_PORT=9002 \
docker compose --env-file "$HOME/.datahub/quickstart/.local-secrets.env" \
  --profile quickstart -f "$HOME/.datahub/quickstart/docker-compose.yml" \
  -f docker/datahub-compose.override.yml -p datahub up -d

docker run --rm -v platform_airflow-data:/logs:ro -v "$PWD":/work -w /work \
  python:3.12-slim python scripts/evidence/build_order_snapshots_datahub_validation.py \
  --logs-root /logs/logs

"$HOME/.local/share/uv/tools/acryl-datahub/bin/python" \
  scripts/run/publish_order_snapshots_datahub.py --server http://127.0.0.1:8089
.venv/bin/python scripts/evidence/capture_order_snapshots_datahub_evidence.py
```

Lần khởi tạo này OpenSearch tự khóa tạo index khi ổ hệ thống còn 13 GB; sau khi
giải phóng image Data & AI Platform không dùng, đã tắt `cluster.routing.allocation.disk.threshold_enabled`
cho OpenSearch DataHub **một node**, kiểm tra dung lượng bằng `df -h /` trong lúc
chạy. Nếu Compose dừng ở `403 cluster create-index blocked`, chỉ sau khi xác
nhận ổ vẫn còn đủ chỗ, gỡ khóa trong OpenSearch DataHub rồi chạy lại lệnh
`docker compose ... up -d` ở trên:

```bash
docker exec datahub-opensearch-1 curl -sS -X PUT \
  -H 'Content-Type: application/json' \
  'http://localhost:9200/_cluster/settings' \
  -d '{"persistent":{"cluster.routing.allocation.disk.threshold_enabled":false,"cluster.blocks.create_index":null}}'
```

Không áp dụng cấu hình đó cho môi trường chia sẻ hoặc production.
Không xóa Docker volume để lấy chỗ: các volume lớn khác thuộc dự án khác.

UI ở `http://localhost:9002`; GMS ở `http://localhost:8089`.

</details>

[Mục lục phần này](README.md) · [Mục lục chung](../README.md)
