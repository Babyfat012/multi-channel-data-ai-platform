# Docker và Docker Compose trên EC2

EDAI-1, dòng 3–4: Docker/Compose và tối ưu Dockerfile.

Code / cấu hình: [Dockerfile](../../Dockerfile), [Compose AWS](../../docker-compose.aws.yml).

Các kết quả dưới đây thuộc các lần chạy AWS được ghi ngày, không xác nhận dịch vụ đang bật.

## Trên AWS (2026-10-27)

[Receipt AWS](../evidence/aws/data-platform-aws-20261027.json) ghi workload Compose trên EC2 Sydney;
[Airflow AWS](pipeline_orchestration.md#trên-aws-2026-10-27-đến-2026-10-28) nhúng Graph/Gantt của các pipeline đã chạy.
Cấu hình triển khai: [Compose AWS](../../docker-compose.aws.yml).

Các kỹ thuật tối ưu trong [`Dockerfile`](../../Dockerfile), đối chiếu với bản
[`Dockerfile.naive`](../../docker/Dockerfile.naive):

| Kỹ thuật | Cách áp dụng và tác dụng |
|---|---|
| Base image slim | Dùng `python:3.12-slim`, giảm thành phần hệ điều hành không cần khi chạy |
| Multi-stage | Stage cuối chỉ copy `.venv` và `configs/`, không mang theo công cụ cài đặt và cache của stage build |
| Cache ngoài image | Dùng BuildKit `--mount=type=cache` cho `uv`, tái sử dụng cache khi build mà không lưu cache đó trong image |
| Tách lớp thư viện và code | Copy `pyproject.toml` và `uv.lock`, cài thư viện trước khi copy generator; sửa code vẫn tận dụng cache lớp thư viện |
| Giảm build context | [`.dockerignore`](../../.dockerignore) loại dữ liệu sinh, môi trường ảo, Git và tài liệu; áp dụng cho cả hai image |

Đo lại trên chính EC2 (2026-10-31) bằng [compare_docker_dependencies.sh](../../scripts/evidence/compare_docker_dependencies.sh):
build [`docker/Dockerfile.naive`](../../docker/Dockerfile.naive) và [`Dockerfile`](../../Dockerfile) từ cùng code, liệt kê
thư viện trong từng image bằng `importlib.metadata` rồi `diff` ⇒ **cùng 27 package** (cùng `requirements.lock`), nên phần
giảm là nhờ cách build, không phải bỏ bớt thư viện.

| Image | Disk usage | Content size (nén, đẩy registry) | `docker image inspect` Size |
|---|---:|---:|---:|
| `platform-generator:naive` | 2,4 GB | 670 MB | 2.395.921.890 B |
| `platform-generator:opt` | 698 MB | 172 MB | 698.142.846 B |

Giảm **70,9%** dung lượng trên đĩa và **74,3%** dung lượng đẩy registry.

![EC2: compare_docker_dependencies.sh in digest + byte hai image, Cùng bộ thư viện: 27 package; docker image ls naive 2.4GB/670MB, opt 698MB/172MB](../evidence/aws/docker-naive-vs-opt-ec2-20261031.png)

<details>
<summary>Tham khảo: thiết kế, hướng dẫn phát triển và ghi nhận trước triển khai AWS</summary>

Các ghi nhận local giữ nguyên ngày và môi trường, không dùng thay bằng chứng cloud.

## Docker và Docker Compose

Tài liệu này ghi cách đóng gói generator thành image, cách dựng môi trường nguồn giả lập bằng Docker Compose, và số liệu tối ưu dung lượng image. Thiết kế tổng thể của generator xem [generator-design.md](reference/generator_design.md).

## Mục lục

1. [Môi trường nguồn bằng Docker Compose](#1-môi-trường-nguồn-bằng-docker-compose)
2. [Tối ưu Dockerfile](#2-tối-ưu-dockerfile)

## 1. Môi trường nguồn bằng Docker Compose

`docker-compose.yml` dựng hai hệ thống giả lập nơi dữ liệu đang nằm trước khi pipeline DP1 kéo về Bronze:

| Service | Image | Vai trò | Kiểu chạy |
|---|---|---|---|
| `minio` | `quay.io/minio/minio` (bản cố định theo ngày) | Object storage chứa export dạng file của Channel | Chạy liên tục. API cổng 9000, console cổng 9001 |
| `minio-init` | như trên (có sẵn `mc`) | Tạo bucket `platform` | Chạy một lần |
| `postgres` | `postgres:15` | Hệ thống của phòng ban khác: `walmart.orders`, `seller.skus` | Chạy liên tục, cổng 5432 |
| `generator-batch` | `platform-generator` (build từ `Dockerfile`) | `generator.generate_order_snapshots`: sinh lịch sử, ghi thẳng vào MinIO | Chạy một lần, sau `minio-init` |
| `generator-load` | `platform-generator` | `load-postgres`: đọc `pg-load/` trên MinIO, nạp vào PostgreSQL | Chạy một lần, sau `generator-batch` và khi `postgres` sẵn sàng |
| `kafka` (profile `stream`) | `apache/kafka:3.9.1` | Kafka một node, chế độ KRaft. Trong Compose là `kafka:9092`, từ host là `localhost:9094` | Chạy liên tục |
| `generator-stream` (profile `stream`) | `platform-generator` | `generator.publish_order_snapshot_events`: phát message sau `cutoff` vào Kafka, ghi `stream_manifest.json` vào MinIO | Chạy tới khi phát xong, sau khi `kafka` sẵn sàng |

Thứ tự được đảm bảo bằng `depends_on` với `service_healthy` (MinIO, PostgreSQL có healthcheck) và `service_completed_successfully` (các bước chạy một lần).

```bash
cp .env.example .env                  # đổi mật khẩu, cổng nếu cần
docker compose up -d                  # mặc định configs/small.yaml
docker compose wait generator-load    # chờ nạp PostgreSQL xong
GENERATOR_CONFIG=configs/default.yaml docker compose up -d --force-recreate generator-batch generator-load   # đủ quy mô
```

**Stream.** Kafka nằm trong profile `stream`, nên `docker compose up -d` không bật nó. `STREAM_SPEED` trong `.env` chỉnh tốc độ: 1 là đúng nhịp (khoảng 40 phút cho 30 ngày), 60 là nhanh gấp 60 lần (khoảng 40 giây).

```bash
docker compose --profile stream up -d kafka
docker compose --profile stream run --rm generator-stream     # in số đo khi xong
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic tiktok_shop.order-webhooks --from-beginning --max-messages 3
```

**Sau khi sửa code, nhớ `docker compose build`.** `Dockerfile` cài generator bằng `uv sync --reinstall-package platform-generator`: thiếu cờ đó, cache của `uv` chỉ so `pyproject.toml` nên sửa code mà không đổi phiên bản thì image vẫn cài bản cũ, build vẫn báo thành công.

**Vì sao chỉ một bucket `platform`.** Mỗi lần chạy ghi bốn thứ: `source-exports/`, `pg-load/`, `ground-truth/` và `run_manifest.json`. Manifest phải nằm trong một bucket, nên generator ghi vào gốc một bucket và bốn phần trên là bốn tiền tố. DP1 chỉ đọc `source-exports/`, còn `ground-truth/` dành cho phần huấn luyện mô hình.

**Mật khẩu và cổng** nằm trong `.env`, file này không được commit. Nếu máy đã có chương trình dùng cổng 9000, 9001 hoặc 5432, đổi `MINIO_API_PORT`, `MINIO_CONSOLE_PORT`, `POSTGRES_PORT` hoặc `KAFKA_PORT` trong `.env`.

**Đọc từ máy host.** Lệnh `report` cần ba biến môi trường để đọc MinIO:

```bash
set -a; . ./.env; set +a
export AWS_ACCESS_KEY_ID=$MINIO_ROOT_USER AWS_SECRET_ACCESS_KEY=$MINIO_ROOT_PASSWORD \
       FSSPEC_S3_ENDPOINT_URL=http://localhost:$MINIO_API_PORT
uv run python -m generator report --source s3://platform \
    --dsn "postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@localhost:$POSTGRES_PORT/$POSTGRES_DB"
```

## 2. Tối ưu Dockerfile

Hai Dockerfile cài cùng một generator với cùng thư viện, chỉ khác cách build:

| | `docker/Dockerfile.naive` | `Dockerfile` |
|---|---|---|
| Base image | `python:3.12` (Debian đầy đủ, có trình biên dịch, header) | `python:3.12-slim` |
| Số stage | 1 | 2: stage build cài vào `.venv`, stage chạy chỉ copy `.venv` |
| Trình cài | `pip install .`, giữ cache pip trong image | `uv sync --frozen --no-dev --no-editable`, cache nằm ngoài image (`--mount=type=cache`) |
| Nội dung copy | Cả build context | Stage chạy chỉ có `.venv` và `configs/`, không có mã nguồn, `uv` hay cache |
| Phiên bản thư viện | Theo `pyproject.toml`, mỗi lần build có thể khác | Khóa theo `uv.lock` |
| User | root | `nobody` |

Cả hai dùng chung `.dockerignore`, loại `.venv`, `out/`, `.git`, `docs/` và `tests/` khỏi build context. Vì vậy phần chênh lệch dưới đây đến từ cách build, không phải từ việc copy nhầm dữ liệu.

### Kết quả

Đo bằng `docker images data-generator` (Docker 29):

| Image | Dung lượng |
|---|---|
| `data-generator:naive` | 2,35 GB |
| `platform-generator:optimize` | 677 MB |

![Dung lượng hai image](../images/docker/docker.png)

Trong ảnh, cột `DISK USAGE` là dung lượng sau khi giải nén trên máy. Cột `CONTENT SIZE` là dung lượng nén khi đẩy lên registry, giảm từ 658 MB xuống 167 MB.

Dung lượng giảm khoảng 71%. Phần lớn đến từ base image: `python:3.12` mang theo bộ công cụ build mà generator không cần lúc chạy. Phần còn lại đến từ việc không giữ cache cài đặt.

**Phần còn lại của 677 MB là thư viện**, đo bằng `du` trong image:

| Thành phần | Dung lượng |
|---|---|
| `pyarrow` (ghi Parquet) | 156 MB |
| `duckdb` (lệnh `report`) | 58 MB |
| `numpy` và thư viện đi kèm | 70 MB |
| `botocore` (qua `s3fs`, ghi MinIO) | 30 MB |
| `psycopg` bản binary (gồm libpq) | 19 MB |
| Python của base slim (`/usr/local`) | 37 MB |

Muốn giảm thêm có thể tách `duckdb` thành nhóm thư viện tùy chọn, vì lệnh `report` thường chạy trên máy host. Hiện vẫn giữ trong image để chạy được báo cáo ngay trong Compose.

### Cách đo lại

```bash
docker build -t data-generator:naive -f docker/Dockerfile.naive .
docker build -t platform-generator:optimize .
docker images data-generator
```

## 3. Đo lại 2026-10 — cùng bộ thư viện

Phép đo ở mục 2 là **lịch sử**: khi đó `Dockerfile.naive` cài `pip install .` (không khóa phiên bản), nên câu "cùng
thư viện, chỉ khác cách build" chưa được chứng minh. Từ 2026-10-19, image naive cài từ `requirements.lock` (xuất từ
`uv.lock` bằng `scripts/setup/export_requirements_lock.sh`, có hash) rồi `pip install --no-deps .`. `scripts/evidence/compare_docker_dependencies.sh`
build cả hai từ cùng working tree, liệt kê package bằng `importlib.metadata` trong từng image và **fail nếu khác nhau**.

| Image | Digest | Dung lượng |
|---|---|---|
| `platform-generator:naive` | `sha256:d9af23a28aed…` | 2,39 GB |
| `platform-generator:opt` | `sha256:00360a65a6a0…` | 698 MB |

Cùng **27 package** (bỏ pip/setuptools/wheel của base và chính project) ⇒ giảm **70,8 %** chỉ nhờ cách build (base slim,
multi-stage, không giữ cache). Receipt: [`evidence/docker-compare-20261019.json`](../evidence/docker-compare-20261019.json)
(kèm sha256 của Dockerfile, lock file, `.dockerignore`). `.dockerignore` loại thêm `.briefs/`, `.vibe-wise/`, `.claude/`
(ghi chú làm việc, không phải mã nguồn).

## 4. Inventory image/service đã ghi nhận

`scripts/evidence/inspect_docker_images.py` đọc `docker compose config` và `docker image inspect` (không pull):
[local, mọi profile](../evidence/docker-inventory-local-20261019.json) — 20 service;
[GCP overlay, profile catalog/spark/airflow](../evidence/docker-inventory-gcp-20261019.json) — 9 service, mọi port bind `127.0.0.1`.

## Tên tài nguyên và dữ liệu đã có

Cấu hình hiện hành dùng Compose project `platform`, image `platform-*`, cụm kind `platform`, và tiền tố Airflow Connection/Variable `platform_`. Các tên này được dùng thống nhất trong script setup, manifest và hướng dẫn.

Nếu nâng cấp từ checkout có tên tài nguyên khác, named volume, bucket, database, Vault path và Airflow metadata cũ vẫn thuộc môi trường cũ. Đổi tên cấu hình không di chuyển dữ liệu hoặc secret. Giữ bản sao dữ liệu và cấu hình cần dùng, chọn môi trường đích rồi chạy lại các bước setup tương ứng; không dùng `down -v` để đổi tên. Evidence lịch sử giữ nguyên định danh của lần chạy gốc.

</details>

[Mục lục phần này](../README.md) · [Mục lục chung](../../README.md)
