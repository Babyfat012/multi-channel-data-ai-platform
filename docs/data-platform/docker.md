# Docker and Docker Compose on EC2

This page covers using Docker/Compose and optimizing the Dockerfile.

Code / configuration: [Dockerfile](../../Dockerfile), [Compose AWS](../../docker-compose.aws.yml).

The results below are from recorded AWS runs; this page does not claim the services are still running.

## On AWS

The services run with Docker Compose on EC2 in Sydney; the [AWS receipt](../evidence/aws/data-platform-aws-20261027.json)
records those workloads. The [Airflow AWS](pipeline_orchestration.md#on-aws) page embeds Graph/Gantt screenshots of the
pipelines that ran. Deployment configuration: [Compose AWS](../../docker-compose.aws.yml).

The table below lists the optimization techniques used in [`Dockerfile`](../../Dockerfile), compared with the unoptimized
[`Dockerfile.naive`](../../docker/Dockerfile.naive):

| Technique | How it is applied and what it does |
|---|---|
| Slim base image | Uses `python:3.12-slim`, removing OS components not needed at runtime |
| Multi-stage | The final stage only copies `.venv` and `configs/`, without the install tools and cache of the build stage |
| Cache outside the image | Uses BuildKit `--mount=type=cache` for `uv`, reusing the cache at build time without storing it in the image |
| Separate dependency and code layers | Copies `pyproject.toml` and `uv.lock` and installs dependencies before copying the generator; code changes still reuse the dependency layer cache |
| Smaller build context | [`.dockerignore`](../../.dockerignore) excludes generated data, virtual environments, Git and docs; applies to both images |

**Re-measured on the EC2 instance itself.** The script [compare_docker_dependencies.sh](../../scripts/evidence/compare_docker_dependencies.sh)
does three steps:

1. Build [`docker/Dockerfile.naive`](../../docker/Dockerfile.naive) and [`Dockerfile`](../../Dockerfile) from the same code.
2. List the packages installed in each image with `importlib.metadata`.
3. Compare the two lists with `diff`.

Result: the two images have **the same 27 packages** (both installed from `requirements.lock`). So the size reduction comes from how the image is
built, not from dropping dependencies.

| Image | Disk usage (unpacked, on disk) | Content size (compressed, pushed to the registry) | `docker image inspect` Size |
|---|---:|---:|---:|
| `platform-generator:naive` | 2.4 GB | 670 MB | 2,395,921,890 B |
| `platform-generator:opt` | 698 MB | 172 MB | 698,142,846 B |

The optimized image is **70.9%** smaller on disk and **74.3%** smaller to push to the registry.

![EC2: compare_docker_dependencies.sh prints digest + bytes of both images, Same dependency set: 27 packages; docker image ls naive 2.4GB/670MB, opt 698MB/172MB](../evidence/aws/docker-naive-vs-opt-ec2-20261031.png)

<details>
<summary>Reference: design, development guide and records from before the AWS deployment</summary>

Local records keep their original environment and do not replace cloud evidence.

## Docker and Docker Compose

This document describes how the generator is packaged as an image, how the simulated source environment is set up with Docker Compose, and the image size optimization numbers. For the overall generator design see [generator-design.md](reference/generator_design.md).

## Contents

1. [Source environment with Docker Compose](#1-source-environment-with-docker-compose)
2. [Dockerfile optimization](#2-dockerfile-optimization)

## 1. Source environment with Docker Compose

`docker-compose.yml` sets up two simulated systems where data lives before the DP1 pipeline pulls it into Bronze:

| Service | Image | Role | Run mode |
|---|---|---|---|
| `minio` | `quay.io/minio/minio` (date-pinned release) | Object storage holding the Channels' file exports | Runs continuously. API on port 9000, console on port 9001 |
| `minio-init` | same as above (includes `mc`) | Creates the `platform` bucket | Runs once |
| `postgres` | `postgres:15` | Another department's system: `walmart.orders`, `seller.skus` | Runs continuously, port 5432 |
| `generator-batch` | `platform-generator` (built from `Dockerfile`) | `generator.generate_order_snapshots`: generates history and writes it straight into MinIO | Runs once, after `minio-init` |
| `generator-load` | `platform-generator` | `load-postgres`: reads `pg-load/` on MinIO and loads it into PostgreSQL | Runs once, after `generator-batch` and once `postgres` is ready |
| `kafka` (profile `stream`) | `apache/kafka:3.9.1` | Single-node Kafka in KRaft mode. Inside Compose it is `kafka:9092`, from the host `localhost:9094` | Runs continuously |
| `generator-stream` (profile `stream`) | `platform-generator` | `generator.publish_order_snapshot_events`: publishes messages after the `cutoff` into Kafka and writes `stream_manifest.json` to MinIO | Runs until publishing is done, after `kafka` is ready |

Ordering is guaranteed with `depends_on` using `service_healthy` (MinIO and PostgreSQL have healthchecks) and `service_completed_successfully` (the run-once steps).

```bash
cp .env.example .env                  # change passwords and ports if needed
docker compose up -d                  # defaults to configs/small.yaml
docker compose wait generator-load    # wait for the PostgreSQL load to finish
GENERATOR_CONFIG=configs/default.yaml docker compose up -d --force-recreate generator-batch generator-load   # full scale
```

**Stream.** Kafka is in the `stream` profile, so `docker compose up -d` does not start it. `STREAM_SPEED` in `.env` sets the speed: 1 is real time (about 40 minutes for 30 days), 60 is 60 times faster (about 40 seconds).

```bash
docker compose --profile stream up -d kafka
docker compose --profile stream run --rm generator-stream     # prints measurements when done
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic tiktok_shop.order-webhooks --from-beginning --max-messages 3
```

**After changing code, remember to run `docker compose build`.** The `Dockerfile` installs the generator with `uv sync --reinstall-package platform-generator`: without that flag, the `uv` cache only compares `pyproject.toml`, so if code changes without a version bump the image still installs the old version and the build still reports success.

**Why only one `platform` bucket.** Each run writes four things: `source-exports/`, `pg-load/`, `ground-truth/` and `run_manifest.json`. The manifest has to live in one bucket, so the generator writes to the root of one bucket and the four parts above are four prefixes. DP1 only reads `source-exports/`; `ground-truth/` is for model training.

**Passwords and ports** live in `.env`, which is not committed. If the machine already has programs using ports 9000, 9001 or 5432, change `MINIO_API_PORT`, `MINIO_CONSOLE_PORT`, `POSTGRES_PORT` or `KAFKA_PORT` in `.env`.

**Reading from the host.** The `report` command needs three environment variables to read MinIO:

```bash
set -a; . ./.env; set +a
export AWS_ACCESS_KEY_ID=$MINIO_ROOT_USER AWS_SECRET_ACCESS_KEY=$MINIO_ROOT_PASSWORD \
       FSSPEC_S3_ENDPOINT_URL=http://localhost:$MINIO_API_PORT
uv run python -m generator report --source s3://platform \
    --dsn "postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@localhost:$POSTGRES_PORT/$POSTGRES_DB"
```

## 2. Dockerfile optimization

The two Dockerfiles install the same generator with the same dependencies; only the build approach differs:

| | `docker/Dockerfile.naive` | `Dockerfile` |
|---|---|---|
| Base image | `python:3.12` (full Debian, with compilers and headers) | `python:3.12-slim` |
| Stages | 1 | 2: the build stage installs into `.venv`, the runtime stage only copies `.venv` |
| Installer | `pip install .`, keeps the pip cache in the image | `uv sync --frozen --no-dev --no-editable`, cache outside the image (`--mount=type=cache`) |
| Copied content | The whole build context | The runtime stage only has `.venv` and `configs/`, no source code, `uv` or cache |
| Dependency versions | From `pyproject.toml`, may differ on every build | Locked by `uv.lock` |
| User | root | `nobody` |

Both share the same `.dockerignore`, which excludes `.venv`, `out/`, `.git`, `docs/` and `tests/` from the build context. So the difference below comes from the build approach, not from accidentally copied data.

### Results

Measured with `docker images data-generator` (Docker 29):

| Image | Size |
|---|---|
| `data-generator:naive` | 2.35 GB |
| `platform-generator:optimize` | 677 MB |

![Size of the two images](../images/docker/docker.png)

In the screenshot, the `DISK USAGE` column is the unpacked size on the machine. The `CONTENT SIZE` column is the compressed size when pushed to the registry, down from 658 MB to 167 MB.

The size drops by about 71%. Most of it comes from the base image: `python:3.12` ships a build toolchain the generator does not need at runtime. The rest comes from not keeping the install cache.

**The rest of the 677 MB is dependencies**, measured with `du` inside the image:

| Component | Size |
|---|---|
| `pyarrow` (Parquet writing) | 156 MB |
| `duckdb` (the `report` command) | 58 MB |
| `numpy` and bundled libraries | 70 MB |
| `botocore` (via `s3fs`, MinIO writes) | 30 MB |
| `psycopg` binary build (includes libpq) | 19 MB |
| Python of the slim base (`/usr/local`) | 37 MB |

To shrink further, `duckdb` could move into an optional dependency group, since the `report` command usually runs on the host. For now it stays in the image so the report can run inside Compose.

### How to re-measure

```bash
docker build -t data-generator:naive -f docker/Dockerfile.naive .
docker build -t platform-generator:optimize .
docker images data-generator
```

## 3. Re-measured 2026-10 — same dependency set

The measurement in section 2 is **historical**: at that time `Dockerfile.naive` ran `pip install .` (no pinned versions), so the claim "same
dependencies, only the build approach differs" was not proven. Now the naive image installs from `requirements.lock` (exported from
`uv.lock` with `scripts/setup/export_requirements_lock.sh`, with hashes) and then `pip install --no-deps .`. `scripts/evidence/compare_docker_dependencies.sh`
builds both from the same working tree, lists the packages with `importlib.metadata` in each image and **fails if they differ**.

| Image | Digest | Size |
|---|---|---|
| `platform-generator:naive` | `sha256:d9af23a28aed…` | 2.39 GB |
| `platform-generator:opt` | `sha256:00360a65a6a0…` | 698 MB |

The two images have the same **27 packages** (excluding the base image's pip/setuptools/wheel and the project itself). So the
**70.8%** reduction comes entirely from the build approach (slim base, multi-stage, no cache kept). Receipt: [`evidence/docker-compare-20261019.json`](../evidence/docker-compare-20261019.json)
(including the sha256 of the Dockerfile, the lock file and `.dockerignore`). `.dockerignore` also excludes `.briefs/`, `.vibe-wise/`, `.claude/`
(working notes, not source code).

## 4. Recorded image/service inventory

`scripts/evidence/inspect_docker_images.py` reads `docker compose config` and `docker image inspect` (no pull):
[local, all profiles](../evidence/docker-inventory-local-20261019.json) — 20 services;
[GCP overlay, profiles catalog/spark/airflow](../evidence/docker-inventory-gcp-20261019.json) — 9 services, every port bound to `127.0.0.1`.

## Resource names and existing data

The current configuration uses the Compose project `platform`, images `platform-*`, the kind cluster `platform`, and the Airflow Connection/Variable prefix `platform_`. These names are used consistently in setup scripts, manifests and guides.

If you upgrade from a checkout with different resource names, the old named volumes, buckets, databases, Vault paths and Airflow metadata still belong to the old environment. Renaming the configuration does not move data or secrets. Keep copies of the data and configuration you need, choose the target environment, then rerun the corresponding setup steps; do not use `down -v` to rename. Historical evidence keeps the identifiers of the original run.

</details>

[Section index](README.md) · [Main index](../README.md)
