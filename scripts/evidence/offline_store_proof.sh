#!/usr/bin/env bash
# In bằng chứng offline store của stream feature sku_return_24h (đọc thẳng S3, không cần bật cluster).
# Dùng: AWS_PROFILE=platform scripts/evidence/offline_store_proof.sh
# Mặc định đọc thư mục part của job PyFlink offline; URI=…/sku_return_24h.parquet đọc file của bản SQL + pusher (31/10).
set -euo pipefail
URI=${URI:-s3://platform-665116832975-lakehouse/feature-store/sku_return_24h/}
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

if [[ "$URI" == */ ]]; then
  echo "\$ aws s3 ls $URI --recursive --summarize"
  aws s3 ls "$URI" --recursive --human-readable --summarize | tail -6
  aws s3 cp "$URI" "$TMP/" --recursive --quiet --exclude '*' --include 'part-*.parquet'
  glob="$TMP/**/*.parquet"
else
  echo "\$ aws s3 ls $URI"
  aws s3 ls "$URI" --human-readable
  aws s3 cp "$URI" "$TMP/f.parquet" --quiet
  glob="$TMP/f.parquet"
fi

uv run -q --no-project --with duckdb python -I - "$glob" <<'PY'
import sys, duckdb
f = sys.argv[1]
duckdb.sql("SET TimeZone='UTC'")
show = lambda title, sql: (print(f"\n# {title}"), duckdb.sql(sql.format(f=f)).show(max_rows=20))
show("Tổng: số file, số dòng, số SKU, khoảng thời gian (UTC)",
     "select count(distinct filename) n_files, count(*) AS n_rows, count(distinct seller_sku) skus, "
     "min(event_timestamp) first_ts, max(event_timestamp) last_ts from read_parquet('{f}', filename=true)")
show("Giá trị cuối mỗi SKU — so với /features (Redis)",
     "select seller_sku, max(event_timestamp) last_ts, arg_max(approved_returns_24h, event_timestamp) last_value "
     "from read_parquet('{f}') where seller_sku in ('BEA-01708','ELE-01084','APP-00042') group by 1 order by 1")
show("Lịch sử BEA-01708 (offline giữ từng giờ, Redis chỉ giữ số mới nhất)",
     "select distinct event_timestamp, approved_returns_24h from read_parquet('{f}') "
     "where seller_sku='BEA-01708' order by 1 desc limit 8")
PY
