#!/usr/bin/env bash
# Chạy hai job Silver trên cụm Spark standalone.
#
# Vì sao script RIÊNG chứ không thêm đích vào `evidence/measure_order_snapshots_bronze.sh`: script kia là
# bằng chứng đo hiệu năng của tầng Bronze (`docs/data-platform/reference/spark.md` mục 5 và 6) — mỗi đích trong đó
# gắn với một con số đã ghi vào tài liệu. Silver là tầng khác, chạy vì mục đích khác (ra
# bảng dùng được, không phải ra số so sánh), nên để riêng cho khỏi lẫn.
#
# BẮT BUỘC bật MinIO trước, cùng lý do như Bronze: `spark-master` không `depends_on` MinIO,
# nên job đọc và tính xong rồi gãy ĐÚNG LÚC GHI với `BadRequestException: Malformed request`.
#   docker compose up -d minio minio-init postgres
#   docker compose --profile spark up -d spark-master spark-worker-1 spark-worker-2
#
# ĐỪNG chạy `docker compose --profile spark up -d` trần — nó bật luôn `generator-batch`
# và sinh lại 100 GB vào MinIO.
#
# Các đích:
#   smoke    — chạy thử nhanh trên hai bảng Bronze NHỎ, ghi ra bảng Silver có hậu tố
#              `_smoke`. Dùng để kiểm job chạy được trước khi đụng bảng thật.
#   dup      — BẰNG CHỨNG DEDUP: nạp Bronze từ 10 ngày LIỀN NHAU (2026-04-05..04-14) rồi
#              chạy Silver lên nó. Phải là ngày liền nhau vì bản trùng của generator chỉ
#              nằm trong overlap_days=3 — 20 ngày rải đều cho dedup bắt được đúng 0 bản.
#              Cửa sổ này cố ý trùm lên 04-07..04-12, nơi hai dạng schema chồng nhau, nên
#              một lần chạy chứng minh được cả dedup lẫn schema evolution.
#              Số đã đo: 134.990 bản trùng (1,5976%), 34.080 vắt qua hai dạng.
#              Xem `docs/data-platform/reference/silver.md` mục 10.6. Tốn ~400 MB đĩa, nhớ DROP sau khi đo.
#   walmart  — bảng Walmart thật  -> silver.stg_walmart_order_lines
#   amazon   — bảng Amazon thật   -> silver.stg_amazon_order_lines  (NẶNG: 108 triệu dòng)
#   tatca    — walmart rồi amazon
#
# Dùng:  bash scripts/run/run_order_snapshots_silver.sh [smoke|dup|walmart|amazon|tatca]
set -euo pipefail
cd "$(dirname "$0")/../.."
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"

WHICH="${1:-smoke}"
# Driver chạy ở worker-1 chứ không ở spark-master: container master chỉ có 512 MB và bị
# Docker kill OOM (phát hiện khi chạy brief 02 của tầng Bronze).
MASTER_CONTAINER="${MASTER_CONTAINER:-platform-spark-worker-1-1}"
ICEBERG_PKGS="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0"
LOG_DIR="out/silver-runs"
mkdir -p "$LOG_DIR"

submit() {
  # $1 = tên lần chạy (dùng cho tên file log), $2 = file job, $@ còn lại = cờ cho job
  local name="$1"; shift
  local job="$1"; shift
  local log="$LOG_DIR/${name}.log"
  printf '==> %s  (log: %s)\n' "$name" "$log"
  docker exec "$MASTER_CONTAINER" /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages "$ICEBERG_PKGS" \
    --driver-memory "${DRIVER_MEMORY:-600m}" \
    ${EXTRA_SUBMIT_ARGS:-} \
    "/opt/spark_jobs/$job" "$@" > "$log" 2>&1
  grep -E "^run_id|^bảng (đích|nguồn)|^Order Line|^bản trùng|^tỉ lệ|^XONG" \
    "$log" || tail -20 "$log"
}

case "$WHICH" in
  smoke)
    # Hai bảng Bronze nhỏ đã có sẵn từ lúc dựng Bronze: `*_smoke` (Walmart, 20 file) và
    # `raw_amazon_orders_sample` (20 ngày mẫu). Đủ để kiểm cả hai nhánh schema của Amazon,
    # vì 20 ngày rải đều cắt qua cả mốc đổi schema (export day 100 = 2026-04-10).
    submit "silver-walmart-smoke" silver/build_walmart_order_snapshots.py \
      --source-table raw_walmart_orders_smoke --table stg_walmart_order_lines_smoke
    submit "silver-amazon-smoke" silver/build_amazon_order_snapshots.py \
      --source-table raw_amazon_orders_sample --table stg_amazon_order_lines_smoke
    ;;
  dup)
    # Hai bước: nạp Bronze 10 ngày liền nhau, rồi Silver lên đúng bảng đó.
    # Bronze dùng ICEBERG_PKGS như Silver — job 01 không cần driver JDBC.
    printf '==> bronze-dup (10 ngày liền nhau 2026-04-05..04-14)\n'
    docker exec "$MASTER_CONTAINER" /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 --packages "$ICEBERG_PKGS" \
      --driver-memory "${DRIVER_MEMORY:-600m}" \
      /opt/spark_jobs/bronze/ingest_amazon_order_snapshots.py \
      --window-days 10 --window-start 2026-04-05 --aqe on \
      --table raw_amazon_orders_dup > "$LOG_DIR/bronze-dup.log" 2>&1
    grep -E "^ngày sẽ nạp|^  ->|^XONG" "$LOG_DIR/bronze-dup.log" || tail -5 "$LOG_DIR/bronze-dup.log"
    submit "silver-amazon-dup" silver/build_amazon_order_snapshots.py \
      --source-table raw_amazon_orders_dup --table stg_amazon_order_lines_dup
    ;;
  walmart|tatca)
    submit "silver-walmart" silver/build_walmart_order_snapshots.py
    ;;&
  amazon|tatca)
    submit "silver-amazon" silver/build_amazon_order_snapshots.py
    ;;
  *)
    echo "Tham số phải là smoke | dup | walmart | amazon | tatca" >&2
    exit 2
    ;;
esac
