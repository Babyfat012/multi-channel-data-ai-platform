#!/usr/bin/env bash
# Host-side planner: fixture → real DAG → MCP → UI screenshots, splitting RAM across two phases.
set -euo pipefail
set -E  # The ERR trap also applies inside functions/subshells.
cd "$(dirname "$0")/../.."
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"

usage() {
  cat <<'HELP'
Usage: bash scripts/run/run_order_lifecycle_live.sh --phase source|airflow|datahub|capture|all
Default: --phase all. --help only prints usage, does not call Docker.
Environment variables:
  PLATFORM_ORDER_LIFECYCLE_DIR  host fixture (default ./out/order_lifecycle_live)
  PLATFORM_ORDER_LIFECYCLE_CUTOFF      default 2026-06-01T00:00:00.000000Z
  PLATFORM_V2_RUN_ID      picks the new run on trigger; picks the old run on publish
  PLATFORM_V2_TIMEOUT    DAG timeout in seconds (1800); PLATFORM_V2_START_TIMEOUT (600)
  PLATFORM_V2_EXECUTOR_MEMORY  default 2g, fits the default worker for the small fixture
  PLATFORM_STORAGE       minio (default) or s3; read from the environment or .env
  PLATFORM_PYTHON        local .venv/bin/python; S3 /mnt/platform-data/venv/bin/python
  RAW_BUCKET, AWS_REGION, PLATFORM_DATASET_NAME  S3 source synced by Ansible
  DATAHUB_PYTHON     Python with the acryl-datahub SDK, defaults to the prepared uv environment
  DATAHUB_GMS_URL    default http://127.0.0.1:8089
  DATAHUB_GMS_TOKEN  optional token, passed only via the environment
  DATAHUB_QUICKSTART_DIR  default $HOME/.datahub/quickstart
Receipt: out/receipts-v2-run/<run_id>/; latest run: out/receipts-v2-run/latest-run-id
Requires Docker Compose, curl, Chrome, aiohttp, and the quickstart/DataHub SDK installed (docs/data-platform/data_governance.md).
HELP
}
phase=all
while (($#)); do
  case "$1" in
    --phase) [[ $# -ge 2 ]] || { usage >&2; exit 2; }; phase=$2; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *) echo "Invalid flag: $1" >&2; usage >&2; exit 2 ;;
  esac
done
case "$phase" in source|airflow|datahub|capture|all) ;; *) echo "Invalid phase: $phase" >&2; exit 2 ;; esac
# Read required values from .env, keep the environment's precedence, and do not export secrets.
if [[ ! -v PLATFORM_STORAGE && -f .env ]]; then
  export PLATFORM_STORAGE="$( (source .env; printf '%s' "${PLATFORM_STORAGE:-minio}") )"
fi
if [[ "${PLATFORM_STORAGE:-minio}" = s3 && -f .env ]]; then
  for key in RAW_BUCKET PLATFORM_DATASET_NAME AWS_REGION PLATFORM_SPARK_PACKAGES_AWS COMPOSE_FILE PLATFORM_ORDER_LIFECYCLE_CUTOFF; do
    if [[ ! -v "$key" ]]; then
      value=$( (set -a; source .env; printf '%s' "${!key:-}") )
      export "$key=$value"
    fi
  done
fi
storage="${PLATFORM_STORAGE:-minio}"
source_root=/data/order_lifecycle
export PLATFORM_ORDER_LIFECYCLE_DIR="${PLATFORM_ORDER_LIFECYCLE_DIR:-./out/order_lifecycle_live}"
py="${PLATFORM_PYTHON:-.venv/bin/python}"
if [[ "$storage" = s3 ]]; then
  source_root="s3a://${RAW_BUCKET:?RAW_BUCKET is required}/order_lifecycle/${PLATFORM_DATASET_NAME:-fixture}"
  py="${PLATFORM_PYTHON:-/mnt/platform-data/venv/bin/python}"
  export PLATFORM_ORDER_LIFECYCLE_DIR="/mnt/platform-data/source/${PLATFORM_DATASET_NAME:-fixture}"
fi
cutoff="${PLATFORM_ORDER_LIFECYCLE_CUTOFF:-2026-06-01T00:00:00.000000Z}"
timeout="${PLATFORM_V2_TIMEOUT:-1800}"
start_timeout="${PLATFORM_V2_START_TIMEOUT:-600}"
for seconds in "$timeout" "$start_timeout"; do
  [[ "$seconds" =~ ^[1-9][0-9]*$ ]] || { echo 'Timeout must be a positive integer number of seconds' >&2; exit 2; }
done
compose=(docker compose --profile airflow --profile spark)
# Stop the dependencies too after phase A; do not delete volumes or stop other projects' services.
services=(airflow spark-worker-1 spark-worker-2 spark-master lakekeeper lakekeeper-migrate lakekeeper-db-init airflow-db-init minio-init minio postgres)
if [[ "$storage" = s3 ]]; then
  services=(airflow spark-worker-1 spark-worker-2 spark-master lakekeeper lakekeeper-migrate lakekeeper-db-init airflow-db-init postgres)
fi
quickstart="${DATAHUB_QUICKSTART_DIR:-$HOME/.datahub/quickstart}"
export DATAHUB_VERSION="${DATAHUB_VERSION:-v1.7.0.1}"
export UI_INGESTION_DEFAULT_CLI_VERSION="${UI_INGESTION_DEFAULT_CLI_VERSION:-1.7.0.14}"
export DATAHUB_MAPPED_GMS_PORT=8089 DATAHUB_MAPPED_FRONTEND_PORT=9002
hub=(docker compose --env-file "$quickstart/.local-secrets.env" --profile quickstart
     -f "$quickstart/docker-compose.yml" -f docker/datahub-compose.override.yml -p datahub)
gms="${DATAHUB_GMS_URL:-http://127.0.0.1:8089}"
cleanup_hint() {
  local code=$?
  echo "Phase $phase failed (exit $code). Data/receipts kept intact for log inspection." >&2
  if [[ "$storage" = s3 ]]; then
    echo 'Clean up phase A: docker compose --profile airflow --profile spark stop airflow spark-worker-1 spark-worker-2 spark-master lakekeeper postgres' >&2
  else
  echo 'Clean up phase A: docker compose --profile airflow --profile spark stop airflow spark-worker-1 spark-worker-2 spark-master lakekeeper postgres minio' >&2
  fi
  echo 'Clean up phase B: rerun the DataHub Compose command in docs/data-platform/data_governance.md, replacing up -d with stop.' >&2
  echo 'Do not use down -v; do not delete volumes. Check the logs, then use a new run_id if Gold needs to be rerun.' >&2
  exit "$code"
}
trap cleanup_hint ERR

wait_until() {
  local label=$1 deadline=$((SECONDS + start_timeout))
  shift
  until "$@" >/dev/null 2>&1; do
    if ((SECONDS >= deadline)); then echo "Timeout waiting for $label" >&2; return 1; fi
    sleep 5
  done
}
source_phase() {
  if [[ "$storage" = s3 ]]; then
    aws s3 ls "s3://${RAW_BUCKET}/order_lifecycle/${PLATFORM_DATASET_NAME:-fixture}/manifest.json" --region "${AWS_REGION:?AWS_REGION is required}"
    return
  fi
  if [[ -f "$PLATFORM_ORDER_LIFECYCLE_DIR/manifest.json" ]]; then
    echo "Keeping existing fixture: $PLATFORM_ORDER_LIFECYCLE_DIR"
  else
    # Do not overwrite a half-written directory or someone else's source.
    if [[ -d "$PLATFORM_ORDER_LIFECYCLE_DIR" ]] && [[ -n "$(ls -A "$PLATFORM_ORDER_LIFECYCLE_DIR")" ]]; then
      echo 'Source directory is not empty but has no manifest; choose a new directory.' >&2; return 1
    fi
    "$py" -m generator.generate_order_lifecycle_fixture --output "$PLATFORM_ORDER_LIFECYCLE_DIR"
  fi
}
airflow_ready() {
  "${compose[@]}" exec -T airflow python -c '
import json, urllib.request
with urllib.request.urlopen("http://localhost:8080/health", timeout=5) as r:
    h = json.load(r)
assert h["metadatabase"]["status"] == h["scheduler"]["status"] == "healthy"
'
}
spark_ready() {
  "${compose[@]}" exec -T spark-master python3 -c '
import json, urllib.request
with urllib.request.urlopen("http://localhost:8080/json/", timeout=5) as r:
    h = json.load(r)
assert h["aliveworkers"] >= 2
'
}
dag_ready() {
  "${compose[@]}" exec -T airflow python -c '
from airflow.models.dag import DagModel
assert DagModel.get_current("dp_order_lifecycle") is not None
'
}
run_info() {
  "${compose[@]}" exec -T airflow python - "$run_id" <<'PY' | tail -n 1
import json, sys
from airflow.models.dagrun import DagRun
runs = DagRun.find(dag_id="dp_order_lifecycle", run_id=sys.argv[1])
if not runs:
    raise SystemExit("Triggered run not found")
r = runs[0]
print(json.dumps({"state": r.state, "directory": r.execution_date.strftime("%Y%m%dT%H%M%S")}))
PY
}
airflow_phase() {
  [[ "$storage" = s3 || -f "$PLATFORM_ORDER_LIFECYCLE_DIR/manifest.json" ]] || { echo 'Run --phase source first' >&2; return 1; }
  if [[ -n "$(docker ps -q --filter label=com.docker.compose.project=datahub)" ]]; then
    echo 'Stop the DataHub Compose stack before phase A to free enough RAM (docs/data-platform/data_governance.md).' >&2; return 1
  fi
  mkdir -p out/receipts out/receipts-v2-run
  # The Airflow user must be able to write the freshly created bind mount. Do not touch old receipt file permissions.
  chmod 0777 out/receipts
  # minio-init runs once and exits; newer Compose treats that exit as a --wait failure, so it runs separately.
  if [[ "$storage" = s3 ]]; then
    source_phase
    "${compose[@]}" up -d --wait --wait-timeout "$start_timeout" airflow spark-master spark-worker-1 spark-worker-2
  else
  "${compose[@]}" up -d minio-init
  "${compose[@]}" up -d --wait --wait-timeout "$start_timeout" airflow spark-master spark-worker-1 spark-worker-2 minio
  fi
  wait_until Airflow airflow_ready
  wait_until Spark spark_ready
  wait_until DAG-v2 dag_ready
  bash scripts/setup/setup_airflow.sh
  "${compose[@]}" exec -T airflow airflow variables set platform_order_lifecycle_executor_memory "${PLATFORM_V2_EXECUTOR_MEMORY:-2g}"
  "${compose[@]}" exec -T airflow airflow dags list-import-errors --output json
  run_id="${PLATFORM_V2_RUN_ID:-}"
  # No ':' or '/' in the default run_id so the host directory stays easy to use.
  if [[ -z "${PLATFORM_V2_RUN_ID:-}" ]]; then
    run_id="live_v2__$(date -u +%Y%m%dT%H%M%S)_$("$py" -c 'import uuid; print(uuid.uuid4().hex[:8])')"
  fi
  validate_run_id
  if [[ "$storage" = s3 ]]; then
  conf=$("$py" -c 'import json,sys; print(json.dumps({"source_root":sys.argv[2], "cutoff":sys.argv[1]}))' "$cutoff" "$source_root")
  else
  conf=$("$py" -c 'import json,sys; print(json.dumps({"source_root":"/data/order_lifecycle", "cutoff":sys.argv[1]}))' "$cutoff")
  fi
  "${compose[@]}" exec -T airflow airflow dags unpause dp_order_lifecycle
  "${compose[@]}" exec -T airflow airflow dags trigger dp_order_lifecycle --run-id "$run_id" --conf "$conf"
  echo "run_id=$run_id"
  local deadline=$((SECONDS + timeout)) state info directory
  while :; do
    info=$(run_info)
    state=$("$py" -c 'import json,sys; print(json.loads(sys.argv[1])["state"])' "$info")
    case "$state" in success|failed) break ;; esac
    if ((SECONDS >= deadline)); then echo "Timeout DAG: $run_id; see Airflow logs/receipt" >&2; return 1; fi
    sleep 10
  done
  directory=$("$py" -c 'import json,sys; print(json.loads(sys.argv[1])["directory"])' "$info")
  mkdir -p "out/receipts-v2-run/$run_id"
  "${compose[@]}" cp "airflow:/opt/airflow/receipts/order_lifecycle/$directory/." "out/receipts-v2-run/$run_id/"
  "${compose[@]}" stop "${services[@]}"
  [[ "$state" = success ]] || { echo "DAG failed: $run_id; receipts copied, not publishing" >&2; return 1; }
  printf '%s\n' "$run_id" > out/receipts-v2-run/latest-run-id
  echo "DAG success: $run_id; receipts in out/receipts-v2-run/$run_id/; phase A stopped."
}
validate_run_id() {
  [[ "$run_id" =~ ^[A-Za-z0-9_][A-Za-z0-9_.:+-]*$ ]] || { echo 'run_id is not valid as a host directory name' >&2; return 1; }
}
datahub_phase() {
  run_id="${PLATFORM_V2_RUN_ID:-$(cat out/receipts-v2-run/latest-run-id)}"
  validate_run_id
  local receipts="out/receipts-v2-run/$run_id"
  # Refuse to publish the wrong run or an incomplete receipt set; never infer PASS from a missing file.
  "$py" - "$receipts" "$run_id" <<'PY'
import json, sys
from pathlib import Path
root, run = Path(sys.argv[1]), sys.argv[2]
for task in ("check_contracts", "ingest_bronze", "build_silver", "build_gold", "validate_gold"):
    doc = json.loads((root / (task + ".json")).read_text())
    assert doc["run_id"] == run, f"Wrong run_id: {task}"
    assert doc.get("status", "passed") == "passed", f"Receipt failed: {task}"
    assert all(v == 0 for v in doc.get("checks", {}).values()), f"Check failed: {task}"
PY
  "${compose[@]}" stop "${services[@]}"
  "${hub[@]}" up -d
  wait_until GMS curl --fail --silent --show-error --max-time 5 "$gms/health"
  local publisher="${DATAHUB_PYTHON:-$HOME/.local/share/uv/tools/acryl-datahub/bin/python}"
  local args=(--receipt-dir "$receipts" --source-root "$source_root" --server "$gms")
  "$publisher" scripts/run/publish_order_lifecycle_datahub.py --dry-run "${args[@]}" > "$receipts/datahub-mcps.json"
  "$publisher" scripts/run/publish_order_lifecycle_datahub.py --publish "${args[@]}"
}
capture_phase() {
  wait_until DataHub-UI curl --fail --silent --show-error --max-time 5 http://127.0.0.1:9002
  "$py" scripts/evidence/capture_order_lifecycle_datahub_evidence.py
}
case "$phase" in
  source) source_phase ;;
  airflow) airflow_phase ;;
  datahub) datahub_phase ;;
  capture) capture_phase ;;
  all)
    source_phase; airflow_phase; datahub_phase
    if [[ "$storage" = s3 ]]; then
      echo 'Run on the laptop over a tunnel: .venv/bin/python scripts/evidence/capture_order_lifecycle_datahub_evidence.py'
    else
      capture_phase
    fi
    ;;
esac
