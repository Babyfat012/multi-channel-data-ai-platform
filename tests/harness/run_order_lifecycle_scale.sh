#!/usr/bin/env bash
# Run by the planner: dedicated network/DB, offline jars, receipts kept in out, resources cleaned up on failure.
set -euo pipefail
cd "$(dirname "$0")/../.."
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"
PROFILE=medium
DRIVER_MEMORY=6g
ARGS=()
while (($#)); do
  case "$1" in
    --profile) PROFILE="${2:?Missing profile}"; shift 2 ;;
    --driver-memory) DRIVER_MEMORY="${2:?Missing driver memory}"; shift 2 ;;
    *) ARGS+=("$1"); shift ;;
  esac
done
case "$PROFILE" in small|medium) ;; *) echo 'Profile must be small or medium' >&2; exit 2 ;; esac
IMAGE=platform-airflow:2.10.5-spark3.5.3
PREFIX="platform-e2e-v2-$$"
NETWORK="$PREFIX-net"
mkdir -p out
WORK="$(mktemp -d "$PWD/out/e2e-v2-${PROFILE}.XXXXXX")"
chmod 777 "$WORK"
cleanup() {
  # Use the image's existing user for Spark; root is only used to chown output back to the host, even on failure.
  docker rm -f "$PREFIX-spark" "$PREFIX-postgres" >/dev/null 2>&1 || true
  docker network rm "$NETWORK" >/dev/null 2>&1 || true
  docker run --rm --pull never --network none --user 0 -v "$WORK":/proof \
    --entrypoint chown "$IMAGE" -R "$(id -u):$(id -g)" /proof >/dev/null 2>&1 || true
  printf 'Receipt/log kept at %s\n' "$WORK"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
docker volume inspect platform_airflow-data >/dev/null
docker image inspect "$IMAGE" >/dev/null
docker image inspect postgres:15 >/dev/null
docker run --rm --pull never --network none -v platform_airflow-data:/ivy:ro \
  --entrypoint bash "$IMAGE" -c '
    ls /ivy/.ivy2/jars
    for pattern in "org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-*.jar" "org.postgresql_postgresql-*.jar"; do
      compgen -G "/ivy/.ivy2/jars/$pattern" >/dev/null || { echo "THIẾU JAR: $pattern" >&2; exit 1; }
    done'
docker network create --internal "$NETWORK" >/dev/null
docker run -d --pull never --name "$PREFIX-postgres" --network "$NETWORK" --network-alias postgres \
  -e POSTGRES_USER=e2e -e POSTGRES_PASSWORD=e2e -e POSTGRES_DB=e2e postgres:15 >/dev/null
READY=0
for ((attempt=0; attempt<60; attempt++)); do
  if docker exec "$PREFIX-postgres" pg_isready -U e2e -d e2e >/dev/null 2>&1; then READY=1; break; fi
  sleep 1
done
if [[ "$READY" != 1 ]]; then echo 'PostgreSQL is not ready' >&2; exit 1; fi
# Do not change HOME or use a host uid missing from the image's passwd; do not download packages/JARs.
# Existing --source: pass the container path /workspace/... (repo mounted read-only).
docker run --rm --pull never --name "$PREFIX-spark" --network "$NETWORK" \
  -e SPARK_LOCAL_HOSTNAME=localhost -e PYTHONPATH=/workspace \
  -e WAREHOUSE_TEST_DSN=postgresql://e2e:e2e@postgres:5432/e2e \
  -v "$PWD":/workspace:ro -v "$WORK":/proof -v platform_airflow-data:/ivy:ro \
  --entrypoint bash "$IMAGE" -c '
    jars=$(find /ivy/.ivy2/jars -maxdepth 1 \( -name "*iceberg-spark-runtime-3.5_2.12-*.jar" -o -name "*postgresql-*.jar" \) -printf "%p,")
    "$SPARK_HOME/bin/spark-submit" --master "local[*]" --driver-memory "$1" \
      --conf spark.jars.ivy=/tmp/.ivy2 --jars "${jars%,}" \
      /workspace/scripts/run/run_order_lifecycle_local.py "${@:2}"
  ' spark-submit "$DRIVER_MEMORY" --profile "$PROFILE" --receipt /proof/receipt.json "${ARGS[@]}" \
  2>&1 | tee "$WORK/spark.log"
printf 'E2E v2 PASS\n'
