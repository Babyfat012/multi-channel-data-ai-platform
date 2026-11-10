#!/usr/bin/env bash
# Set up the Iceberg catalog on Lakekeeper: bootstrap the server (once) + create a warehouse pointing at MinIO.
#
# Why: Lakekeeper is the "ledger" holding the pointer to the latest metadata of each Iceberg table.
# Spark cannot create the warehouse itself — Lakekeeper's Management API must be called first.
# Metadata files and the actual data files still live on MinIO; Lakekeeper only keeps the pointer.
#
# Rerunnable (idempotent): skips if already bootstrapped, skips if a warehouse with the same name exists.
#
# Usage:  bash scripts/setup/setup_lakekeeper.sh
# Requires: docker compose --profile catalog up -d  (lakekeeper + postgres + minio running)
set -euo pipefail

cd "$(dirname "$0")/../.."
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"
# shellcheck disable=SC1091
set -a; source .env; set +a

LAKEKEEPER_URL="http://localhost:${LAKEKEEPER_PORT:-8181}"
WAREHOUSE_NAME="${WAREHOUSE_NAME:-platform}"
# MinIO endpoint as seen from INSIDE the Compose network: Lakekeeper and Spark are both containers.
MINIO_ENDPOINT="${MINIO_INTERNAL_ENDPOINT:-http://minio:9000}"

say() { printf '==> %s\n' "$1"; }

# --- 1. Bootstrap --------------------------------------------------------------
# A fresh server has no default project; every create-warehouse call fails until bootstrapped.
if [ "$(curl -fsS "$LAKEKEEPER_URL/management/v1/info" | python3 -c 'import sys,json;print(json.load(sys.stdin)["bootstrapped"])')" = "True" ]; then
  say "Already bootstrapped, skipping."
else
  say "Bootstrap Lakekeeper..."
  curl -fsS -X POST "$LAKEKEEPER_URL/management/v1/bootstrap" \
    -H 'Content-Type: application/json' \
    -d '{"accept-terms-of-use": true}'
  say "Bootstrap done."
fi

# --- 2. Warehouse --------------------------------------------------------------
if curl -fsS "$LAKEKEEPER_URL/management/v1/warehouse" \
     | python3 -c "import sys,json;ws=json.load(sys.stdin)['warehouses'];sys.exit(0 if any(w['name']=='$WAREHOUSE_NAME' for w in ws) else 1)"; then
  say "Warehouse '$WAREHOUSE_NAME' already exists, skipping."
else
  say "Creating warehouse '$WAREHOUSE_NAME' pointing at MinIO bucket '$MINIO_BUCKET'..."
  # flavor=s3-compat: required for MinIO (MinIO is not real AWS S3).
  # sts-enabled=false: MinIO here does not run STS, so vended credentials are not used.
  #   Spark brings its own access key (configured in the job) instead of asking Lakekeeper.
  # key-prefix: all Iceberg tables live under s3://$MINIO_BUCKET/lakehouse/, not mixed
  #   with the generator's source-exports/ in the same bucket.
  python3 - "$LAKEKEEPER_URL" "$WAREHOUSE_NAME" "$MINIO_BUCKET" "$MINIO_ENDPOINT" \
           "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" <<'PY'
import json, sys, urllib.request

url, name, bucket, endpoint, key_id, secret = sys.argv[1:7]
body = {
    "warehouse-name": name,
    "storage-profile": {
        "type": "s3",
        "bucket": bucket,
        "key-prefix": "lakehouse",
        "region": "local-01",
        "endpoint": endpoint,
        "flavor": "s3-compat",
        "sts-enabled": False,
        "path-style-access": True,
    },
    "storage-credential": {
        "type": "s3",
        "credential-type": "access-key",
        "aws-access-key-id": key_id,
        "aws-secret-access-key": secret,
    },
}
req = urllib.request.Request(
    f"{url}/management/v1/warehouse",
    data=json.dumps(body).encode(),
    headers={"Content-Type": "application/json"},
    method="POST",
)
try:
    with urllib.request.urlopen(req) as r:
        print(r.read().decode())
except urllib.error.HTTPError as e:
    print(f"LỖI HTTP {e.code}: {e.read().decode()}", file=sys.stderr)
    raise SystemExit(1)
PY
  say "Warehouse created."
fi

say "Existing warehouses:"
curl -fsS "$LAKEKEEPER_URL/management/v1/warehouse" | python3 -m json.tool
