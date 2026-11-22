"""Set up the Iceberg catalog on Lakekeeper (local MinIO): bootstrap the server once + create the warehouse.

Lakekeeper holds the pointer to the latest metadata of each Iceberg table; metadata and data files stay on MinIO.
Spark cannot create the warehouse itself, so the Management API is called first. Rerunnable: skips what exists.
Requires `docker compose --profile catalog up -d` (lakekeeper + postgres + minio running). AWS: setup_lakekeeper_s3.

    python3 -m scripts.data.setup_lakekeeper
"""

import json
import os
import urllib.request

from scripts.common import ROOT, env, load_env


def management(path: str, body: dict | None = None) -> dict:
    """GET (or POST when body is given) on Lakekeeper's Management API; HTTP errors stop the script."""
    url = f"http://localhost:{os.environ.get('LAKEKEEPER_PORT', '8181')}/management/v1/{path}"
    request = urllib.request.Request(
        url,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        raw = response.read()
    return json.loads(raw) if raw else {}


def warehouse_body(name: str, bucket: str, endpoint: str, key_id: str, secret: str) -> dict:
    """MinIO warehouse: s3-compat flavor, no STS (Spark brings its own keys), tables under s3://<bucket>/lakehouse/."""
    return {
        "warehouse-name": name,
        "storage-profile": {
            "type": "s3",
            "bucket": bucket,
            "key-prefix": "lakehouse",  # keeps Iceberg tables apart from the generator's source-exports/
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


def main() -> None:
    """Bootstrap, create the warehouse when missing, then list warehouses."""
    os.chdir(ROOT)
    load_env()
    name = os.environ.get("WAREHOUSE_NAME", "platform")
    # MinIO as seen from INSIDE the Compose network: Lakekeeper and Spark are both containers.
    endpoint = os.environ.get("MINIO_INTERNAL_ENDPOINT", "http://minio:9000")
    if management("info")["bootstrapped"]:
        print("==> Already bootstrapped, skipping.")
    else:
        management("bootstrap", {"accept-terms-of-use": True})
        print("==> Bootstrap done.")
    if any(w["name"] == name for w in management("warehouse")["warehouses"]):
        print(f"==> Warehouse '{name}' already exists, skipping.")
    else:
        bucket = env("MINIO_BUCKET")
        print(f"==> Creating warehouse '{name}' pointing at MinIO bucket '{bucket}'...")
        management(
            "warehouse",
            warehouse_body(
                name, bucket, endpoint, env("MINIO_ROOT_USER"), env("MINIO_ROOT_PASSWORD")
            ),
        )
        print("==> Warehouse created.")
    print("==> Existing warehouses:")
    print(json.dumps(management("warehouse"), indent=2))


if __name__ == "__main__":
    main()
