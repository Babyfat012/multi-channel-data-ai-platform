"""Set up the S3 warehouse on Lakekeeper (EC2) using the instance IAM role: no access keys.

An existing warehouse with the same name is verified, never modified. Called by Ansible, which treats the line
"Created the S3 warehouse" as a change.

    python3 -m scripts.data.setup_lakekeeper_s3          (needs LAKEHOUSE_BUCKET, AWS_REGION in .env)
"""

import json
import os
import sys
import urllib.request

from scripts.common import ROOT, env, load_env


def management(path: str, body: dict | None = None) -> dict:
    """Read or send JSON to Lakekeeper on the VM loopback; an HTTP error stops the deployment."""
    url = f"http://127.0.0.1:{os.environ.get('LAKEKEEPER_PORT', '8181')}/management/v1/{path}"
    request = urllib.request.Request(
        url,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        raw = response.read()
    return json.loads(raw) if raw else {}


def main() -> None:
    """Bootstrap if needed, then create or verify the warehouse."""
    os.chdir(ROOT)
    load_env()
    expected = {
        "type": "s3",
        "bucket": env("LAKEHOUSE_BUCKET"),
        "key-prefix": "lakehouse",
        "region": env("AWS_REGION"),
    }
    if not management("info")["bootstrapped"]:
        management("bootstrap", {"accept-terms-of-use": True})
    name = os.environ.get("WAREHOUSE_NAME", "platform-aws")
    existing = next((w for w in management("warehouse")["warehouses"] if w["name"] == name), None)
    if existing:
        actual = management(f"warehouse/{existing['id']}")["storage-profile"]
        if any(actual.get(k) != v for k, v in expected.items()):
            sys.exit(
                "A warehouse with the same name has a different storage profile; refusing to change it."
            )
        print("Warehouse already correctly configured; skipping.")
        return
    # sts-enabled=False: clients use the EC2 IAM role themselves; Lakekeeper does not vend credentials.
    management(
        "warehouse",
        {
            "warehouse-name": name,
            "storage-profile": {**expected, "flavor": "aws", "sts-enabled": False},
            "storage-credential": {"type": "s3", "credential-type": "aws-system-identity"},
        },
    )
    print("Created the S3 warehouse.")


if __name__ == "__main__":
    main()
