#!/usr/bin/env bash
# Initialize the S3 warehouse using the EC2 IAM role; no access keys, existing warehouses are not modified.
set -euo pipefail
cd "$(dirname "$0")/../.."
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"
set -a
source .env
set +a
: "${LAKEHOUSE_BUCKET:?LAKEHOUSE_BUCKET is required}"
: "${AWS_REGION:?AWS_REGION is required}"
python3 - <<'PY'
# Call the local Management API; an HTTP error must stop the deployment.
import json
import os
import urllib.request


def request(path, body=None):
    """Read or send JSON to Lakekeeper on the VM loopback."""
    url = 'http://127.0.0.1:' + os.environ.get('LAKEKEEPER_PORT', '8181')
    req = urllib.request.Request(url + '/management/v1/' + path,
        data=None if body is None else json.dumps(body).encode(),
        headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=120) as response:
        raw = response.read()
        return json.loads(raw) if raw else {}


if not request('info')['bootstrapped']:
    request('bootstrap', {'accept-terms-of-use': True})
name = os.environ.get('WAREHOUSE_NAME', 'platform-aws')
expected = {'type': 's3', 'bucket': os.environ['LAKEHOUSE_BUCKET'], 'key-prefix': 'lakehouse',
            'region': os.environ['AWS_REGION']}
warehouses = request('warehouse')['warehouses']
existing = next((w for w in warehouses if w['name'] == name), None)
if existing:
    actual = request('warehouse/' + existing['id'])['storage-profile']
    if any(actual.get(k) != v for k, v in expected.items()):
        raise SystemExit('A warehouse with the same name has a different storage profile; refusing to change it.')
    print('Warehouse already correctly configured; skipping.')
else:
    # sts-enabled=False: clients use the EC2 IAM role themselves; Lakekeeper does not vend credentials.
    request('warehouse', {'warehouse-name': name,
        'storage-profile': {**expected, 'flavor': 'aws', 'sts-enabled': False},
        'storage-credential': {'type': 's3', 'credential-type': 'aws-system-identity'}})
    print('Created the S3 warehouse.')
PY
