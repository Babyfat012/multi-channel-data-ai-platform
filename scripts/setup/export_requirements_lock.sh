#!/usr/bin/env bash
# Export requirements.lock from uv.lock for docker/Dockerfile.naive. Rerun whenever uv.lock changes,
# otherwise the naive image installs a different library set than the optimized image.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"
uv export --frozen --no-dev --no-emit-project --format requirements-txt --output-file requirements.lock
