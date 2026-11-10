#!/usr/bin/env bash
# Local-compatible entry point; the shared script picks the config from PLATFORM_STORAGE/KCTX/REGISTRY.
set -euo pipefail
exec bash "$(dirname "$0")/deploy_feature_api.sh" "$@"
