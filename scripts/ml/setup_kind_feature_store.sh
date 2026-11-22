#!/usr/bin/env bash
# Create the kind cluster `platform` + Redis (Feast online store), then attach the node to the Compose network
# so Airflow can reach platform-control-plane:30379. Safe to rerun many times.
set -euo pipefail
cd "$(dirname "$0")/../.."

kind get clusters | grep -qx platform || kind create cluster --config infra/k8s/kind-config.yaml
kubectl --context kind-platform apply -f infra/k8s/feature-store/redis.yaml
kubectl --context kind-platform -n feature-store rollout status deploy/redis --timeout=180s
docker network connect platform_default platform-control-plane 2>/dev/null || true
printf 'Redis ready: platform-control-plane:30379 (from the platform_default network)\n'
