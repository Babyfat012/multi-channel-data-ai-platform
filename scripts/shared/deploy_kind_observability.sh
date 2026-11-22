#!/usr/bin/env bash
# Deploy the observability stack in kind platform (ns observability): Prometheus (metrics), Loki + Fluent Bit (logs),
# Jaeger (traces), Grafana (views all three, with the PLATFORM dashboard). Safe to rerun many times.
set -euo pipefail
cd "$(dirname "$0")/../.."
KC="kubectl --context ${KCTX:-kind-platform} -n observability"
H="helm --kube-context "${KCTX:-kind-platform}" -n observability"
V=infra/k8s/observability

helm repo add prometheus-community https://prometheus-community.github.io/helm-charts >/dev/null
helm repo add grafana https://grafana.github.io/helm-charts >/dev/null
helm repo add fluent https://fluent.github.io/helm-charts >/dev/null
helm repo add jaegertracing https://jaegertracing.github.io/helm-charts >/dev/null
helm repo update >/dev/null

kubectl --context ${KCTX:-kind-platform} create namespace observability --dry-run=client -o yaml | kubectl --context ${KCTX:-kind-platform} apply -f -
# The Grafana admin password is generated once and kept in a Secret (read: kubectl -n observability get secret grafana-admin).
if ! $KC get secret grafana-admin >/dev/null 2>&1; then
  $KC create secret generic grafana-admin --from-literal=admin-user=admin \
    --from-literal=admin-password="$(openssl rand -hex 12)"
fi
$KC create configmap grafana-dashboards-platform --from-file=$V/dashboards --dry-run=client -o yaml | $KC apply -f -

# EKS: add the EC2 exporter scrape + alert rules (kind has no EC2, so no false alarms).
prom_aws=(); [[ "${KCTX:-kind-platform}" == kind-* ]] || prom_aws=(-f $V/prometheus-values-aws.yaml)
$H upgrade --install prometheus prometheus-community/prometheus --version 29.36.2 -f $V/prometheus-values.yaml "${prom_aws[@]}" --wait
$H upgrade --install loki grafana/loki --version 7.3.0 -f $V/loki-values.yaml --wait --timeout 10m
$H upgrade --install fluent-bit fluent/fluent-bit --version 0.58.3 -f $V/fluent-bit-values.yaml --wait
$H upgrade --install jaeger jaegertracing/jaeger --version 4.16.1 -f $V/jaeger-values.yaml --wait
$H upgrade --install grafana grafana/grafana --version 10.5.15 -f $V/grafana-values.yaml --wait
$KC get pods
