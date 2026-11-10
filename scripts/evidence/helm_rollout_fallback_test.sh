#!/usr/bin/env bash
# Prove Helm rolling update + automatic rollback (--atomic) of the Feature API on a real cluster, under continuous load.
# Run: KCTX=<kube-context> scripts/evidence/helm_rollout_fallback_test.sh
# 1) good rolling update  2) broken release (envSecret does not exist) ⇒ helm rolls back automatically  3) helm history + probe HTTP code counts.
# The probe runs INSIDE the cluster (no port-forward: a forward is bound to one pod, so it drops when that pod is replaced).
# Writes docs/evidence/aws/feature-api-helm-rollout.txt. The release was deployed with scripts/setup/deploy_feature_api.sh.
set -euo pipefail
cd "$(dirname "$0")/../.."
ns=${FEATURE_API_NAMESPACE:-feature-store}
release=feature-api
out=${OUT:-docs/evidence/aws/feature-api-helm-rollout.txt}
K=(kubectl --context "${KCTX:?set KCTX=<kube-context>}" -n "$ns")
H=(helm --kube-context "$KCTX" -n "$ns")
probe=rollout-probe
cleanup() { "${K[@]}" delete pod "$probe" --ignore-not-found --wait=false >/dev/null 2>&1 || true; }
trap cleanup EXIT

"${K[@]}" delete pod "$probe" --ignore-not-found >/dev/null
"${K[@]}" run "$probe" --restart=Never --image=curlimages/curl:8.11.1 --command -- sh -c \
  'while true; do curl -s -o /dev/null -m 3 -w "%{http_code}\n" -X POST -H "Content-Type: application/json" -d "{\"seller_skus\":[\"APP-00042\"]}" http://feature-api:8000/features; sleep 0.2; done'
"${K[@]}" wait --for=condition=Ready "pod/$probe" --timeout=120s >/dev/null
sleep 5

{
  echo "# helm rolling update + auto rollback $release ($KCTX, $(date -u +%Y-%m-%dT%H:%M:%SZ))"
  echo "# helm history before:"
  "${H[@]}" history "$release" --max 3
  echo "# 1) Good rolling update (change envChecksum ⇒ pod template changes), --atomic --timeout 4m"
  "${H[@]}" upgrade "$release" infra/helm/feature-api --reuse-values \
    --set-string envChecksum="rollout-$(date -u +%s)" --atomic --timeout 4m
  echo "# 2) Broken release: envSecret=does-not-exist ⇒ new pod does not start, --atomic --timeout 90s (expect FAILED then rollback)"
  if "${H[@]}" upgrade "$release" infra/helm/feature-api --reuse-values \
      --set envSecret=does-not-exist --atomic --timeout 90s; then
    echo "# WARNING: the broken release succeeded, fallback not proven"
  else
    echo "# upgrade failed as expected; helm rolled back automatically"
  fi
  echo "# 3) helm history after:"
  "${H[@]}" history "$release" --max 6
  "${K[@]}" get pods -l "app=$release"
  sleep 3
  echo "# Probe HTTP codes across both upgrades (request count, code):"
  "${K[@]}" logs "$probe" | sort | uniq -c
} 2>&1 | tee "$out"
