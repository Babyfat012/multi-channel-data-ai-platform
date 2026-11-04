#!/usr/bin/env bash
# Chứng minh Helm rolling update + tự rollback (--atomic) của Feature API trên cluster thật, có tải liên tục.
# Chạy: KCTX=<kube-context> scripts/evidence/helm_rollout_fallback_test.sh
# 1) rolling update tốt  2) bản hỏng (envSecret không tồn tại) ⇒ helm tự rollback  3) helm history + đếm mã HTTP của probe.
# Probe chạy TRONG cluster (không port-forward: forward gắn một pod nên rớt khi pod đó bị thay).
# Ghi docs/evidence/aws/feature-api-helm-rollout.txt. Release đã deploy bằng scripts/setup/deploy_feature_api.sh.
set -euo pipefail
cd "$(dirname "$0")/../.."
ns=${FEATURE_API_NAMESPACE:-feature-store}
release=feature-api
out=${OUT:-docs/evidence/aws/feature-api-helm-rollout.txt}
K=(kubectl --context "${KCTX:?đặt KCTX=<kube-context>}" -n "$ns")
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
  echo "# helm history trước:"
  "${H[@]}" history "$release" --max 3
  echo "# 1) Rolling update tốt (đổi envChecksum ⇒ pod template đổi), --atomic --timeout 4m"
  "${H[@]}" upgrade "$release" infra/helm/feature-api --reuse-values \
    --set-string envChecksum="rollout-$(date -u +%s)" --atomic --timeout 4m
  echo "# 2) Bản hỏng: envSecret=does-not-exist ⇒ pod mới không khởi động, --atomic --timeout 90s (mong đợi FAILED rồi rollback)"
  if "${H[@]}" upgrade "$release" infra/helm/feature-api --reuse-values \
      --set envSecret=does-not-exist --atomic --timeout 90s; then
    echo "# CẢNH BÁO: bản hỏng lại thành công, chưa chứng minh được fallback"
  else
    echo "# upgrade thất bại như mong đợi; helm đã tự rollback"
  fi
  echo "# 3) helm history sau:"
  "${H[@]}" history "$release" --max 6
  "${K[@]}" get pods -l "app=$release"
  sleep 3
  echo "# Mã HTTP của probe trong suốt hai lần upgrade (số request, mã):"
  "${K[@]}" logs "$probe" | sort | uniq -c
} 2>&1 | tee "$out"
