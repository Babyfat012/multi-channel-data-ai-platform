#!/usr/bin/env bash
# Shared by kind/ECR/Artifact Registry (log in to external registries first).
# REGISTRY=localhost:5001 TAG=<sha> scripts/run/push_images.sh platform-feature-api:<source-tag>
# With TAG: release the source image under that TAG; without TAG: keep the immutable source tag.
set -euo pipefail
REGISTRY=${REGISTRY:-localhost:5001}
[[ $# -gt 0 ]] || { echo 'At least one image:tag is required' >&2; exit 1; }
# ECR: log in with the available credentials (EC2 IAM role for Jenkins, SSO profile on the laptop).
if [[ "$REGISTRY" =~ ^([0-9]+\.dkr\.ecr\.([a-z0-9-]+)\.amazonaws\.com) ]]; then
  aws ecr get-login-password --region "${BASH_REMATCH[2]}" \
    | docker login --username AWS --password-stdin "${BASH_REMATCH[1]}" >/dev/null
fi
for image in "$@"; do
  source_tag=${image##*:}
  [[ "$source_tag" != "$image" && "$source_tag" != */* ]] || { echo "Missing tag: $image" >&2; exit 1; }
  tag=${TAG:-$source_tag}
  [[ "$tag" != latest ]] || { echo 'Refusing to release the latest tag' >&2; exit 1; }
  name=${image%:*}
  name=${name##*/}
  target="${REGISTRY%/}/$name:$tag"
  docker image inspect "$image" >/dev/null
  docker tag "$image" "$target"
  docker push "$target"
done
