#!/usr/bin/env bash
# Generate a Compose override: deployed services pull exactly the TAG released in the Push stage.
set -euo pipefail
: "${TAG:?set an immutable TAG}"
[[ "$TAG" != latest ]] || exit 1
REGISTRY=${REGISTRY:-localhost:5001}
printf 'services:\n'
for mapping in "$@"; do
  service=${mapping%%=*}
  image=${mapping#*=}
  [[ "$service" =~ ^[a-z0-9-]+$ && "$image" =~ ^[a-z0-9-]+$ ]] || exit 1
  printf '  %s:\n    image: "%s/%s:%s"\n' "$service" "${REGISTRY%/}" "$image" "$TAG"
done
