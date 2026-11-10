#!/usr/bin/env bash
# Build the naive and optimized generator images from the same working tree, compare the libraries installed in both,
# print size + digest. Exits nonzero if the library sets differ (the "only the build differs" claim is false).
# Usage: bash scripts/evidence/compare_docker_dependencies.sh [output directory]
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"
out=${1:-out/docker-compare}
mkdir -p "$out"
docker build -q -t platform-generator:naive -f docker/Dockerfile.naive . >/dev/null
docker build -q -t platform-generator:opt -f Dockerfile . >/dev/null
# The optimized image has no pip/uv in the final stage; read dist-info metadata with importlib.metadata of
# the venv's own Python. Skip the project and installer tools (pip/setuptools/wheel) since they belong to the base image.
freeze='import importlib.metadata as m
skip = {"platform-generator", "pip", "setuptools", "wheel"}
names = sorted({(d.metadata["Name"].lower().replace("_", "-"), d.version) for d in m.distributions()})
print("\n".join(f"{n}=={v}" for n, v in names if n not in skip))'
docker run --rm --entrypoint python platform-generator:naive -c "$freeze" >"$out/naive.txt"
docker run --rm --entrypoint python platform-generator:opt -c "$freeze" >"$out/opt.txt"
for tag in naive opt; do
  docker image inspect "platform-generator:$tag" --format "$tag {{.Id}} {{.Size}}"
done | tee "$out/images.txt"
if diff -u "$out/naive.txt" "$out/opt.txt" >"$out/deps.diff"; then
  echo "Same library set: $(wc -l <"$out/opt.txt") packages"
else
  echo "DIFFERENT library sets — see $out/deps.diff" >&2
  exit 1
fi
