#!/usr/bin/env bash
# docker compose for Jenkins: local = base file; AWS (DATA_STORAGE=s3, Jenkins on EC2) adds the AWS overlay (+ stream when
# CI_STREAM=1) so running services are NOT recreated with the local config. If out/ci-compose-images.yaml exists, released images are pulled.
set -euo pipefail
files=(-f docker-compose.yml)
if [[ "${DATA_STORAGE:-minio}" == s3 ]]; then
  files+=(-f docker-compose.aws.yml)
  [[ "${CI_STREAM:-0}" != 1 ]] || files+=(-f docker-compose.aws-stream.yml)
fi
[[ ! -f out/ci-compose-images.yaml ]] || files+=(-f out/ci-compose-images.yaml)
exec docker compose "${files[@]}" "$@"
