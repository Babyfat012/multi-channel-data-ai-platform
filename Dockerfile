# Optimized generator image: multi-stage, slim base, keeps only the installed virtualenv.
# For the size comparison with docker/Dockerfile.naive see docs/Docker.md.

# Stage 1: install the libraries and the generator into /app/.venv with uv.
FROM python:3.12-slim AS build
COPY --from=ghcr.io/astral-sh/uv:0.8 /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
WORKDIR /app
# Install libraries first, separate from the code: code edits do not invalidate this layer's cache.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project
COPY generator ./generator
# --no-editable: install the generator as a package in .venv, so the next stage needs no source code.
# --reinstall-package: uv's cache only looks at pyproject.toml, not the code. Without this flag
# a code change without a pyproject.toml change leaves the image with the old cached generator.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable --reinstall-package platform-generator

# Stage 2: only slim Python, the virtualenv and config files. No uv, cache or compiler.
FROM python:3.12-slim
# GitHub uses this label to link the package (ghcr.io) to the repo.
LABEL org.opencontainers.image.source="https://github.com/Babyfat012/multi-channel-data-ai-platform" \
      org.opencontainers.image.description="Simulated five-channel data generator (skew, schema evolution, duplicates, late arrivals)" \
      org.opencontainers.image.licenses="Apache-2.0"
WORKDIR /app
COPY --from=build /app/.venv /app/.venv
COPY configs ./configs
ENV PATH=/app/.venv/bin:$PATH PYTHONUNBUFFERED=1
USER nobody
ENTRYPOINT ["python", "-m", "generator"]
