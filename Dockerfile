# Image tối ưu của generator: multi-stage, base slim, chỉ giữ virtualenv đã cài.
# So sánh dung lượng với docker/Dockerfile.naive xem docs/Docker.md.

# Stage 1: cài thư viện và generator vào /app/.venv bằng uv.
FROM python:3.12-slim AS build
COPY --from=ghcr.io/astral-sh/uv:0.8 /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
WORKDIR /app
# Cài thư viện trước, tách khỏi code: sửa code không làm mất cache của lớp này.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project
COPY generator ./generator
# --no-editable: cài generator thành package trong .venv, nên stage sau không cần mã nguồn.
# --reinstall-package: cache của uv chỉ nhìn pyproject.toml, không nhìn code. Thiếu cờ này
# thì sửa code mà không đổi pyproject.toml, image vẫn cài bản generator cũ từ cache.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable --reinstall-package platform-generator

# Stage 2: chỉ Python slim, virtualenv và file cấu hình. Không có uv, cache hay trình biên dịch.
FROM python:3.12-slim
# GitHub dựa vào nhãn này để gắn package (ghcr.io) vào repo.
LABEL org.opencontainers.image.source="https://github.com/Babyfat012/multichannel-data-ai-platform" \
      org.opencontainers.image.description="Generator dữ liệu giả lập năm kênh (skew, schema evolution, duplicate, late arrival)" \
      org.opencontainers.image.licenses="Apache-2.0"
WORKDIR /app
COPY --from=build /app/.venv /app/.venv
COPY configs ./configs
ENV PATH=/app/.venv/bin:$PATH PYTHONUNBUFFERED=1
USER nobody
ENTRYPOINT ["python", "-m", "generator"]
