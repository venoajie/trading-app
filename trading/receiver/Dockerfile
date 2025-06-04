# Stage 1: UV base
FROM python:3.11-slim AS base
ENV UV_VENV=/opt/venv
RUN python -m pip install --no-cache-dir uv && \
    python -m uv venv $UV_VENV
ENV PATH="$UV_VENV/bin:$PATH"

# Stage 2: Dependency installation
FROM base AS builder
WORKDIR /build

# Copy dependency specs
COPY src/shared/pyproject.toml ./src/shared/
COPY src/receiver/pyproject.toml ./src/receiver/

# Install with UV
RUN uv pip install \
    --no-cache \
    --strict \
    -e ./src/shared \
    -e ./src/receiver

# Stage 3: Optimized runtime
FROM base AS runtime

# Security hardening
RUN groupadd -r appuser --gid=1000 && \
    useradd -r -g appuser --uid=1000 appuser && \
    mkdir -p /app/data && \
    chown -R 1000:1000 /app && \
    chmod -R 750 /app

# Copy artifacts
COPY --from=builder $UV_VENV $UV_VENV
COPY --chown=1000:1000 . /app
WORKDIR /app

# Final settings
USER 1000
ENV DB_BASE_PATH=/app/data \
    PYTHONPATH=/app/src:$PYTHONPATH \
    PYTHONUNBUFFERED=1 \
    PYTHONPYCACHEPREFIX=/tmp/.pycache

HEALTHCHECK --interval=30s --timeout=3s \
  CMD curl -f http://localhost:8000/health || exit 1

CMD ["python", "src/receiver/deribit/main.py"]