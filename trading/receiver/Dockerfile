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
COPY trading/shared/pyproject.toml ./trading/shared/
COPY trading/receiver/pyproject.toml ./trading/receiver/

# Install with UV
RUN uv pip install \
    --no-cache \
    --strict \
    -e ./trading/shared \
    -e ./trading/receiver

# Stage 3: Optimized runtime
FROM base AS runtime

# Security hardening
RUN groupadd -r appuser && \
    useradd -r -g appuser appuser && \
    mkdir -p /app/data && \
    chown appuser:appuser /app/data && \
    chmod 750 /app/data

# Copy artifacts
COPY --from=builder $UV_VENV $UV_VENV
COPY --chown=appuser:appuser . /app
WORKDIR /app

# Final settings
USER appuser
ENV DB_BASE_PATH=/app/data \
    PYTHONUNBUFFERED=1 \
    PYTHONPYCACHEPREFIX=/tmp/.pycache

HEALTHCHECK --interval=30s --timeout=3s \
  CMD python -c "import socket; s=socket.socket(); s.connect(('localhost',8000))" || exit 1

CMD ["python", "trading/receiver/src/receiver/deribit/main.py"]