# Stage 1: Base with UV - common for all stages
FROM python:3.11-slim AS base
ENV UV_VENV=/opt/venv
RUN python -m pip install --no-cache-dir uv \
    && python -m uv venv ${UV_VENV}
ENV PATH="${UV_VENV}/bin:$PATH"

# Stage 2: Builder for dependencies
FROM base AS builder
WORKDIR /build

# Copy only dependency files for cache optimization
COPY trading/shared/pyproject.toml trading/shared/
COPY trading/receiver/pyproject.toml trading/receiver/

# Install dependencies with UV
RUN uv pip install \
    --no-cache \
    --strict \
    --resolution=lowest-direct \
    -e ./trading/shared \
    -e ./trading/receiver

# Stage 3: Runtime image
FROM base AS runtime

# Create non-root user
RUN groupadd -r appuser --gid=1000 && \
    useradd -r -g appuser --uid=1000 appuser && \
    mkdir -p /app/data && \
    chown -R 1000:1000 /app && \
    chmod -R 750 /app

# Copy virtual environment
COPY --from=builder --chown=1000:1000 ${UV_VENV} ${UV_VENV}

# Copy application files
WORKDIR /app
COPY --chown=1000:1000 trading/shared/src/shared ./trading/shared/src/shared
COPY --chown=1000:1000 trading/receiver/src/receiver ./trading/receiver/src/receiver
COPY --chown=1000:1000 trading/scripts/src/scripts ./trading/scripts/src/scripts
COPY --chown=1000:1000 config ./config

# Environment variables
ENV DB_BASE_PATH=/app/data \
    PYTHONPATH=/app/trading/shared/src:/app/trading/receiver/src:/app/trading/scripts/src:$PYTHONPATH \
    PYTHONUNBUFFERED=1 \
    PYTHONPYCACHEPREFIX=/tmp/.pycache

# Switch to non-root user
USER 1000

# Health check
HEALTHCHECK --interval=30s --timeout=3s \
  CMD python -c "import socket; s=socket.socket(); s.connect(('localhost',8000))" || exit 1

# Run receiver service
CMD ["python", "trading/receiver/src/receiver/deribit/main.py"]