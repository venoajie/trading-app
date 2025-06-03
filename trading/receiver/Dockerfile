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
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Copy virtual environment
COPY --from=builder ${UV_VENV} ${UV_VENV}

# Copy only necessary application files
WORKDIR /app

COPY trading/shared/src/shared /app/shared
COPY trading/receiver/src/receiver /app/receiver
COPY trading/scripts/src/scripts /app/scripts
COPY config config

# Set up data directory with proper permissions
RUN mkdir -p /app/data && chown appuser:appuser /app/data && chmod 775 /app/data

# Environment variables
ENV DB_BASE_PATH=/app/data \
    PYTHONPATH=/app:$PYTHONPATH \
    PYTHONUNBUFFERED=1 \
    PYTHONPYCACHEPREFIX=/tmp/.pycache

# Switch to non-root user
USER appuser

# Health check
HEALTHCHECK --interval=30s --timeout=3s \
  CMD python -c "import socket; socket.create_connection(('localhost', 8000), 3)" || exit 1

# Run receiver service
CMD ["python", "/app/receiver/deribit/main.py"]