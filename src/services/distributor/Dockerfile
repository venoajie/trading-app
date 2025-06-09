# src\services\distributor\Dockerfile

# Stage 1: Base with UV
FROM python:3.13-slim AS base
ENV UV_VENV=/opt/venv
RUN python -m pip install --no-cache-dir uv && \
    python -m uv venv ${UV_VENV}
ENV PATH="${UV_VENV}/bin:$PATH"

# Stage 2: Builder for dependencies
FROM base AS builder
WORKDIR /build

# Copy ONLY dependency files
COPY src/shared/pyproject.toml src/shared/
COPY src/shared/src/ src/shared/src/
COPY src/services/distributor/pyproject.toml src/services/distributor/
COPY src/services/distributor/src/ src/services/distributor/src/

# Install dependencies
RUN uv pip install \
    --no-cache \
    --strict \
    --resolution=lowest-direct \
    ./src/shared \
    ./src/services/distributor

# Stage 3: Runtime
FROM base AS runtime

# Create non-root user
RUN groupadd -r appuser --gid=1000 && \
    useradd -r -g appuser --uid=1000 appuser && \
    mkdir -p /app/data && \
    chown -R 1000:1000 /app && \
    chmod -R 750 /app

# Copy virtual environment
COPY --from=builder --chown=1000:1000 ${UV_VENV} ${UV_VENV}

# Copy application files using actual project structure
WORKDIR /app
COPY --chown=1000:1000 ./src ./src
COPY --chown=1000:1000 ./config ./config
COPY --chown=1000:1000 ./core ./core

# Environment variables
ENV DB_BASE_PATH=/app/data \
    PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    PYTHONPYCACHEPREFIX=/tmp/.pycache

# Switch to non-root user
USER 1000

# Run  service
CMD ["python", "src/services/distributor/deribit/main.py"]