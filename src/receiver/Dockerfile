# Stage 1: Base with UV - common for all stages
FROM python:3.13-slim AS base
ENV UV_VENV=/opt/venv
RUN python -m pip install --no-cache-dir uv \
    && python -m uv venv ${UV_VENV}
ENV PATH="${UV_VENV}/bin:$PATH"

# Stage 2: Builder for dependencies
FROM base AS builder
WORKDIR /build

# Copy dependency files using actual project structure
COPY src/shared/pyproject.toml src/shared/
COPY src/receiver/pyproject.toml src/receiver/

# Install dependencies with UV
RUN uv pip install \
    --no-cache \
    --strict \
    --resolution=lowest-direct \
    -e ./src/shared \
    -e ./src/receiver

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

# Copy application files using actual project structure
WORKDIR /app
COPY --chown=1000:1000 ./src ./src
COPY --chown=1000:1000 ./config ./config

# Environment variables
ENV DB_BASE_PATH=/app/data \
    PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    PYTHONPYCACHEPREFIX=/tmp/.pycache

# Switch to non-root user
USER 1000

# Run receiver service
CMD ["python", "src/receiver/deribit/main.py"]