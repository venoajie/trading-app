# src\services\receiver\Dockerfile

# Stage 1: Base with UV
FROM python:3.13-slim AS base
ENV UV_VENV=/opt/venv
RUN python -m pip install --no-cache-dir uv \
    && python -m uv venv ${UV_VENV}
ENV PATH="${UV_VENV}/bin:$PATH"

# Stage 2: Builder
FROM base AS builder
WORKDIR /build


# Copy application files
COPY src/shared/ src/shared/
COPY src/services/receiver/ src/services/receiver/

# Install Python dependencies
RUN uv pip install \
    --no-cache \
    --strict \
    --resolution=lowest-direct \
    ./src/shared \
    ./src/services/receiver
    
# Stage 3: Runtime
FROM base AS runtime

RUN groupadd -r appuser --gid=1000 && \
    useradd -r -g appuser --uid=1000 appuser && \
    mkdir -p /app/data && \
    chown -R 1000:1000 /app && \
    chmod -R 750 /app

COPY --from=builder --chown=1000:1000 ${UV_VENV} ${UV_VENV}

WORKDIR /app
COPY --chown=1000:1000 ./src ./src
COPY --chown=1000:1000 ./core ./core

ENV DB_BASE_PATH=/app/data \
    PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    PYTHONPYCACHEPREFIX=/tmp/.pycache\
    STRATEGY_CONFIG_PATH=/app/config/shared/strategies.toml


USER 1000
CMD ["python", "src/services/receiver/deribit/main.py"]