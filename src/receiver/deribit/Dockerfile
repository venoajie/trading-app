# Stage 1: Base image
FROM python:3.13-slim AS base
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libsqlite3-0 libssl3 && \
    rm -rf /var/lib/apt/lists/*

# Stage 2: Builder
FROM base AS builder
RUN pip install --no-cache-dir uv==0.1.0 && \
    uv venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Stage 3: Dependency installation
FROM builder AS deps
WORKDIR /app
COPY src /app/src
COPY shared /app/shared
RUN uv pip install --no-cache-dir /app/src/shared

# Stage 4: Application build
FROM deps AS app-builder
COPY src /app/src
COPY config /app/config
RUN uv pip install --no-cache-dir /app/src/receiver

# Stage 5: Production image
FROM base AS production
COPY --from=app-builder /opt/venv /opt/venv
COPY --from=app-builder /app /app
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH="/app/src" \
    DB_BASE_PATH="/app/data"

# Create app user and data directory
RUN adduser --disabled-password --gecos "" appuser && \
    mkdir -p /app/data && \
    chown appuser:appuser /app/data

USER appuser
CMD ["python", "-u", "/app/src/receiver/deribit/main.py"]