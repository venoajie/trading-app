# Stage 1: Base image with build dependencies
FROM python:3.13-slim AS base
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential libssl-dev && \
    rm -rf /var/lib/apt/lists/*

# Stage 2: Dependency installation
FROM base AS builder
WORKDIR /app

# Create virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install build tools
RUN pip install --no-cache-dir --upgrade pip wheel setuptools

# Copy only dependency files first for better caching
COPY src/shared/pyproject.toml /app/src/shared/
COPY src/receiver/pyproject.toml /app/src/receiver/
COPY src/shared/ /app/src/shared
COPY src/receiver/ /app/src/receiver

# Install shared dependencies
RUN pip install --no-cache-dir /app/src/shared

# Install receiver dependencies
RUN pip install --no-cache-dir /app/src/receiver

# Stage 3: Production image
FROM python:3.13-slim AS production
WORKDIR /app

# Copy virtual env from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy application code
COPY src/ /app/src
COPY config/ /app/config

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH="/app/src" \
    DB_BASE_PATH="/app/data"

# Create app user and data directory
RUN useradd -m appuser && \
    mkdir -p /app/data && \
    chown -R appuser:appuser /app

USER appuser

# Healthcheck for Redis dependency
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD python -c "import redis; r = redis.Redis(host='redis', port=6379); r.ping()"

CMD ["python", "-u", "/app/src/receiver/deribit/main.py"]