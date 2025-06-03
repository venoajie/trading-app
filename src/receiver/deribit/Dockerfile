# Stage 1: Base image with Python 3.13
FROM python:3.13-slim AS base

# Install minimal runtime dependencies only
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libsqlite3-0 \
    libssl3 \
    # Security hardening
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install UV and create virtual environment
RUN pip install --no-cache-dir uv==0.1.0 && \
    uv venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH"

# Stage 2: Build dependencies
FROM base AS builder

# Install only essential build tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential pkg-config && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy dependency files only
COPY ./src/shared/requirements.txt /tmp/
COPY ./src/receiver/pyproject.toml /tmp/

# Install dependencies with UV (single layer)
RUN uv pip install --no-cache -r /tmp/requirements.txt

# Stage 3: Application builder
FROM base AS app-builder

WORKDIR /app

# Copy application code
COPY ./src /app/src
COPY ./config /app/config

# Install application without editable mode
RUN uv pip install --no-cache /app/src/receiver

# Stage 4: Final production image
FROM base AS production

# Create non-root user
RUN addgroup --system --gid 1001 appgroup && \
    adduser --system --uid 1001 --ingroup appgroup appuser

# Create data directory with correct permissions
RUN mkdir -p /app/data && \
    chown -R 1001:1001 /app/data && \
    chmod 2775 /app/data

# Copy application from builder
COPY --from=app-builder --chown=1001:1001 /app /app
COPY --from=app-builder --chown=1001:1001 /opt/venv /opt/venv

# Environment configuration
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH="/app/src" \
    CONFIG_PATH="/app/config/strategies.toml" \
    DB_BASE_PATH="/app/data" \
    # Security hardening
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Switch to non-root user
USER 1001

# Health check (simplified)
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import os; exit(0) if os.access('/app/data', os.W_OK) else exit(1)"

# Execute application
CMD ["python", "-u", "/app/src/receiver/deribit/main.py"]