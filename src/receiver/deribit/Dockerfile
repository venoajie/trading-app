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

# Install build dependencies (separate from runtime)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libssl-dev \
    pkg-config \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy dependency specifications first for better caching
COPY ./src/shared/requirements.txt /app/src/shared/
COPY ./src/receiver/pyproject.toml /app/src/receiver/

# Install base dependencies with UV
RUN . /opt/venv/bin/activate && \
    uv pip install --no-cache \
    -r /app/src/shared/requirements.txt

# Stage 3: Application builder
FROM builder AS app-builder

# Copy application code (exclude unnecessary files)
COPY ./src /app/src
COPY ./config /app/config

# Install application in editable mode
RUN . /opt/venv/bin/activate && \
    uv pip install --no-cache -e /app/src/receiver

# Stage 4: Final production image
FROM base AS production

# Create non-root user with explicit UID/GID
RUN addgroup --system --gid 1001 appgroup && \
    adduser --system --uid 1001 --ingroup appgroup appuser

# Create data and config directories with correct permissions
RUN mkdir -p /app/data/databases /app/config && \
    chown -R 1001:1001 /app/data /app/config && \
    chmod -R 755 /app/data /app/config

# Copy application with proper ownership
COPY --from=app-builder --chown=1001:1001 /app /app
COPY --from=app-builder --chown=1001:1001 /opt/venv /opt/venv

# Environment configuration
ENV PYTHONUNBUFFERED=1 \
    PYTHONPYCACHEPREFIX=/tmp/.pycache \
    PYTHONPATH="/app/src" \
    CONFIG_PATH="/app/config/strategies.toml" \
    DB_BASE_PATH="/app/data" \
    # Security hardening
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Set runtime permissions
RUN find /app -type d -exec chmod 755 {} \; && \
    find /app -type f -exec chmod 644 {} \;

# Health check for container orchestration
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Switch to non-root user
USER 1001

# Add missing CMD to start application - SOLUTION TO PRINT ISSUE
CMD ["python", "-u", "/app/src/receiver/deribit/main.py"]