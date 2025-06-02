# Stage 1: Base image with Python 3.13
FROM python:3.13-slim AS base

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libsqlite3-dev \
    sqlite3 \
    libssl-dev \
    libsnappy-dev \
    libz-dev \
    curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install UV and create virtual environment
RUN pip install --no-cache-dir uv==0.1.0 && \
    uv venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH"

# Stage 2: Install dependencies
FROM base AS dependencies
WORKDIR /app

# Copy dependency specifications first for better caching
COPY ./src/shared/requirements.txt /app/src/shared/
COPY ./src/receiver/pyproject.toml /app/src/receiver/

# Install base dependencies
RUN . /opt/venv/bin/activate && \
    uv pip install --no-cache -r /app/src/shared/requirements.txt

# Copy application code
COPY ./src /app/src
COPY ./config /app/config

# Install application-specific dependencies
RUN . /opt/venv/bin/activate && \
    uv pip install --no-cache /app/src/receiver/pyproject.toml
    
# Stage 3: Final production image
FROM base AS production

# Create non-root user with explicit UID/GID
RUN addgroup --system --gid 1001 appgroup && \
    adduser --system --uid 1001 --ingroup appgroup appuser

# Create data directories with correct permissions
RUN mkdir -p /app/data/databases && \
    chown -R 1001:1001 /app/data && \
    chmod -R 755 /app/data

# Copy application with proper ownership
COPY --from=dependencies --chown=1001:1001 /app /app
COPY --from=dependencies --chown=1001:1001 /opt/venv /opt/venv

# Environment configuration
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH="/app/src" \
    CONFIG_PATH="/app/config/strategies.toml" \
    DB_BASE_PATH="/app/data" \
    # Security hardening
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Switch to non-root user
USER 1001

# Health check (simplified and more reliable)
HEALTHCHECK --interval=30s --timeout=10s \
    CMD python -c "import os; \
    exit(0) if os.path.exists('/app/data/status.db') else exit(1)"

# Execute application
WORKDIR /app
CMD ["python", "-m", "receiver.deribit.main"]