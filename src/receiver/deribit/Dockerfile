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

# Copy dependency specifications
COPY ./src /app/src
COPY ./config /app/config


# Install dependencies using UV
RUN . /opt/venv/bin/activate && \
    uv pip install --no-cache -r /app/src/shared/requirements.txt && \
    uv pip install --no-cache -e /app/src/receiver

# Stage 3: Final production image
FROM base AS production
COPY --from=dependencies /opt/venv /opt/venv
COPY --from=dependencies /app /app

# Environment configuration

# Environment configuration
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH="/app/src" \
    CONFIG_PATH="/app/config/config_strategies.toml" \
    DB_BASE_PATH="/app/data" \
    # Security-related environment variables
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Health check
HEALTHCHECK --interval=30s --timeout=3s \
    CMD python -c "import sqlite3; conn = sqlite3.connect('/app/data/status.db'); conn.close()" || exit 1

# Execute application
WORKDIR /app
ENTRYPOINT ["python", "-m", "receiver.deribit.main"]