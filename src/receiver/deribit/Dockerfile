# Dockerfile
FROM python:3.13-slim AS base

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libsqlite3-dev sqlite3 curl && \
    rm -rf /var/lib/apt/lists/*

# Install UV and create virtual environment
RUN pip install --no-cache-dir uv==0.1.0 && \
    uv venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Set working directory
WORKDIR /app

# Copy only necessary files
COPY ./src /app/src
COPY ./config /app/config

# Install dependencies
RUN . /opt/venv/bin/activate && \
    uv pip install --no-cache -r /app/src/trading_app/shared/requirements.txt && \
    uv pip install --no-cache -e /app/src

# Environment configuration
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH="/app:/app/src" \
    CONFIG_PATH="/app/config/config_strategies.toml" \
    DB_BASE_PATH="/app/data"

# Health check
HEALTHCHECK --interval=30s --timeout=3s \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health', timeout=3)" || exit 1

# Entrypoint
ENTRYPOINT ["python", "-m", "trading_app.receiver.src.main"]