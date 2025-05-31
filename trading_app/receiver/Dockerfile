# trading_app/receiver/Dockerfile

# Stage 1: Base image with Python 3.13
FROM python:3.13-slim AS base

# Install system dependencies including SQLite and curl for health checks
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libsqlite3-dev \
    sqlite3 \
    curl && \
    rm -rf /var/lib/apt/lists/*

# Install UV and create virtual environment
RUN pip install --no-cache-dir uv==0.1.0 && \
    uv venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH"

# Stage 2: Install dependencies
FROM base AS dependencies
WORKDIR /app

# Copy dependency specifications
COPY trading_app/shared/requirements.txt /shared/requirements.txt
COPY trading_app/receiver/pyproject.toml .

# Install dependencies using UV
RUN . /opt/venv/bin/activate && \
    uv pip install --no-cache -r /shared/requirements.txt && \
    uv pip install --no-cache -e .

# Stage 3: Final production image
FROM base AS production
COPY --from=dependencies /opt/venv /opt/venv

# Create application structure
WORKDIR /app/trading_app
COPY trading_app/receiver/src/ ./receiver/src/
COPY trading_app/configuration/ ./configuration/
COPY trading_app/restful_api/ ./restful_api/
COPY trading_app/shared/ ./shared/

# Copy root-level configuration
COPY config_strategies.toml /app/

# Environment configuration
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH="/app:/app/trading_app" \
    UVICORN_HOST=0.0.0.0 \
    UVICORN_PORT=8000 \
    DB_BASE_PATH="/app/data"  

# Health check using Python's HTTP client (no curl dependency)
HEALTHCHECK --interval=30s --timeout=3s \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:$UVICORN_PORT/health', timeout=3)" || exit 1

# Execute application
WORKDIR /app
ENTRYPOINT ["python", "-m", "trading_app.receiver.src.main"]