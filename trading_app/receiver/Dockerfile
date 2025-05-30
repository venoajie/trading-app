# trading_app/receiver/Dockerfile
# Optimized solution with virtual environment handling

# Stage 1: Base image with Python 3.13
FROM python:3.13-slim AS base

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

# Install dependencies using UV within the virtual environment
RUN . /opt/venv/bin/activate && \
    uv pip install --no-cache -r /shared/requirements.txt && \
    uv pip install --no-cache -e .

# Stage 3: Final production image
FROM base AS production
COPY --from=dependencies /opt/venv /opt/venv

# Create proper Python package structure
WORKDIR /app/trading_app
COPY trading_app/receiver/src/ ./receiver/src/
COPY trading_app/configuration/ ./configuration/
COPY trading_app/restful_api/ ./restful_api/
COPY trading_app/shared/ ./shared/

# Copy root-level configuration files
COPY config_strategies.toml /app/

# Add __init__.py files to create packages
RUN touch ./__init__.py && \
    touch ./configuration/__init__.py && \
    touch ./restful_api/__init__.py && \
    touch ./shared/__init__.py && \
    touch ./receiver/__init__.py

# Environment configuration
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH="/app:/app/trading_app" \
    UVICORN_HOST=0.0.0.0 \
    UVICORN_PORT=8000

# Health check for automatic restart
HEALTHCHECK --interval=30s --timeout=3s \
    CMD curl -f http://localhost:$UVICORN_PORT/health || exit 1

# Execute using Python module loader
WORKDIR /app
ENTRYPOINT ["python", "-m", "trading_app.receiver.src.main"]