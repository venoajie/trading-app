# Stage 1: Base image with UV
FROM python:3.13-slim AS base

# Install UV and create virtual environment

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/code/.venv

COPY --from=ghcr.io/astral-sh/uv:0.5.2 /uv /uvx /bin/

# Create a venv with Pyton 3.12.6, installing Python along the way
RUN uv venv --python 3.13 /venv

# Stage 2: Dependency installation
FROM base as builder

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends build-essential

# Copy only dependency files first
WORKDIR /app
COPY trading/shared/pyproject.toml ./
COPY trading/receiver/pyproject.toml ./

# Install dependencies with UV
WORKDIR /app/trading/shared
RUN uv pip install --system . ...

WORKDIR /app/trading/receiver
RUN uv pip install --system . ...

# Stage 3: Runtime image
FROM base as runtime

# Copy virtual environment
COPY --from=builder /opt/venv /opt/venv

# Copy application code
WORKDIR /app
COPY . .

# Set up data directory
RUN mkdir -p /app/data && chmod 775 /app/data

# Environment variables
ENV DB_BASE_PATH=/app/data \
    PYTHONPATH=/app:$PYTHONPATH

# Run receiver service
CMD ["python", "trading/receiver/receiver/deribit/main.py"]