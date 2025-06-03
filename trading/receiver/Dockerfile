# Stage 1: Base image with UV
FROM python:3.11-slim as base

# Install UV and create virtual environment
RUN pip install --no-cache-dir uv \
    && python -m uv venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Stage 2: Dependency installation
FROM base as builder

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends build-essential

# Copy only dependency files first
WORKDIR /app
COPY trading/shared/pyproject.toml trading/shared/
COPY trading/receiver/pyproject.toml trading/receiver/

# Install dependencies with UV
WORKDIR /app/trading/shared
RUN uv pip install -e .

WORKDIR /app/trading/receiver
RUN uv pip install -e .

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