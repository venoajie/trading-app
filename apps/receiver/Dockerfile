# Use optimized Python image with UV
FROM python:3.13-slim as base

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends libssl3 && \
    rm -rf /var/lib/apt/lists/*

# Install UV for faster package installation
RUN pip install --no-cache-dir uv

# Set up application environment
WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    DATA_DIR=/data

# Copy only necessary files
COPY pyproject.toml .
COPY shared/ ./shared/
COPY apps/receiver/ ./apps/receiver/

# Create and activate virtual environment
RUN uv venv .venv
ENV PATH="/app/.venv/bin:$PATH"

# Install dependencies
RUN uv pip install --no-cache-dir -e .

# Create data volume
RUN mkdir -p ${DATA_DIR} && chmod 777 ${DATA_DIR}
VOLUME ${DATA_DIR}

# Set entrypoint
CMD ["python", "-m", "apps.receiver.main"]