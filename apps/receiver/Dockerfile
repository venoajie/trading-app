# Use official Python 3.13 slim image
FROM python:3.13-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    DATA_DIR=/data

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libc-dev && \
    rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY apps/receiver/requirements.txt .

# Install Python dependencies using uv
RUN pip install --no-cache-dir uv && \
    uv pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY shared /app/shared
COPY apps/receiver /app/apps/receiver

# Copy configuration files
COPY .env /app/.env
COPY config_strategies.toml /app/config_strategies.toml
COPY pyproject.toml /app/pyproject.toml

# Create data volume
RUN mkdir -p ${DATA_DIR} && chmod 777 ${DATA_DIR}
VOLUME ${DATA_DIR}

# Set entrypoint
COPY apps/receiver/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]