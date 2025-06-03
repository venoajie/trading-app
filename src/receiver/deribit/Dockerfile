FROM python:3.13-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH="/app/src:/app/src/shared:/app/src/receiver" \
    DB_BASE_PATH="/app/data"

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends build-essential && \
    rm -rf /var/lib/apt/lists/*

# Create app structure
RUN mkdir -p /app/src && chmod 775 /app
WORKDIR /app

# Copy only necessary files
COPY src/shared /app/src/shared
COPY src/receiver /app/src/receiver
COPY config /app/config

# Install Python dependencies directly from pyproject.toml
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir /app/src/shared && \
    pip install --no-cache-dir /app/src/receiver

# Create non-root user
RUN useradd -u 1001 -m appuser && \
    chown -R appuser:appuser /app
USER appuser

CMD ["python", "-u", "/app/src/receiver/deribit/main.py"]