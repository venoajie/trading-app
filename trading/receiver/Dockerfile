# Stage 1: Builder
FROM python:3.13-slim AS builder

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    DB_BASE_PATH="/app/data"

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Create app structure
RUN mkdir -p /app && chmod 775 /app
WORKDIR /app

# Copy only necessary files for dependency installation
COPY trading/shared/pyproject.toml trading/shared/shared/ /app/shared/
COPY trading/receiver/pyproject.toml trading/receiver/receiver/ /app/receiver/
COPY trading/scripts /app/scripts

# Install Python dependencies using proper package structure
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -e /app/shared && \
    pip install --no-cache-dir -e /app/receiver

# Stage 2: Runtime
FROM python:3.13-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    DB_BASE_PATH="/app/data" \
    PYTHONPATH="/app/scripts:$PYTHONPATH"

# Create app user and structure
RUN useradd -u 1001 -m appuser && \
    mkdir -p /app/data && chown -R appuser:appuser /app && \
    chmod 775 /app /app/data

# Copy installed packages from builder
COPY --from=builder --chown=appuser:appuser /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
COPY --from=builder --chown=appuser:appuser /usr/local/bin /usr/local/bin

# Copy application code
COPY --chown=appuser:appuser config /app/config
COPY --chown=appuser:appuser trading/scripts /app/scripts
COPY --chown=appuser:appuser trading/receiver/receiver/deribit /app/receiver

# Switch to non-root user
USER appuser
WORKDIR /app/receiver

# Run the application
CMD ["python", "-u", "main.py"]