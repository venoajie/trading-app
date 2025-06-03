# Stage 1: Builder
FROM python:3.13-slim AS builder

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Create app structure with proper permissions
RUN mkdir -p /app && chmod 775 /app
WORKDIR /app

# Copy only necessary files for dependency installation
COPY trading/shared/pyproject.toml trading/shared/ /app/shared/
COPY trading/receiver/pyproject.toml trading/receiver/ /app/receiver/

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir /app/shared && \
    pip install --no-cache-dir /app/receiver

# Stage 2: Runtime
FROM python:3.13-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    DB_BASE_PATH="/app/data" \
    PYTHONPATH="/app/scripts:$PYTHONPATH"

# Create app user and structure
RUN useradd -u 1001 -m appuser && \
    mkdir -p /app/data /app/config /app/scripts && \
    chown -R appuser:appuser /app && \
    chmod 775 /app /app/data

# Copy installed packages from builder
COPY --from=builder --chown=appuser:appuser /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
COPY --from=builder --chown=appuser:appuser /usr/local/bin /usr/local/bin

# Copy application code
COPY --chown=appuser:appuser config /app/config
COPY --chown=appuser:appuser trading/scripts /app/scripts
COPY --chown=appuser:appuser trading/receiver/receiver /app/receiver

# Switch to non-root user
USER appuser
WORKDIR /app/receiver

# Run the application
CMD ["python", "-m", "deribit.main"]