# Stage 1: Builder
FROM python:3.13-slim AS builder

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Create app structure
RUN mkdir -p /app && chmod 775 /app
WORKDIR /app

# Copy only necessary files
COPY trading/shared/pyproject.toml /app/shared/
COPY trading/shared/shared /app/shared/shared
COPY trading/receiver/pyproject.toml /app/receiver/
COPY trading/receiver/receiver /app/receiver/receiver

# Install using direct path to package source
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

# Switch to non-root user
USER appuser
WORKDIR /app

# Health check
HEALTHCHECK --interval=30s --timeout=5s \
    CMD python -c "import socket; socket.create_connection(('localhost', 8000), timeout=2)" || exit 1

# Run the application
CMD ["python", "-m", "receiver.deribit.main"]