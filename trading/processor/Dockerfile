# Stage 1: Builder
FROM python:3.13-slim AS builder

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Create app structure
RUN mkdir -p /app && chmod 775 /app
WORKDIR /app

# Copy shared package (same as receiver)
COPY trading/shared /app/shared
RUN mv /app/shared/shared /app/shared_pkg && \
    mv /app/shared /app/shared_src && \
    mkdir -p /app/shared && \
    mv /app/shared_src/pyproject.toml /app/shared/ && \
    mv /app/shared_pkg /app/shared/shared

# Copy processor package
COPY trading/processor /app/processor
RUN mv /app/processor/processor /app/processor_pkg && \
    mv /app/processor /app/processor_src && \
    mkdir -p /app/processor && \
    mv /app/processor_src/pyproject.toml /app/processor/ && \
    mv /app/processor_pkg /app/processor/processor

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir /app/shared && \
    pip install --no-cache-dir /app/processor

# Stage 2: Runtime
FROM python:3.13-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    DB_BASE_PATH="/app/data"

# Create app user and structure
RUN useradd -u 1001 -m appuser && \
    mkdir -p /app/data && \
    chown -R appuser:appuser /app && \
    chmod 775 /app /app/data

# Copy installed packages from builder
COPY --from=builder --chown=appuser:appuser /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
COPY --from=builder --chown=appuser:appuser /usr/local/bin /usr/local/bin

# Switch to non-root user
USER appuser
WORKDIR /app

# Run the processor
CMD ["python", "-m", "processor.main"]