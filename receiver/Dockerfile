# Builder stage
FROM python:3.13-slim as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends gcc

# Create virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install dependencies
COPY pyproject.toml .
COPY receiver/pyproject.toml ./receiver/
RUN pip install --no-cache-dir uv && \
    uv pip install -r receiver/pyproject.toml

# Runtime stage
FROM python:3.13-slim

WORKDIR /app
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

# Copy virtual environment
COPY --from=builder /opt/venv /opt/venv

# Copy application code
COPY shared ./shared
COPY receiver ./receiver

# Run application
CMD ["python", "receiver/main.py"]  