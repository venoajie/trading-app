# Builder stage
FROM python:3.12-slim as builder

WORKDIR /app
RUN pip install uv

COPY pyproject.toml .
RUN uv venv .venv && \
    . .venv/bin/activate && \
    uv pip install -r pyproject.toml

# Runtime stage
FROM python:3.12-slim

WORKDIR /app
ENV PATH="/app/.venv/bin:$PATH"

# Copy virtual environment
COPY --from=builder /app/.venv /app/.venv

# Copy application code
COPY apps/receiver /app
COPY shared /app/shared
COPY streaming_helper /app/streaming_helper

# Install runtime dependencies
RUN apt-get update && apt-get install -y libssl3 && rm -rf /var/lib/apt/lists/*

CMD ["python", "-m", "uvloop", "main.py"]