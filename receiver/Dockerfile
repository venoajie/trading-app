# apps/receiver/Dockerfile
FROM python:3.13-slim

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libssl-dev && \
    rm -rf /var/lib/apt/lists/*

# Install UV
RUN pip install --no-cache-dir uv

# Set working directory
WORKDIR /app

# Copy only necessary files
COPY apps/receiver /app/receiver

# Install Python dependencies
RUN uv pip install -r /app/receiver/pyproject.toml

# Set environment variables
ENV PYTHONPATH=/app \
    UVICORN_WORKERS=1 \
    UVICORN_LOG_LEVEL=warning

# Run the application
CMD ["python", "-m", "uvloop", "/app/receiver/main.py"]