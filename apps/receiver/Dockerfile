# Use optimized Python image
FROM python:3.13-slim

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libssl3 && \
    rm -rf /var/lib/apt/lists/*

# Set up application environment
WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    DATA_DIR=/data

# Copy only necessary files
COPY pyproject.toml .
COPY shared/ ./shared/
COPY apps/receiver/ ./apps/receiver/

# Install dependencies using standard pip
RUN pip install --no-cache-dir -e .

# Create data volume
RUN mkdir -p ${DATA_DIR} && chmod 777 ${DATA_DIR}
VOLUME ${DATA_DIR}

# Set entrypoint
CMD ["python", "-m", "apps.receiver.main"]