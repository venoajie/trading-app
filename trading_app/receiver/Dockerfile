# receiver/Dockerfile
# Optimized solution with virtual environment handling

# Stage 1: Base image with Python 3.13
FROM python:3.13-slim AS base

# Install UV and create virtual environment
RUN pip install --no-cache-dir uv==0.1.0 && \
    uv venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Stage 2: Install dependencies
FROM base AS dependencies
WORKDIR /app

# Copy dependency specifications
COPY trading_app/shared/requirements.txt /