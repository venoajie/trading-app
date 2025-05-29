# receiver/Dockerfile
# Optimized for your directory structure

# Stage 1: Base image with Python 3.13 and UV
FROM python:3.13-slim AS base
RUN pip install --no-cache-dir uv==0.1.0 && \
    uv venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Stage 2: Install dependencies directly (no shared wheel)
FROM base AS dependencies
WORKDIR /app
COPY shared/ /shared/
COPY receiver/pyproject.toml .
RUN uv pip install -e . -r /shared/requirements.txt

# Stage 3: Final production image
FROM base AS production
COPY --from=dependencies /opt/venv /opt/venv
COPY --from=dependencies /app /app
COPY receiver/src/ /app/src/
COPY configuration/ /app/configuration/
COPY restful_api/ /app/restful_api/
COPY shared/ /app/shared/
WORKDIR /app

# Environment configuration
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    UVICORN_HOST=0.0.0.0 \
    UVICORN_PORT=8000

# Health check for automatic restart
HEALTHCHECK --interval=30s --timeout=3s \
    CMD curl -f http://localhost:$UVICORN_PORT/health || exit 1

# Entrypoint configuration
ENTRYPOINT ["uv", "run", "src.main:main"]