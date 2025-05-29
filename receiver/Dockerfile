# receiver/Dockerfile
# Stage 1: Base image with Python 3.13 and UV
FROM python:3.13-slim AS base
RUN pip install --no-cache-dir uv==0.1.0 && \
    uv venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Stage 2: Shared library builder
FROM base AS shared-builder
WORKDIR /wheels
COPY shared/pyproject.toml /shared/
COPY shared/src/ /shared/src/
RUN uv pip build /shared -o /wheels

# Stage 3: Receiver dependencies installation
FROM base AS receiver-installer
COPY --from=shared-builder /wheels /wheels
RUN uv pip install --no-cache-dir /wheels/trading_shared-*.whl
COPY receiver/pyproject.toml /app/
WORKDIR /app
RUN uv pip install -e .

# Stage 4: Final production image
FROM base AS production
COPY --from=receiver-installer /opt/venv /opt/venv
COPY --from=receiver-installer /app /app
COPY receiver/src/ /app/src/
COPY config/ /app/config/
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