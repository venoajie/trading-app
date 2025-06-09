# src\services\distributor\Dockerfile

FROM python:3.13-slim AS base
ENV UV_VENV=/opt/venv
RUN python -m pip install --no-cache-dir uv && \
    python -m uv venv ${UV_VENV}
ENV PATH="${UV_VENV}/bin:$PATH"

FROM base AS builder
WORKDIR /build
COPY src/shared/pyproject.toml src/shared/
COPY src/services/distributor/pyproject.toml src/services/distributor/
RUN uv pip install --no-cache -e ./src/shared -e ./src/services/distributor

FROM base AS runtime
RUN groupadd -r appuser --gid=1000 && \
    useradd -r -g appuser --uid=1000 appuser && \
    mkdir -p /app/data && chown -R 1000:1000 /app && chmod -R 750 /app
COPY --from=builder --chown=1000:1000 ${UV_VENV} ${UV_VENV}
WORKDIR /app
COPY --chown=1000:1000 ./src ./src
COPY --chown=1000:1000 ./config ./config
COPY --chown=1000:1000 ./core ./core
ENV DB_BASE_PATH=/app/data PYTHONPATH=/app PYTHONUNBUFFERED=1
USER 1000
CMD ["python", "src/services/distributor/deribit/main.py"]