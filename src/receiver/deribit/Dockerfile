# Stage 3: Final production image
FROM base AS production

# Create non-root user with explicit UID/GID
RUN addgroup --system --gid 1001 appgroup && \
    adduser --system --uid 1001 --ingroup appgroup appuser

# Create data directories with correct permissions
RUN mkdir -p /app/data/databases && \
    chown -R 1001:1001 /app/data && \
    chmod -R 755 /app/data

# Create config directory with correct permissions
RUN mkdir -p /app/config && \
    chown -R 1001:1001 /app/config && \
    chmod -R 755 /app/config

# Copy application with proper ownership
COPY --from=dependencies --chown=1001:1001 /app /app
COPY --from=dependencies --chown=1001:1001 /opt/venv /opt/venv

# Environment configuration
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH="/app/src" \
    CONFIG_PATH="/app/config/strategies.toml" \
    DB_BASE_PATH="/app/data" \
    # Security hardening
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Set runtime permissions
RUN chmod 755 /app/data && \
    find /app/data -type d -exec chmod 755 {} \; && \
    find /app/data -type f -exec chmod 644 {} \;

# Switch to non-root user
USER 1001

# Health check and CMD remain the same