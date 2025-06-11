"""
src/shared/config/runtime_settings.py
"""
from .config import config

# Runtime accessible settings (avoid direct access, use through config)
REDIS_URL = config.redis.url
POSTGRES_DSN = config.postgres.dsn
SERVICE_NAME = config.services.name