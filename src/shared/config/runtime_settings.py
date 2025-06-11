# src/shared/config/runtime_settings.py

"""
Optimized runtime access
"""

from .config import config

# Runtime accessible settings (avoid direct access, use through config)
    """
    Redundant Config Copies? Improvement: Access via singleton directly instead of creating copies.
    """
REDIS_URL = config.redis.url
POSTGRES_DSN = config.postgres.dsn
SERVICE_NAME = config.services.name