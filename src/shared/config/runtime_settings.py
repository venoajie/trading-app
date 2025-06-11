# src/shared/config/runtime_settings.py

"""
Optimized runtime access
"""

from .config import config


# Runtime accessible settings
REDIS_URL = config["redis"]["url"]
POSTGRES_DSN = config["postgres"]["dsn"] if config["postgres"] else None
SERVICE_NAME = config["services"]["name"]