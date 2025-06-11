# shared/config/settings.py

"""
Configuration settings for the application, loading from environment variables and TOML files
"""


from .config import config

# Runtime accessible settings (prefer accessing config directly)
REDIS_URL = config.redis.url
POSTGRES_DSN = config.postgres.dsn
SERVICE_NAME = config.services.name