# shared/config/settings.py

"""
Configuration settings for the application, loading from environment variables and TOML files
"""


from .config import config


# Runtime accessible settings with lazy loading
def get_redis_url():
    return config.redis.url


def get_postgres_dsn():
    return config.postgres.dsn if config.postgres else None


def get_service_name():
    return config.services.name


# Runtime accessible settings (prefer accessing config directly)
REDIS_URL = config.redis.url
POSTGRES_DSN = config.postgres.dsn
SERVICE_NAME = config.services.name
