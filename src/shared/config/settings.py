# shared/config/settings.py

"""
Configuration settings for the application, loading from environment variables and TOML files
"""


from .config import config


# Runtime accessible settings
def get_redis_url():
    return config["redis"]["url"]


def get_postgres_dsn():
    return config["postgres"]["dsn"] if config["postgres"] else None


def get_service_name():
    return config["services"]["name"]


# Direct access aliases
REDIS_URL = config["redis"]["url"]
POSTGRES_DSN = config["postgres"]["dsn"] if config["postgres"] else None
SERVICE_NAME = config["services"]["name"]
