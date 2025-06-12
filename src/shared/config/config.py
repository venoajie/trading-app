# src\shared\config\config.py

"""
Primary configuration loader
"""


import os
import tomli
from core.security import get_secret


class ConfigLoader:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.config = cls._load_config()
        return cls._instance

    @staticmethod
    def _load_config():
        # Load strategy config
        config_path = os.getenv(
            "STRATEGY_CONFIG_PATH", "/app/src/shared/config/strategies.toml"
        )

        strategy_config = {}
        try:
            with open(config_path, "rb") as f:
                strategy_config = tomli.load(f)
        except FileNotFoundError:
            pass

        # Get Telegram credentials
        telegram_bot_token = ""
        telegram_chat_id = ""
        try:
            telegram_bot_token = get_secret("telegram_bot_token")
        except Exception:
            pass
        try:
            telegram_chat_id = get_secret("telegram_chat_id")
        except Exception:
            pass

        # Build PostgreSQL configuration
        postgres_config = None
        if os.getenv("SERVICE_NAME") == "distributor":
            try:
                password_str = get_secret("db_password")
            except Exception:
                password_str = os.getenv("POSTGRES_PASSWORD", "")

            if not password_str:
                raise RuntimeError("DB password missing for distributor service")

            user = os.getenv("POSTGRES_USER", "trading_app")
            host = os.getenv("POSTGRES_HOST", "postgres")
            port = int(os.getenv("POSTGRES_PORT", 5432))
            db = os.getenv("POSTGRES_DB", "trading")

            postgres_config = {
                "host": host,
                "port": port,
                "db": db,
                "user": user,
                "password": password_str,
                "dsn": f"postgresql://{user}:{password_str}@{host}:{port}/{db}",
                "pool": {"min_size": 5, "max_size": 20, "command_timeout": 60},
            }

        # Build Redis configuration
        redis_config = {
            "url": os.getenv("REDIS_URL", "redis://localhost:6379"),
            "db": int(os.getenv("REDIS_DB", 0)),
        }

        # Build services configuration
        services_config = {
            "name": os.getenv("SERVICE_NAME", "distributor"),
            "environment": os.getenv("ENVIRONMENT", "production"),
        }

        # Build error handling configuration
        error_handling_config = {
            "telegram": {
                "bot_token": telegram_bot_token,
                "chat_id": telegram_chat_id,
            },
            "notify_redis": os.getenv("ERROR_NOTIFY_REDIS", "true").lower() == "true",
        }

        # Return consolidated configuration
        return {
            "redis": redis_config,
            "postgres": postgres_config,
            "services": services_config,
            "strategies": strategy_config,
            "error_handling": error_handling_config,
        }


# Global config instance
config = ConfigLoader().config
