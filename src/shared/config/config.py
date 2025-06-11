# src\shared\config\config.py

"""
Primary configuration loader
"""

import os
import tomli
from .models import AppConfig, RedisConfig, PostgresConfig
from core.security import get_secret

# from pydantic import SecretStr


class ConfigLoader:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.config = cls._load_config()
        return cls._instance

    @staticmethod
    def _load_config() -> AppConfig:

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

        # Build configuration
        postgres_config = None

        if os.getenv("SERVICE_NAME") == "distributor":
            try:
                password_str = get_secret("db_password")
            except Exception:
                password_str = os.getenv("POSTGRES_PASSWORD", "")

            if not password_str:  # Fix variable name here
                raise RuntimeError("DB password missing for distributor service")

            postgres_config = {
                "host": os.getenv("POSTGRES_HOST", "postgres"),
                "port": int(os.getenv("POSTGRES_PORT", 5432)),
                "db": os.getenv("POSTGRES_DB", "trading"),
                "user": os.getenv("POSTGRES_USER", "trading_app"),
                "password": password_str,
                "dsn": f"postgresql://{os.getenv('POSTGRES_USER')}:{password_str}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}",  # Fix here
                "pool": {"min_size": 5, "max_size": 20, "command_timeout": 60},
            }
            
        postgres_config_all=(
                PostgresConfig(**postgres_config) if postgres_config else None
            )
        print(f"postgres_config {postgres_config_all}")
        redis_config=RedisConfig(
                url=os.getenv("REDIS_URL", "redis://localhost:6379"),
                db=int(os.getenv("REDIS_DB", 0)),
            )
        print(f"redis_config {redis_config}")
        services_config={
                "name": os.getenv("SERVICE_NAME", "distributor"),
                "environment": os.getenv("ENVIRONMENT", "production"),
            }
        print(f"services_config {services_config}")
        error_handling_config={
                "telegram": {
                    "bot_token": telegram_bot_token,
                    "chat_id": telegram_chat_id,
                }
            }
        print(f"error_handling_config {error_handling_config}")

        return AppConfig(
            redis=redis_config,
            postgres=postgres_config_all,  # Will be None for receiver
            strategies=strategy_config,
            services=services_config,
            error_handling=error_handling_config,
        )


# Global config instance
config = ConfigLoader().config
