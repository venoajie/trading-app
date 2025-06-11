# src\shared\config\config.py

"""
Primary configuration loader
"""

import os
import tomllib
from .models import AppConfig, RedisConfig, PostgresConfig
from core.security import get_secret

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
        config_path = os.getenv("STRATEGY_CONFIG_PATH", "/app/config/shared/strategies.toml")
        strategy_config = {}
        try:
            with open(config_path, "rb") as f:
                strategy_config = tomllib.load(f)
        except FileNotFoundError:
            pass

        # Build configuration
        postgres_config = None
        if os.getenv("SERVICE_NAME") == "distributor":
            try:
                password = get_secret("db_password")
            except Exception:
                password = os.getenv("POSTGRES_PASSWORD", "")
            
            if not password:
                raise RuntimeError("DB password missing for distributor service")
            
            postgres_config = {
                "host": os.getenv("POSTGRES_HOST", "postgres"),
                "port": int(os.getenv("POSTGRES_PORT", 5432)),
                "db": os.getenv("POSTGRES_DB", "trading"),
                "user": os.getenv("POSTGRES_USER", "trading_app"),
                "password": password,
                "dsn": f"postgresql://{os.getenv('POSTGRES_USER')}:{password}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}",
                "pool": {"min_size": 5, "max_size": 20, "command_timeout": 60}
            }
     
        return AppConfig(
    redis=RedisConfig(
        url=os.getenv("REDIS_URL", "redis://localhost:6379"),
        db=int(os.getenv("REDIS_DB", 0))
    ),
    postgres=PostgresConfig(**postgres_config) if postgres_config else None,# Will be None for receiver
            strategies=strategy_config,
            services={
                "name": os.getenv("SERVICE_NAME", "unknown"),
                "environment": os.getenv("ENVIRONMENT", "development")
            },
              
#            error_handling={
#                "telegram": {
#                    "bot_token": get_secret("telegram_bot_token") or "",
#                    "chat_id": get_secret("telegram_chat_id") or ""
#                }
#            }
            
        )

# Global config instance
config = ConfigLoader().config