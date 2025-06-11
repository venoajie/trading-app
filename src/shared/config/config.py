import os
import tomllib
from .models import AppConfig
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
        return AppConfig(
            postgres={
                "host": os.getenv("POSTGRES_HOST", "postgres"),
                "port": int(os.getenv("POSTGRES_PORT", 5432)),
                "db": os.getenv("POSTGRES_DB", "trading"),
                "user": os.getenv("POSTGRES_USER", "trading_app"),
                "password": get_secret("db_password") or os.getenv("POSTGRES_PASSWORD", ""),
                "dsn": f"postgresql://{os.getenv('POSTGRES_USER')}:{get_secret('db_password')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
            },
            strategies=strategy_config,
            services={
                "name": os.getenv("SERVICE_NAME", "unknown"),
                "environment": os.getenv("ENVIRONMENT", "development")
            },
            error_handling={
                "telegram": {
                    "bot_token": get_secret("telegram_bot_token"),
                    "chat_id": get_secret("telegram_chat_id")
                }
            }
        )

# Global config instance
config = ConfigLoader().config