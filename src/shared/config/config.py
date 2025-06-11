"""
src\shared\config\config.py
Primary configuration loader
"""

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
        try:
            password = get_secret("db_password")
        except Exception:
            password = os.getenv("POSTGRES_PASSWORD", "")
            
        if not password and os.getenv("SERVICE_NAME") == "distributor":
            raise RuntimeError("DB password missing for distributor service")
     
        return AppConfig(
            postgres={
                "host": os.getenv("POSTGRES_HOST", "postgres"),
                "port": int(os.getenv("POSTGRES_PORT", 5432)),
                "db": os.getenv("POSTGRES_DB", "trading"),
                "user": os.getenv("POSTGRES_USER", "trading_app"),
                "password": password,
                "dsn": f"postgresql://{os.getenv('POSTGRES_USER')}:{get_secret('db_password')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
            },
            strategies=strategy_config,
            services={
                "name": os.getenv("SERVICE_NAME", "unknown"),
                "environment": os.getenv("ENVIRONMENT", "development")
            },
              
            error_handling={
                "telegram": {
                    "bot_token": get_secret("telegram_bot_token") or "",
                    "chat_id": get_secret("telegram_chat_id") or ""
                }
            }
            
        )

# Global config instance
config = ConfigLoader().config