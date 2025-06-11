# src\shared\config\config.py

"""
Primary configuration loader
"""

import os
import tomli
from .models import AppConfig, RedisConfig, PostgresConfig
from core.security import get_secret
#from pydantic import SecretStr

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
        print(f"Current working directory: {os.getcwd()}")
        config_path = os.getenv("STRATEGY_CONFIG_PATH", "/app/src/shared/config/strategies.toml")
        print(f"Loading strategy config from: {config_path}")
        print(f"Directory contents: {os.listdir(os.path.dirname(config_path))}")

        strategy_config = {}
        try:
            with open(config_path, "rb") as f:
                strategy_config = tomli.load(f)
                print(f"strategy_config {strategy_config}")
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
    
            print(f"password_secret {password_str}")
            postgres_config = {
                "host": os.getenv("POSTGRES_HOST", "postgres"),
                "port": int(os.getenv("POSTGRES_PORT", 5432)),
                "db": os.getenv("POSTGRES_DB", "trading"),
                "user": os.getenv("POSTGRES_USER", "trading_app"),
                "password": password_str,
                "dsn": f"postgresql://{os.getenv('POSTGRES_USER')}:{password_str}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}",  # Fix here
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
                "name": os.getenv("SERVICE_NAME", "distributor"),
                "environment": os.getenv("ENVIRONMENT", "production")
            },
              
            error_handling={
                "telegram": {
                    "bot_token": telegram_bot_token,
                    "chat_id": telegram_chat_id
                }
            }
            
        )

# Global config instance
config = ConfigLoader().config