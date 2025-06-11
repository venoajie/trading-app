# src/shared/config/config.py
import os
import tomllib
from typing import Any, Dict
from . import settings
from core.security import get_secret  # Ensure this doesn't import config

class ConfigLoader:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load()
        return cls._instance
    
    def _load(self):
        """Load configuration from multiple sources with priority order"""
        self.config = {
            "redis": self._load_redis_config(),
            "postgres": self._load_postgres_config(),
            "deribit": self._load_deribit_config(),
            "error_handling": self._load_error_config(),
            "services": self._load_service_config(),
            "strategies": self._load_strategy_config(),
        }
    
    def _load_redis_config(self) -> Dict[str, Any]:
        return {
            "url": os.getenv("REDIS_URL", "redis://localhost:6379"),
            "db": int(os.getenv("REDIS_DB", "0")),
            "streams": {
                "market_data": "stream:market_data",
                "dispatcher_group": "dispatcher_group"
            }
        }
    
    def _load_postgres_config(self) -> Dict[str, Any]:
        return {
            "host": os.getenv("POSTGRES_HOST", "postgres"),
            "port": os.getenv("POSTGRES_PORT", "5432"),
            "db": os.getenv("POSTGRES_DB", "trading"),
            "user": os.getenv("POSTGRES_USER", "trading_app"),
            "password": get_secret("db_password") or os.getenv("POSTGRES_PASSWORD"),
            "dsn": f"postgresql://{os.getenv('POSTGRES_USER')}:{get_secret('db_password')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}",
            "pool": {
                "min_size": 5,
                "max_size": 20,
                "command_timeout": 60
            }
        }
    
    def _load_deribit_config(self) -> Dict[str, Any]:
        return {
            "subaccount": os.getenv("DERIBIT_SUBACCOUNT", "deribit-148510"),
            "currencies": os.getenv("DERIBIT_CURRENCIES", "BTC,ETH").split(","),
            "ws": {
                "reconnect_base_delay": 5,
                "max_reconnect_delay": 300,
                "maintenance_threshold": 300,
                "heartbeat_interval": 30,
            }
        }
    
    def _load_error_config(self) -> Dict[str, Any]:
        return {
            "notify_telegram": os.getenv("ERROR_NOTIFY_TELEGRAM", "true").lower() == "true",
            "notify_redis": os.getenv("ERROR_NOTIFY_REDIS", "true").lower() == "true",
            "telegram": {
                "bot_token": get_secret("telegram_bot_token"),
                "chat_id": get_secret("telegram_chat_id")
            }
        }
    
    def _load_service_config(self) -> Dict[str, Any]:
        return {
            "name": os.getenv("SERVICE_NAME", "unknown"),
            "environment": os.getenv("ENVIRONMENT", "development"),
            "db_base_path": "/app/data"
        }
    
    def _load_strategy_config(self) -> Dict[str, Any]:
        config_path = os.getenv("STRATEGY_CONFIG_PATH", "/app/config/strategies.toml")
        try:
            with open(config_path, "rb") as f:
                return tomllib.load(f)
        except FileNotFoundError:
            return {}
    
    def __getattr__(self, name):
        return self.config.get(name, {})

# Singleton instance
config = ConfigLoader()