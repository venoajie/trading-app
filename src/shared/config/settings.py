"""
shared/config/settings.py
Configuration settings for the application, loading from environment variables and TOML files
"""

import os
import tomllib
from typing import Any, Dict
from core.security import get_secret

def load_toml(file_path: str) -> Dict[str, Any]:
    """Load TOML file directly"""
    try:
        with open(file_path, "rb") as f:
            return tomllib.load(f)
    except Exception:
        return {}

def get_config() -> Dict[str, Any]:
    """Load configuration from environment and TOML"""
    config = {}
    
    # Load from environment variables first
    config.update({
        "redis": {
            "url": os.getenv("REDIS_URL", "redis://localhost:6379"),
            "db": int(os.getenv("REDIS_DB", "0"))
        },
        "deribit": {
            "subaccount": os.getenv("DERIBIT_SUBACCOUNT", "deribit-148510"),
            "currencies": os.getenv("DERIBIT_CURRENCIES", "BTC,ETH").split(",")
        }
    })
    
    # Load from TOML if exists
    try:
        config_path = os.getenv("CONFIG_PATH", "/app/config/strategies.toml")
        config.update(load_toml(config_path))
    except Exception:
        pass
        
    return config

# Direct configuration values
CONFIG = get_config()

# Runtime settings
REDIS_URL = CONFIG["redis"]["url"]
REDIS_DB = CONFIG["redis"]["db"]
DERIBIT_SUBACCOUNT = CONFIG["deribit"]["subaccount"]
DERIBIT_CURRENCIES = CONFIG["deribit"]["currencies"]
# Maintenance Configuration
DERIBIT_MAINTENANCE_THRESHOLD = 300  # 15 minutes (in seconds)
DERIBIT_HEARTBEAT_INTERVAL = 30      # 30 seconds
DB_BASE_PATH="/app/data"

# PostgreSQL Configuration# PostgreSQL Configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "trading")
POSTGRES_USER = os.getenv("POSTGRES_USER", "trading_app")

# Handle secret securely
try:
    POSTGRES_PASSWORD = get_secret("db_password")
except RuntimeError:
    # Fallback for development
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "fallback_password")

POSTGRES_DSN = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"