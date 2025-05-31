"""
shared/config.py
Centralized configuration management with environment variable support
"""

import os
import logging
from typing import Any, Dict, Union, Optional

# For Python 3.11+ use tomllib, else tomli
try:
    import tomllib
except ImportError:
    import tomli as tomllib

# Configure logger
log = logging.getLogger(__name__)

# Base configuration directory
CONFIG_DIR = os.getenv("CONFIG_DIR", "/app/config")

def load_config() -> Dict[str, Any]:
    """Load configuration from TOML files with error handling"""
    config = {}
    
    try:
        # Load main strategy config
        strategy_path = os.path.join(CONFIG_DIR, "config_strategies.toml")
        if os.path.exists(strategy_path):
            with open(strategy_path, "rb") as f:
                config.update(tomllib.load(f))
        
        # Load environment-specific config
        env = os.getenv("APP_ENV", "production")
        env_path = os.path.join(CONFIG_DIR, f"config_{env}.toml")
        if os.path.exists(env_path):
            with open(env_path, "rb") as f:
                config.update(tomllib.load(f))
    except Exception as e:
        log.error(f"Error loading configuration: {e}")
        # Fallback to default config if possible
        config = {}
    
    return config

def get_config_value(
    key: str, 
    default: Union[str, int, float],
    config: Optional[Dict] = None
) -> Union[str, int, float]:
    """
    Get config value with environment variable override.
    Supports nested keys using dot notation (e.g., 'ws.reconnect_base_delay')
    """
    # Use global config if not provided
    if config is None:
        config = CONFIG
    
    # Environment variables take precedence
    env_key = key.upper().replace(".", "_")
    env_value = os.getenv(env_key)
    if env_value:
        try:
            # Convert numeric values
            if isinstance(default, int):
                return int(env_value)
            if isinstance(default, float):
                return float(env_value)
            return env_value
        except ValueError:
            log.warning(f"Invalid environment value for {env_key}: {env_value}")
            return default
    
    # Traverse config dictionary using dot notation
    keys = key.split('.')
    current = config
    try:
        for k in keys:
            current = current[k]
        return current
    except (KeyError, TypeError):
        return default

# Global config instance
CONFIG = load_config()