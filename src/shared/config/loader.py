"""
shared/config/loader.py
Unified configuration loader
"""

import os
import tomllib
from typing import Any, Dict

def load_toml(file_path: str) -> Dict[str, Any]:
    """Load TOML file with error handling"""
    try:
        with open(file_path, "rb") as f:
            return tomllib.load(f)
    except Exception as e:
        return {}

def get_config() -> Dict[str, Any]:
    """Load all configuration files"""
    base_path = os.getenv("CONFIG_DIR", "/app/config")
    config = {}
    
    # Load strategy config
    config.update(load_toml(f"{base_path}/strategies.toml"))
    
    # Load security config
    config.update(load_toml(f"{base_path}/security.toml"))
    
    return config

def get_setting(key: str, default: Any = None) -> Any:
    """
    Get setting with priority:
    1. Environment variable
    2. TOML config
    3. Default value
    """
    # Environment variables take precedence
    env_value = os.getenv(key.upper())
    if env_value:
        return env_value
        
    # Check TOML config
    keys = key.split('.')
    current = get_config()
    for k in keys:
        current = current.get(k, {})
    if current != {}:
        return current
        
    return default