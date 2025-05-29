import os
import logging
from dotenv import load_dotenv

# For Python 3.11+ use tomllib, else tomli
try:
    import tomllib
except ImportError:
    import tomli as tomllib

load_dotenv()

def load_config(config_path: str) -> dict:
    """Load TOML configuration file"""
    try:
        with open(config_path, "rb") as f:
            return tomllib.load(f)
    except FileNotFoundError:
        logging.error(f"Config file not found: {config_path}")
        return {}
    except Exception as e:
        logging.error(f"Error loading config: {str(e)}")
        return {}

def get_env(key: str, default: str = None) -> str:
    """Get environment variable with fallback"""
    return os.environ.get(key, default)