import os
import tomli
from dotenv import load_dotenv

load_dotenv()

def load_config(config_path: str) -> dict:
    """Load TOML configuration file"""
    with open(config_path, "rb") as f:
        return tomli.load(f)

def get_env(key: str, default: str = None) -> str:
    """Get environment variable with fallback"""
    return os.environ.get(key, default)