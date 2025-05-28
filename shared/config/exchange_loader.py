import tomli
import os
from pathlib import Path

def load_exchange_config(exchange_name):
    config_path = Path(__file__).parent / "exchanges" / f"{exchange_name}.toml"
    with open(config_path, "rb") as f:
        config = tomli.load(f)
        
    # Inject secrets from environment
    config['auth']['client_id'] = os.getenv(f"{exchange_name.upper()}_CLIENT_ID")
    config['auth']['client_secret'] = os.getenv(f"{exchange_name.upper()}_SECRET")
    
    return config