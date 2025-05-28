from .settings import load_toml_config

def load_exchange_config(exchange: str) -> dict:
    return load_toml_config(f"config/exchanges/{exchange}.toml")