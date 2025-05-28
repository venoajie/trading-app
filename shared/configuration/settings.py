from pydantic import BaseSettings
import tomli

class CoreSettings(BaseSettings):
    redis_host: str
    redis_port: int = 6379
    env_file: str = ".env"

    class Config:
        secrets_dir = "/run/secrets"

def load_toml_config(path: str) -> dict:
    with open(path, "rb") as f:
        return tomli.load(f)