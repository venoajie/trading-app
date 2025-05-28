"""
Centralized configuration management
"""

import os
from pydantic import BaseSettings

class DeribitConfig(BaseSettings):
    client_id: str
    client_secret: str
    sub_account_id: str

    class Config:
        env_prefix = "DERIBIT_"

class RedisConfig(BaseSettings):
    host: str = "redis"

    class Config:
        env_prefix = "REDIS_"

class AppConfig(BaseSettings):
    deribit: DeribitConfig = DeribitConfig()
    redis: RedisConfig = RedisConfig()
    data_dir: str = "/data"

def get_config() -> AppConfig:
    """Load and return application configuration"""
    return AppConfig()