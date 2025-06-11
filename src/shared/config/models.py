"""
src\shared\config\models.py
Pydantic schema definitions
"""

from pydantic import BaseModel, Field, RedisDsn, PostgresDsn
from pydantic_settings import BaseSettings
from pydantic.tools import parse_obj_as
from typing import Dict, List, Any, Optional
from pathlib import Path

class RedisConfig(BaseModel):
    url: RedisDsn = "redis://localhost:6379"
    db: int = 0
    streams: Dict[str, str] = Field(
        default={"market_data": "stream:market_data"},
        description="Redis stream names"
    )

class PostgresConfig(BaseModel):
    host: str = "postgres"
    port: int = Field(ge=1024, le=65535, default=5432)
    db: str = "trading"
    user: str = "trading_app"
    password: str = Field(..., description="Database password")
    dsn: PostgresDsn = Field(..., description="Connection string")
    pool: Dict[str, Any] = Field(
        default={"min_size": 5, "max_size": 20, "command_timeout": 60},
        description="Connection pool settings"
    )

class DeribitConfig(BaseModel):
    subaccount: str = "deribit-148510"
    currencies: List[str] = ["BTC", "ETH"]
    ws: Dict[str, Any] = Field(
        default={
            "reconnect_base_delay": 5,
            "max_reconnect_delay": 300,
            "maintenance_threshold": 300,
            "heartbeat_interval": 30
        },
        description="WebSocket configuration"
    )

class ErrorHandlingConfig(BaseModel):
    notify_telegram: bool = True
    notify_redis: bool = True
    telegram: Dict[str, str] = Field(
        default_factory=dict,
        description="Telegram notification settings"
    )

class ServiceConfig(BaseModel):
    name: str = "unknown"
    environment: str = "development"
    db_base_path: Path = Path("/app/data")


class StrategyConfig(BaseModel):
    strategy_label: str
    is_active: bool
    
class AppConfig(BaseSettings):
    redis: RedisConfig = RedisConfig()
    postgres: PostgresConfig = Field(...)
    deribit: DeribitConfig = Field(default_factory=DeribitConfig)
    error_handling: ErrorHandlingConfig = Field(default_factory=ErrorHandlingConfig)
    services: ServiceConfig = Field(default_factory=ServiceConfig)
    strategies: Dict[str, StrategyConfig] = Field(default_factory=dict)

    class Config:
        env_prefix = "APP_"
        env_nested_delimiter = "__"
        secrets_dir = "/run/secrets"