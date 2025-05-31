"""
shared/db_management/redis_client.py
Async Redis client utilities with enhanced connection management
"""

import logging
from typing import Any, Dict, Optional, Union

import redis.asyncio as aioredis
import orjson
from shared import error_handling
from shared.config import get_config_value

# Configure logger
log = logging.getLogger(__name__)

async def create_redis_pool() -> aioredis.Redis:
    """Create Redis connection pool with configuration"""
    redis_url = get_config_value("redis.url", "redis://localhost:6379")
    redis_db = get_config_value("redis.db", 0)
    log.info(f"Creating Redis pool for {redis_url}, db {redis_db}")
    
    return await aioredis.from_url(
        redis_url,
        db=redis_db,
        encoding="utf-8",
        decode_responses=True,
        socket_connect_timeout=5,
        socket_keepalive=True
    )

async def publish_message(
    client_redis: aioredis.Redis,
    channel: str,
    message: Union[Dict, str],
) -> None:
    """Publish message to Redis channel with error handling"""
    try:
        # Serialize if message is dict
        if isinstance(message, dict):
            message = orjson.dumps(message).decode("utf-8")
        await client_redis.publish(channel, message)
    except Exception as error:
        log.error(f"Redis publish error: {error}")
        await error_handling.handle_error(client_redis, error, "REDIS_PUB_ERROR")

async def save_to_hash(
    client_redis: aioredis.Redis,
    hash_key: str,
    field: str,
    data: Any,
) -> None:
    """Save data to Redis hash field"""
    try:
        # Serialize if data is not string
        if not isinstance(data, (str, bytes)):
            data = orjson.dumps(data).decode("utf-8")
        await client_redis.hset(hash_key, field, data)
    except Exception as error:
        log.error(f"Redis save error: {error}")
        await error_handling.handle_error(client_redis, error, "REDIS_SAVE_ERROR")

async def get_from_hash(
    client_redis: aioredis.Redis,
    hash_key: str,
    field: str,
) -> Optional[Any]:
    """Retrieve data from Redis hash field"""
    try:
        data = await client_redis.hget(hash_key, field)
        if data:
            try:
                return orjson.loads(data)
            except orjson.JSONDecodeError:
                return data
        return None
    except Exception as error:
        log.error(f"Redis get error: {error}")
        await error_handling.handle_error(client_redis, error, "REDIS_GET_ERROR")
        return None