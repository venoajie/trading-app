"""
shared/db_management/redis_client.py
Async Redis client utilities with enhanced connection management
"""

import logging
from typing import Any, Dict, Optional, Union

import redis.asyncio as aioredis
import orjson
from shared.utils import error_handling
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
    
    
async def saving_and_publishing_result(
    client_redis: object,
    channel: str,
    keys: str,
    cached_data: list,
    message: dict,
) -> None:
    """ """

    try:
        # updating cached data
        if cached_data:
            await saving_result(
                client_redis,
                channel,
                keys,
                cached_data,
            )

        # publishing message
        await publishing_result(
            client_redis,
            channel,
            message,
        )

    except Exception as error:

        await error_handling.parse_error_message_with_redis(
            client_redis,
            error,
        )


async def publishing_result(
    client_redis: object,
    message: dict,
) -> None:
    """ """

    try:

        channel = message["params"]["channel"]

        # publishing message
        await client_redis.publish(
            channel,
            orjson.dumps(message),
        )

    except Exception as error:

        await error_handling.parse_error_message_with_redis(
            client_redis,
            error,
        )


async def saving_result(
    client_redis: object,
    channel: str,
    keys: str,
    cached_data: list,
) -> None:
    """ """

    try:

        channel = message["channel"]

        await client_redis.hset(
            keys,
            channel,
            orjson.dumps(cached_data),
        )

    except Exception as error:

        await error_handling.parse_error_message_with_redis(
            client_redis,
            error,
        )


async def querying_data(
    client_redis: object,
    channel: str,
    keys: str,
) -> None:
    """ """

    try:

        return await client_redis.hget(
            keys,
            channel,
        )

    except Exception as error:

        await error_handling.parse_error_message_with_redis(
            client_redis,
            error,
        )


async def publishing_specific_purposes(
    purpose,
    message,
    redis_channels: list = None,
    client_redis: object = None,
) -> None:
    """
    purposes:
    + porfolio
    + sub_account_update
    + trading_update

        my_trades_channel:
        + send messages that "high probabilities" trade DB has changed
            sender: redis publisher + sqlite insert, update & delete
        + updating trading cache at end user
            consumer: fut spread, hedging, cancelling
        + checking data integrity
            consumer: app data cleaning/size reconciliation

        sub_account_channel:
        + send messages that sub_account has changed
            sender: deribit API module
        + updating sub account cache at end user
            consumer: fut spread, hedging, cancelling
        + checking data integrity
            consumer: app data cleaning/size reconciliation

    """

    try:

        if not client_redis:
            pool = aioredis.ConnectionPool.from_url(
                "redis://localhost",
                port=6379,
                db=0,
                protocol=3,
                decode_responses=True,
            )
            client_redis: object = aioredis.Redis.from_pool(pool)

        if not redis_channels:

            from streaming_helper.utilities.system_tools import get_config_tomli

            # registering strategy config file
            file_toml = "config_strategies.toml"

            # parsing config file
            config_app = get_config_tomli(file_toml)

            # get redis channels
            redis_channels: dict = config_app["redis_channels"][0]

        if purpose == "sqlite_record_updating":
            channel: str = redis_channels["sqlite_record_updating"]
            message["params"].update({"channel": channel})

        await publishing_result(
            client_redis,
            message,
        )

    except Exception as error:

        await error_handling.parse_error_message_with_redis(
            client_redis,
            error,
        )
