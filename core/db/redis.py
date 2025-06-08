# core/db/redis.py
"""Consolidated Redis client with connection pooling and stream management"""

import logging
import orjson
import redis.asyncio as aioredis
from typing import Any, Dict, Optional, Union

from src.shared.config.settings import REDIS_URL, REDIS_DB
from src.shared.utils import error_handling

# Configure logger
log = logging.getLogger(__name__)

class RedisClient:
    """Singleton Redis client with connection pooling and stream operations"""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.pool = None
        return cls._instance
    
    async def get_pool(self) -> aioredis.Redis:
        """Get or create Redis connection pool"""
        if self.pool is None:
            self.pool = aioredis.from_url(
                REDIS_URL,
                db=REDIS_DB,
                encoding="utf-8",
                decode_responses=False,  # Keep binary for performance
                socket_connect_timeout=5,
                socket_keepalive=True,
                max_connections=50
            )
            log.info(f"Created Redis pool for {REDIS_URL}")
        return self.pool

    # ... (other methods remain mostly the same) ...

    async def xadd(
        self,
        stream_name: str,
        data: dict,
        maxlen: int = 1000  # Changed parameter name to match Redis API
    ) -> None:
        """
        Add entry to Redis stream with trimming capability
        
        Args:
            stream_name: Name of Redis stream
            data: Dictionary of data to add
            maxlen: Maximum stream length (trims old entries)
        """
        pool = await self.get_pool()
        # Use msgpack for more efficient serialization
        await pool.xadd(
            stream_name,
            {"data": orjson.dumps(data)},
            maxlen=maxlen,
            approximate=True  # More efficient trimming
        )

    async def ensure_consumer_group(
        self,
        stream_name: str,
        group_name: str,
        create_stream: bool = True
    ) -> None:
        """
        Ensure consumer group exists with proper error handling
        
        Args:
            stream_name: Redis stream name
            group_name: Consumer group name
            create_stream: Create stream if doesn't exist
        """
        pool = await self.get_pool()
        try:
            await pool.xgroup_create(
                name=stream_name,
                groupname=group_name,
                id="0",  # Start from beginning
                mkstream=create_stream
            )
            log.info(f"Created consumer group {group_name} for {stream_name}")
        except aioredis.ResponseError as e:
            if "BUSYGROUP" in str(e):
                log.debug(f"Consumer group {group_name} already exists")
            else:
                log.error(f"Error creating consumer group: {str(e)}")
                raise

    async def read_stream_messages(
        self,
        stream_name: str,
        group_name: str,
        consumer_name: str,
        count: int = 100,
        block: int = 5000
    ) -> list:
        """
        Read messages from stream with consumer group
        
        Args:
            stream_name: Redis stream name
            group_name: Consumer group name
            consumer_name: Consumer identifier
            count: Max messages per read
            block: Blocking time in ms
            
        Returns:
            List of (message_id, message_data) tuples
        """
        pool = await self.get_pool()
        try:
            messages = await pool.xreadgroup(
                groupname=group_name,
                consumername=consumer_name,
                streams={stream_name: ">"},  # Deliver never-seen messages
                count=count,
                block=block
            )
            return messages[0][1] if messages else []  # (stream, [(id, data)])
        except aioredis.ResponseError as e:
            if "NOGROUP" in str(e):
                log.warning("Consumer group missing, recreating...")
                await self.ensure_consumer_group(stream_name, group_name)
                return []
            raise

    async def acknowledge_message(
        self,
        stream_name: str,
        group_name: str,
        message_id: str
    ) -> None:
        """Acknowledge successful processing of message"""
        pool = await self.get_pool()
        await pool.xack(stream_name, group_name, message_id)

    async def trim_stream(
        self,
        stream_name: str,
        maxlen: int = 1000
    ) -> None:
        """Trim stream to prevent excessive memory usage"""
        pool = await self.get_pool()
        await pool.xtrim(stream_name, maxlen=maxlen, approximate=True)

async def stream_health_check():
    return {
        "length": await client_redis.xlen("stream:market_data"),
        "pending": await client_redis.xpending("stream:market_data", "dispatcher_group"),
        "consumers": await client_redis.xinfo_consumers("stream:market_data", "dispatcher_group")
    }
    
async def saving_and_publishing_result(
    client_redis: object,
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
                keys,
                cached_data,
            )

        # publishing message
        await publishing_result(
            client_redis,
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
        
        print(f"publishing_result message {message}")

        await error_handling.parse_error_message_with_redis(
            client_redis,
            error,
        )


async def saving_result(
    client_redis: object,
    keys: str,
    cached_data: list,
) -> None:
    """ """

    try:
        
        channel = cached_data["params"]["channel"]
        
        await client_redis.hset(
            keys,
            channel,
            orjson.dumps(cached_data),
        )

    except Exception as error:
        
        print(f"saving_result cached_data {cached_data}")

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
            client_redis = aioredis.Redis(
                host="localhost",
                port=6379,
                db=0,
                decode_responses=True
            )

        if not redis_channels:

            from src.shared.utils.system_tools import get_config_tomli

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

# Global Redis client instance
redis_client = RedisClient()

