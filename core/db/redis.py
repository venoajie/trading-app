# core\db\redis.py

""" 
core/db/redis.py 
Consolidated Redis client with connection pooling 
""" 

import logging 
import orjson 
import redis.asyncio as aioredis 
from typing import Any, Dict, Optional, Union

from src.shared.config.settings import REDIS_URL, REDIS_DB 
from src.shared.utils import error_handling

# Configure logger 
log = logging.getLogger(__name__) 


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

            from shared.utils.system_tools import get_config_tomli

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

class RedisClient: 
    """Singleton Redis client with connection pooling""" 
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
                decode_responses=True, 
                socket_connect_timeout=5, 
                socket_keepalive=True, 
                max_connections=50 
            ) 
            log.info(f"Created Redis pool for {REDIS_URL}") 
        return self.pool 

    async def publish(self, channel: str, message: Union[Dict, str]) -> None: 
        """Publish message to Redis channel""" 
        pool = await self.get_pool() 
        if isinstance(message, dict): 
            message = orjson.dumps(message).decode("utf-8") 
        await pool.publish(channel, message) 

    async def save_to_hash(
        self,
        hash_key: str,
        field: str,
        data: Any
    ) -> None:
        """Save data to Redis hash field"""
        pool = await self.get_pool()
        if not isinstance(data, (str, bytes)):
            data = orjson.dumps(data).decode("utf-8")
        await pool.hset(hash_key, field, data)

    async def get_from_hash(
        self,
        hash_key: str,
        field: str
    ) -> Optional[Any]:
        """Retrieve data from Redis hash field"""
        pool = await self.get_pool()
        data = await pool.hget(hash_key, field)
        if data:
            try:
                return orjson.loads(data)
            except orjson.JSONDecodeError:
                return data
        return None

    async def xadd(
        self,
        stream_name: str,
        data: dict,
        max_queue_size: int = 1000
    ) -> None:
        """Add to Redis stream with queue size limit"""
        pool = await self.get_pool()
        await pool.xadd(
            stream_name,
            {"data": orjson.dumps(data).decode("utf-8")},
            maxlen=max_queue_size
        )

# Global Redis client instance 
redis_client = RedisClient()