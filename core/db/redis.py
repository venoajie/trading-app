# core\db\redis.py

"""
Consolidated Redis client with connection pooling
"""

import os

import logging
import orjson
import redis.asyncio as aioredis
from typing import Any, Dict, List, Optional, Union

from src.shared.config.config import config

# Configure logger
log = logging.getLogger(__name__)


async def stream_health_check():
    return {
        "length": await client_redis.xlen("stream:market_data"),
        "pending": await client_redis.xpending(
            "stream:market_data", "dispatcher_group"
        ),
        "consumers": await client_redis.xinfo_consumers(
            "stream:market_data", "dispatcher_group"
        ),
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
                host="localhost", port=6379, db=0, decode_responses=True
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


class CustomRedisClient:
    """Singleton Redis client with connection pooling"""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.pool = None
            # Initialize circuit breaker state
            cls._instance._circuit_open = False
            cls._instance._last_failure = 0
        return cls._instance

    async def get_pool(self) -> aioredis.Redis:
        """Get or create Redis connection pool"""

        # Circuit breaker logic (5-second cooldown)
        if self._circuit_open and time.time() - self._last_failure < 5:
            raise ConnectionError("Redis circuit breaker open")
                
        if self.pool is None:

            redis_url = config["redis"]["url"]
            redis_db = config["redis"]["db"]

        try:
            
            if self.pool is None or self.pool._closed:

                self.pool = aioredis.from_url(
                    redis_url,
                    db=redis_db,
                    encoding="utf-8",
                    decode_responses=False,  # Keep binary for performance
                    socket_connect_timeout=5,
                    socket_keepalive=True,
                    max_connections=50,
                )
                log.info(f"Created Redis pool for {redis_url}")
                
            return self.pool

        except Exception as e:
            self._circuit_open = True
            self._last_failure = time.time()
            raise
        
    async def publish(self, channel: str, message: Union[Dict, str]) -> None:
        """Publish message to Redis channel"""
        pool = await self.get_pool()
        if isinstance(message, dict):
            message = orjson.dumps(message).decode("utf-8")
        await pool.publish(channel, message)

    async def save_to_hash(self, hash_key: str, field: str, data: Any) -> None:
        """Save data to Redis hash field"""
        pool = await self.get_pool()
        if not isinstance(data, (str, bytes)):
            data = orjson.dumps(data).decode("utf-8")
        await pool.hset(hash_key, field, data)

    async def get_from_hash(self, hash_key: str, field: str) -> Optional[Any]:
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
        maxlen: int = 1000,  # Changed parameter name to match Redis API
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
            data,  # Pass directly as field-value pairs
            maxlen=maxlen,
            approximate=True,  # More efficient trimming
        )

    async def xreadgroup(
        self,
        group_name: str,
        consumer_name: str,
        stream_name: str,
        count: int = 10,
        block: int = 5000,
    ) -> list:
        pool = await self.get_pool()
        return await pool.xreadgroup(
            groupname=group_name,
            consumername=consumer_name,
            streams={stream_name: ">"},
            count=count,
            block=block,
        )

    @staticmethod
    def encode_stream_message(message: dict) -> dict:
        """Encode message values for Redis stream compatibility"""
        encoded = {}
        for key, value in message.items():
            try:
                if isinstance(value, (dict, list)):
                    encoded[key] = orjson.dumps(value)
                else:
                    encoded[key] = str(value).encode("utf-8")
            except Exception as e:
                log.warning(f"Error encoding field {key}: {e}")
                encoded[key] = b""  # Fallback to empty bytes
        return encoded

    @staticmethod
    def parse_stream_message(message_data: Dict[bytes, bytes]) -> dict:
        """Parse Redis stream message into Python types"""
        result = {}
        for key, value in message_data.items():
            try:
                k = key.decode("utf-8")

                # Handle special fields
                if k == "data":
                    try:
                        result[k] = orjson.loads(value)
                    except orjson.JSONDecodeError:
                        result[k] = value.decode("utf-8")
                else:
                    result[k] = value.decode("utf-8")
            except Exception as e:
                log.warning(f"Error parsing field {key}: {e}")
                result[key] = value  # Keep original value on error
        return result

    async def xadd_bulk(
        self,
        stream_name: str,
        messages: List[dict],
        maxlen: int = 500,
    ) -> None:

        pool = await self.get_pool()
        
        # pre-trim
        await self.trim_stream(stream_name, maxlen)

        try:
            async with pool.pipeline(transaction=False) as pipe:
                for message in messages:
                    # Encode all values to string/bytes
                    encoded_msg = self.encode_stream_message(message)
                    pipe.xadd(stream_name, encoded_msg, maxlen=maxlen, approximate=True)
                await pipe.execute()
            log.debug(f"Sent {len(messages)} messages to {stream_name}")

        except Exception as e:
            log.error(f"Bulk xadd failed: {e}")
            # Implement retry logic or dead-letter queue here

    async def xack(self, stream_name: str, group_name: str, message_id: str) -> None:
        pool = await self.get_pool()
        await pool.xack(stream_name, group_name, message_id)

    async def create_consumer_group(self, stream_name: str, group_name: str) -> None:
        pool = await self.get_pool()
        try:
            await pool.xgroup_create(stream_name, group_name, id="0", mkstream=True)
        except Exception as e:
            if "BUSYGROUP" not in str(e):
                raise

    async def ensure_consumer_group(
        self, stream_name: str, group_name: str, create_stream: bool = True
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
                mkstream=create_stream,
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
        block: int = 5000,
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
                block=block,
            )
            return messages[0][1] if messages else []  # (stream, [(id, data)])
        except aioredis.ResponseError as e:
            if "NOGROUP" in str(e):
                log.warning("Consumer group missing, recreating...")
                await self.ensure_consumer_group(stream_name, group_name)
                return []
            raise

    async def acknowledge_message(
        self, stream_name: str, group_name: str, message_id: str
    ) -> None:
        """Acknowledge successful processing of message"""
        pool = await self.get_pool()
        await pool.xack(stream_name, group_name, message_id)

    async def trim_stream(self, stream_name: str, maxlen: int = 1000) -> None:
        """Trim stream to prevent excessive memory usage"""
        pool = await self.get_pool()
        await pool.xtrim(stream_name, maxlen=maxlen, approximate=True)

    async def publish_error(self, error_data: Dict[str, Any]):
        """Publish errors to Redis channel"""
        message = template.redis_error_template(error_data)
        await self.publish("system_errors", message)


# Global Redis client instance
redis_client = CustomRedisClient()
