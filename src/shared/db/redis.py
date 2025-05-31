""" 
shared/db/redis.py 
Consolidated Redis client with connection pooling 
""" 

import logging 
import orjson 
import redis.asyncio as aioredis 
from typing import Any, Dict, Optional, Union

from shared.config.settings import REDIS_URL, REDIS_DB 

# Configure logger 
log = logging.getLogger(__name__) 

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