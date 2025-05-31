import aioredis
import json
import logging
from .config_utils import get_env  # Updated import

async def get_redis_pool() -> aioredis.Redis:
    """Create Redis connection pool"""
    return aioredis.from_url(
        f"redis://{get_env('REDIS_HOST', 'localhost')}:{get_env('REDIS_PORT', 6379)}",
        decode_responses=False,
        max_connections=20
    )

class RedisPublisher:
    def __init__(self, stream_name: str, max_queue_size: int):
        self.stream_name = stream_name
        self.max_queue_size = max_queue_size
        self.redis_pool = None
        self.logger = logging.getLogger("redis_publisher")

    async def connect(self):
        """Establish Redis connection"""
        self.redis_pool = await get_redis_pool()

    async def publish(self, data: dict):
        """Publish data to Redis stream"""
        if not self.redis_pool:
            await self.connect()
        
        try:
            await self.redis_pool.xadd(
                self.stream_name,
                {"data": json.dumps(data).encode()},
                maxlen=self.max_queue_size
            )
        except Exception as e:
            self.logger.error(f"Redis publish error: {str(e)}")
            await self.reconnect()

    async def reconnect(self):
        """Reconnect to Redis"""
        if self.redis_pool:
            await self.redis_pool.close()
        self.redis_pool = None
        await self.connect()