import aioredis
from .config import get_env

async def get_redis_pool() -> aioredis.Redis:
    """Create Redis connection pool"""
    return aioredis.from_url(
        f"redis://{get_env('REDIS_HOST', 'localhost')}:{get_env('REDIS_PORT', 6379)}",
        decode_responses=False,
        max_connections=20
    )