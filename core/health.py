# core/health.py

from core.db.postgres import postgres_client
from core.db.redis import redis_client
import psutil


async def health_check():
    # PostgreSQL connections
    pg_stats = {}
    if postgres_client._pool and not postgres_client._pool._closed:
        pg_stats = {
            "connections": postgres_client._pool.get_size(),
            "idle": postgres_client._pool.get_idle_size(),
        }

    # Redis connections
    redis_stats = {}
    try:
        pool = await redis_client.get_pool()
        redis_stats = {"connections": len(pool._connections)}
    except Exception:
        pass

    # Memory diagnostics
    process = psutil.Process()
    mem_info = process.memory_full_info()

    # Stream backlog monitoring
    stream_backlog = 0
    try:
        stream_backlog = await redis_client.xlen("stream:market_data")
    except Exception:
        pass

    return {
        "postgres": pg_stats,
        "redis": redis_stats,
        "memory": {
            "rss_mb": mem_info.rss / 1024 / 1024,
            "uss_mb": mem_info.uss / 1024 / 1024,
            "swap_mb": mem_info.swap / 1024 / 1024,
        },
        "stream_backlog": stream_backlog,
        "connections": {
            "open_files": len(process.open_files()),
            "threads": process.num_threads(),
        },
    }
