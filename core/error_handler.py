# error_handler.py (centralized)
import traceback
from core.db.redis import RedisClient

async def handle_error(error, context=""):
    error_data = {
        "error": str(error),
        "context": context,
        "traceback": traceback.format_exc()
    }
    await RedisClient.publish("system_errors", error_data)