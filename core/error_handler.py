# core\error_handler.py

import asyncio
import logging
import traceback
from datetime import datetime
from typing import Any, Callable, Coroutine, Dict, Optional, Type

from src.shared.config.config import config
from src.shared.utils import template

# Centralized logger
logger = logging.getLogger("error_handler")


class ErrorHandler:
    """Centralized error handling with real-time notifications"""

    def __init__(self):
        self.notifiers = []
        self._setup_notifiers()

    def _setup_notifiers(self):
        """Initialize notification channels based on config"""
        telegram_config = config.error_handling.telegram
        if telegram_config.get("bot_token") and telegram_config.get("chat_id"):
            from src.scripts.telegram import connector as telegram
            
            # Extract and convert secrets
            bot_token = telegram_config["bot_token"]
            chat_id = telegram_config["chat_id"]
            
            if isinstance(bot_token, SecretStr):
                bot_token = bot_token.get_secret_value()
            if isinstance(chat_id, SecretStr):
                chat_id = chat_id.get_secret_value()
                
            self.notifiers.append(
                lambda data: telegram.send_message(
                    chat_id,
                    f"🚨 ERROR in {data['service']} ({data['environment']})\n"
                    f"⏰ {data['timestamp']}\n"
                    f"📝 {data['context']}\n"
                    f"💥 {data['message']}\n"
                    f"🔍 {data.get('metadata', '')}",
                    token=bot_token
                )
            )
            
        if config.error_handling.get("notify_redis", True):
            from core.db.redis import redis_client
            self.notifiers.append(redis_client.publish_error)
            
    async def capture(
        self,
        exception: Exception,
        context: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        severity: str = "ERROR",
    ):
        """Capture and process exceptions with notifications"""
        error_data = self._format_error(exception, context, metadata, severity)

        # Log locally
        logger.error(error_data["message"], extra={"data": error_data})

        # Send notifications concurrently
        tasks = [notify(error_data) for notify in self.notifiers]
        await asyncio.gather(*tasks, return_exceptions=True)

    def _format_error(
        self,
        exception: Exception,
        context: Optional[str],
        metadata: Optional[Dict[str, Any]],
        severity: str,
    ) -> Dict[str, Any]:
        """Create structured error data"""
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "severity": severity,
            "type": type(exception).__name__,
            "message": str(exception),
            "context": context or "Unspecified error",
            "stack_trace": traceback.format_exc(),
            "metadata": metadata or {},
            "service": settings.SERVICE_NAME,
            "environment": settings.ENVIRONMENT,
        }

    def wrap_async(self, func: Callable[..., Coroutine]) -> Callable[..., Coroutine]:
        """Decorator for async functions"""

        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                await self.capture(e, f"Async function: {func.__name__}")
                raise

        return wrapper

    def wrap_sync(self, func: Callable) -> Callable:
        """Decorator for sync functions"""

        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                asyncio.run(self.capture(e, f"Sync function: {func.__name__}"))
                raise

        return wrapper


# Global instance
error_handler = ErrorHandler()
