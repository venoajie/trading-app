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
        if config.error_handling.telegram.get("bot_token"):
            from src.scripts.telegram import connector as telegram
            self.notifiers.append(
                lambda data: telegram.send_message(
                    config.error_handling.telegram["chat_id"],
                    data["message"],
                    token=config.error_handling.telegram["bot_token"]
                )
            )
            
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
