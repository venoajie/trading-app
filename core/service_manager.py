import asyncio
import logging
from typing import Dict, Callable, Coroutine, Any


class ServiceManager:
    def __init__(self):
        self.services: Dict[str, Callable[[], Coroutine[Any, Any, None]]] = {}
        self.tasks: Dict[str, asyncio.Task] = {}
        self.shutdown_event = asyncio.Event()

    def register(self, name: str, coro: Callable[[], Coroutine[Any, Any, None]]):
        self.services[name] = coro

    async def start_service(self, name: str):
        """Start a single service with restart logic"""
        while not self.shutdown_event.is_set():
            try:
                await self.services[name]()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"Service {name} failed: {e}. Restarting...")
                await asyncio.sleep(1)

    async def start_all(self):
        """Start all registered services"""
        for name in self.services:
            self.tasks[name] = asyncio.create_task(self.start_service(name))

    async def stop_all(self):
        """Gracefully stop all services"""
        self.shutdown_event.set()
        for task in self.tasks.values():
            task.cancel()
        await asyncio.gather(*self.tasks.values(), return_exceptions=True)


# Global service manager instance
service_manager = ServiceManager()
