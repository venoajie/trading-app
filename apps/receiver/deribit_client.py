import asyncio
import websockets
import json
from typing import AsyncIterable

class DeribitClient:
    def __init__(self, ws_url: str, channels: list):
        self.ws_url = ws_url
        self.channels = channels
        self.websocket = None

    async def connect(self):
        """Establish WebSocket connection"""
        self.websocket = await websockets.connect(
            self.ws_url,
            ping_interval=30,
            close_timeout=60
        )
        await self._subscribe()

    async def _subscribe(self):
        """Subscribe to configured channels"""
        subscribe_msg = {
            "jsonrpc": "2.0",
            "method": "public/subscribe",
            "id": 42,
            "params": {
                "channels": self.channels
            }
        }
        await self.websocket.send(json.dumps(subscribe_msg))

    async def receive_messages(self) -> AsyncIterable[dict]:
        """Yield incoming messages from WebSocket"""
        while self.websocket.open:
            try:
                message = await self.websocket.recv()
                yield json.loads(message)
            except websockets.ConnectionClosed:
                await self.reconnect()

    async def reconnect(self):
        """Reconnect with exponential backoff"""
        retry_delay = 1
        while True:
            try:
                await self.connect()
                return
            except ConnectionRefusedError:
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)