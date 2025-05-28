"""
Deribit WebSocket receiver implementation
"""

import asyncio
import json
import orjson
import websockets
from datetime import datetime, timedelta, timezone
from loguru import logger as log
from aioredis import Redis

class DeribitReceiver:
    """Manages WebSocket connection and data streaming from Deribit"""
    
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        sub_account_id: str,
        redis_host: str = "redis"
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.sub_account_id = sub_account_id
        self.redis_host = redis_host
        self.ws_url = "wss://www.deribit.com/ws/api/v2"
        self.websocket = None
        self.redis = None
        self.refresh_token = None
        self.token_expiry = None
    
    async def connect(self):
        """Establish connections and start processing"""
        self.redis = await Redis(self.redis_host)
        while True:
            try:
                async with websockets.connect(self.ws_url) as ws:
                    self.websocket = ws
                    await self.authenticate()
                    await self.subscribe_to_channels()
                    await self.process_messages()
            except Exception as e:
                log.error(f"Connection error: {e}, retrying in 5s")
                await asyncio.sleep(5)
    
    async def disconnect(self):
        """Clean up resources"""
        if self.redis:
            await self.redis.close()
    
    async def authenticate(self):
        """Authenticate with Deribit API"""
        auth_msg = {
            "jsonrpc": "2.0",
            "id": 9929,
            "method": "public/auth",
            "params": {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret
            }
        }
        await self.websocket.send(json.dumps(auth_msg))
        response = await self.websocket.recv()
        result = orjson.loads(response)
        self.refresh_token = result["result"]["refresh_token"]
        self.set_token_expiry(result["result"]["expires_in"])
    
    async def subscribe_to_channels(self):
        """Subscribe to necessary data channels"""
        channels = [
            "user.orders.any.any.raw",
            "user.trades.any.any.raw",
            "user.portfolio.btc",
            "user.portfolio.eth"
        ]
        subscribe_msg = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "private/subscribe",
            "params": {"channels": channels}
        }
        await self.websocket.send(json.dumps(subscribe_msg))
    
    async def process_messages(self):
        """Process incoming WebSocket messages"""
        while True:
            message = await self.websocket.recv()
            data = orjson.loads(message)
            
            if "params" in data:
                await self.redis.publish("market-data", orjson.dumps(data["params"]))
            elif "method" in data and data["method"] == "heartbeat":
                await self.handle_heartbeat()
    
    async def handle_heartbeat(self):
        """Respond to heartbeat requests"""
        response = {
            "jsonrpc": "2.0",
            "id": 8212,
            "method": "public/test",
            "params": {}
        }
        await self.websocket.send(json.dumps(response))
    
    def set_token_expiry(self, expires_in: int):
        """Calculate token expiration time"""
        buffer = 240  # 4 minute buffer
        self.token_expiry = datetime.now(timezone.utc) + timedelta(
            seconds=expires_in - buffer
        )
    
    async def refresh_auth(self):
        """Refresh authentication token"""
        if datetime.now(timezone.utc) > self.token_expiry:
            refresh_msg = {
                "jsonrpc": "2.0",
                "id": 9929,
                "method": "public/auth",
                "params": {
                    "grant_type": "refresh_token",
                    "refresh_token": self.refresh_token
                }
            }
            await self.websocket.send(json.dumps(refresh_msg))