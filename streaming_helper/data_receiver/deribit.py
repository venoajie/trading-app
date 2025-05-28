"""
Deribit WebSocket receiver implementation
Handles real-time market data and account updates
"""

import asyncio
import json
import orjson
from datetime import datetime, timedelta, timezone
from dataclassy import dataclass
import websockets
from loguru import logger as log

from shared.streaming_helper.utilities import error_handling, system_tools
from shared.streaming_helper.restful_api.deribit import end_point_params_template

@dataclass(unsafe_hash=True, slots=True)
class StreamingAccountData:
    """Manages WebSocket connection and data streaming from Deribit"""
    
    sub_account_id: str
    client_id: str
    client_secret: str
    ws_connection_url: str = "wss://www.deribit.com/ws/api/v2"
    websocket_client: websockets.WebSocketClientProtocol = None
    refresh_token: str = None
    refresh_token_expiry_time: int = None

    async def ws_manager(
        self,
        client_redis: object,
        exchange: str,
        queue_general: asyncio.Queue,
        futures_instruments: list,
        resolutions: list
    ) -> None:
        """Main WebSocket management coroutine"""
        log.info("Initializing WebSocket connection")
        
        while True:
            try:
                async with websockets.connect(
                    self.ws_connection_url,
                    ping_interval=30,
                    close_timeout=60,
                    max_size=2**25  # 32MB
                ) as self.websocket_client:
                    log.success("WebSocket connection established")
                    
                    try:
                        # Authentication and subscription setup
                        await self._authenticate(client_redis)
                        await self._setup_heartbeat(client_redis)
                        await self._subscribe_to_channels(
                            futures_instruments,
                            resolutions
                        )
                        
                        # Start authentication refresh task
                        asyncio.create_task(self.ws_refresh_auth())
                        
                        # Main data receiving loop
                        await self._receive_messages(
                            client_redis,
                            exchange,
                            queue_general
                        )
                        
                    except websockets.ConnectionClosed:
                        log.warning("WebSocket connection closed unexpectedly")
                    except Exception as e:
                        await error_handling.parse_error_message_with_redis(
                            client_redis,
                            e
                        )
                        raise
            except Exception as e:
                log.error(f"Connection error: {e}")
                await asyncio.sleep(5)  # Reconnect delay

    async def _authenticate(self, client_redis: object) -> None:
        """Authenticate WebSocket connection"""
        log.info("Authenticating WebSocket connection")
        msg = {
            "jsonrpc": "2.0",
            "id": 9929,
            "method": "public/auth",
            "params": {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret
            }
        }
        await self.websocket_client.send(json.dumps(msg))
        response = await self.websocket_client.recv()
        result = orjson.loads(response)
        self.refresh_token = result["result"]["refresh_token"]
        self._set_token_expiry(result["result"]["expires_in"])

    async def _setup_heartbeat(self, client_redis: object) -> None:
        """Set up heartbeat mechanism"""
        log.info("Setting up heartbeat")
        msg = {
            "jsonrpc": "2.0",
            "id": 9098,
            "method": "public/set_heartbeat",
            "params": {"interval": 10}
        }
        await self.websocket_client.send(json.dumps(msg))

    async def _subscribe_to_channels(
        self,
        futures_instruments: list,
        resolutions: list
    ) -> None:
        """Subscribe to market data channels"""
        log.info("Subscribing to data channels")
        instruments_name = futures_instruments["instruments_name"]
        channels = [
            f"user.changes.future.any.raw",
            f"user.changes.future_combo.any.raw",
            "user.orders.any.any.raw",
            "user.trades.any.any.raw"
        ]
        
        for instrument in instruments_name:
            currency = system_tools.extract_currency_from_text(instrument)
            if "PERPETUAL" in instrument:
                channels.append(f"user.portfolio.{currency}")
                for resolution in resolutions:
                    channels.append(f"chart.trades.{instrument}.{resolution}")
            channels.append(f"incremental_ticker.{instrument}")
        
        await self.ws_operation("subscribe", channels)

    async def _receive_messages(
        self,
        client_redis: object,
        exchange: str,
        queue_general: asyncio.Queue
    ) -> None:
        """Receive and process incoming messages"""
        log.info("Starting message processing loop")
        while True:
            try:
                message = await self.websocket_client.recv()
                message_data = orjson.loads(message)
                await self._process_message(
                    message_data,
                    client_redis,
                    exchange,
                    queue_general
                )
            except Exception as e:
                await error_handling.parse_error_message_with_redis(
                    client_redis,
                    e
                )
                raise

    async def _process_message(
        self,
        message_data: dict,
        client_redis: object,
        exchange: str,
        queue_general: asyncio.Queue
    ) -> None:
        """Process incoming WebSocket message"""
        if "id" in message_data:
            await self._handle_response(message_data)
        elif "method" in message_data and message_data["method"] == "heartbeat":
            await self.heartbeat_response(client_redis)
        elif "params" in message_data:
            await self._handle_data_message(
                message_data,
                exchange,
                queue_general
            )

    async def _handle_response(self, response: dict) -> None:
        """Handle response messages"""
        if response["id"] == 9929:  # Authentication response
            self.refresh_token = response["result"]["refresh_token"]
            self._set_token_expiry(response["result"]["expires_in"])
            log.success("Authentication refreshed")
        elif response["id"] == 9098:  # Heartbeat setup response
            log.debug("Heartbeat configured")

    async def _handle_data_message(
        self,
        message: dict,
        exchange: str,
        queue_general: asyncio.Queue
    ) -> None:
        """Handle market data messages"""
        message_params = message["params"]
        message_params.update({
            "exchange": exchange,
            "account_id": self.sub_account_id
        })
        await queue_general.put(message_params)

    async def heartbeat_response(self, client_redis: object) -> None:
        """Respond to heartbeat request"""
        msg = {
            "jsonrpc": "2.0",
            "id": 8212,
            "method": "public/test",
            "params": {}
        }
        try:
            await self.websocket_client.send(json.dumps(msg))
        except Exception as e:
            await error_handling.parse_error_message_with_redis(client_redis, e)

    async def ws_refresh_auth(self) -> None:
        """Refresh authentication token periodically"""
        while True:
            await asyncio.sleep(150)  # Refresh every 2.5 minutes
            if datetime.now(timezone.utc) > self.refresh_token_expiry_time:
                msg = {
                    "jsonrpc": "2.0",
                    "id": 9929,
                    "method": "public/auth",
                    "params": {
                        "grant_type": "refresh_token",
                        "refresh_token": self.refresh_token
                    }
                }
                try:
                    await self.websocket_client.send(json.dumps(msg))
                except Exception:
                    log.warning("Failed to refresh authentication")

    async def ws_operation(
        self,
        operation: str,
        channels: list
    ) -> None:
        """Perform subscribe/unsubscribe operations"""
        msg = {
            "jsonrpc": "2.0",
            "id": end_point_params_template.id_numbering(operation, channels),
            "method": f"private/{operation}",
            "params": {"channels": channels}
        }
        await self.websocket_client.send(json.dumps(msg))

    def _set_token_expiry(self, expires_in: int) -> None:
        """Set token expiry time with buffer"""
        buffer_seconds = 240  # 4 minutes buffer
        self.refresh_token_expiry_time = datetime.now(timezone.utc) + timedelta(
            seconds=expires_in - buffer_seconds
        )
        log.debug(f"Token expires at: {self.refresh_token_expiry_time}")