# src\services\receiver\deribit\deribit_ws.py

"""
WebSocket client for Deribit exchange with enhanced maintenance handling
"""

import asyncio
from collections import deque
from datetime import datetime, timedelta, timezone
import json
import sys
import time

# Third-party imports
import orjson
from dataclassy import dataclass
import websockets
from websockets import WebSocketClientProtocol
from typing import Any, Dict, List, Optional, Union, cast
from loguru import logger as log

# Application imports
from core.error_handler import error_handler
from src.scripts.deribit.restful_api import end_point_params_template
from src.shared.utils import string_modification as str_mod
from src.shared.config.constants import (
    ServiceConstants,
    WebsocketParameters,
    AddressUrl,
)

@dataclass(unsafe_hash=True, slots=True)
class StreamingAccountData:
    """Enhanced WebSocket manager with maintenance detection and recovery"""

    sub_account_id: str
    client_id: str
    client_secret: str
    reconnect_base_delay: int = WebsocketParameters.RECONNECT_BASE_DELAY
    max_reconnect_delay: int = WebsocketParameters.MAX_RECONNECT_DELAY
    maintenance_threshold: int = WebsocketParameters.MAINTENANCE_THRESHOLD
    websocket_timeout: int = WebsocketParameters.WEBSOCKET_TIMEOUT
    heartbeat_interval: int = WebsocketParameters.HEARTBEAT_INTERVAL
    loop: asyncio.AbstractEventLoop = cast(asyncio.AbstractEventLoop, None)
    ws_connection_url: str = AddressUrl.DERIBIT_WS
    websocket_client: Optional[WebSocketClientProtocol] = None
    refresh_token: Optional[str] = None
    refresh_token_expiry_time: Optional[datetime] = None
    last_message_time: float = 0.0
    reconnect_attempts: int = 0
    connection_active: bool = False
    maintenance_mode: bool = False
    refresh_task: Optional[asyncio.Task] = None
    heartbeat_task: Optional[asyncio.Task] = None

    def __post_init__(self):
        """Initialize event loop reference"""
        self.loop = asyncio.get_event_loop()

    async def ws_refresh_auth(self) -> None:
        """Refresh authentication token periodically"""
        while self.connection_active:
            try:
                if not self.refresh_token_expiry_time:
                    await asyncio.sleep(30)
                    continue

                now_utc = datetime.now(timezone.utc)
                if now_utc >= self.refresh_token_expiry_time:
                    if not self.websocket_client:
                        log.warning("Skipping refresh - WebSocket not connected")
                        await asyncio.sleep(30)
                        continue

                    msg = {
                        "jsonrpc": "2.0",
                        "id": 9929,
                        "method": "public/auth",
                        "params": {
                            "grant_type": "refresh_token",
                            "refresh_token": self.refresh_token,
                        },
                    }
                    await self.websocket_client.send(json.dumps(msg))
                    log.debug("Authentication refresh sent")

                # Check every 30 seconds
                await asyncio.sleep(30)
            except Exception as e:
                log.error(f"Error in auth refresh: {e}")
                await asyncio.sleep(60)

    async def ws_auth(self, client_redis: Any) -> None:
        """Authenticate WebSocket connection"""
        if not self.websocket_client:
            log.error("Cannot authenticate - WebSocket not connected")
            return

        msg = {
            "jsonrpc": "2.0",
            "id": 9929,
            "method": "public/auth",
            "params": {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
        }

        try:
            await self.websocket_client.send(json.dumps(msg))
        except Exception as error:
            log.error(f"Authentication failed: {error}")
            await error_handling.parse_error_message_with_redis(client_redis, error)

    async def establish_heartbeat(self, client_redis: Any) -> None:
        """Establish heartbeat with Deribit"""
        if not self.websocket_client:
            log.error("Cannot establish heartbeat - WebSocket not connected")
            return

        msg = {
            "jsonrpc": "2.0",
            "id": 0,
            "method": "public/set_heartbeat",
            "params": {"interval": 30},
        }

        try:
            await self.websocket_client.send(json.dumps(msg))
        except Exception as error:
            log.error(f"Heartbeat setup failed: {error}")
            await error_handling.parse_error_message_with_redis(client_redis, error)

    async def authenticate_and_setup(
        self,
        client_redis: Any,
        exchange: str,
        futures_instruments: Dict[str, Any],
        resolutions: List[str],
    ) -> None:
        """Authenticate and setup subscriptions"""
        # Authenticate WebSocket Connection
        await self.ws_auth(client_redis)

        # Establish Heartbeat
        await self.establish_heartbeat(client_redis)

        # Prepare and subscribe to instruments
        instruments_name = futures_instruments["instruments_name"]
        ws_instruments = self.generate_subscription_list(instruments_name, resolutions)

        await self.ws_operation(
            operation="subscribe",
            ws_channel=ws_instruments,
            source="ws-combination",
        )

    async def cancel_background_tasks(self) -> None:
        """Safely cancel background tasks"""
        for task in [self.heartbeat_task, self.refresh_task]:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    log.debug("Background task cancelled")
                except Exception as e:
                    log.error(f"Error cancelling task: {e}")

        # Reset task references
        self.heartbeat_task = None
        self.refresh_task = None

    async def monitor_heartbeat(self, client_redis: Any) -> None:
        """Monitor connection health with maintenance detection"""
        while self.connection_active:
            await asyncio.sleep(self.heartbeat_interval)  # Use instance variable
            time_since_last = time.time() - self.last_message_time

            # Detect extended silence (possible maintenance)
            if time_since_last > self.maintenance_threshold:
                alert = {
                    "component": "deribit_ws",
                    "event": "heartbeat_timeout",
                    "reason": f"No messages for {time_since_last:.0f} seconds",
                }
                await client_redis.publish("system_alerts", orjson.dumps(alert))

            # Normal timeout handling
            elif time_since_last > self.websocket_timeout:  # Use instance variable
                log.warning(
                    f"No messages for {time_since_last:.0f} seconds. Reconnecting..."
                )
                if self.websocket_client:
                    await self.websocket_client.close()
                break

    async def handle_reconnect(self) -> None:
        """Handle reconnection with exponential backoff
        jitter to reconnect timing
        """
        import random

        self.reconnect_attempts += 1
        base_delay = min(
            self.reconnect_base_delay * (2**self.reconnect_attempts),
            self.max_reconnect_delay,
        )

        # Add jitter (up to 50% of base delay)
        jitter = base_delay * 0.5 * random.random()
        delay = min(base_delay + jitter, self.max_reconnect_delay)

        log.info(
            f"Reconnecting attempt {self.reconnect_attempts} in {delay:.1f} seconds..."
        )
        await asyncio.sleep(delay)

    async def process_messages(self, client_redis, exchange):
        """Process incoming messages with state recovery"""
        
        
        STREAM_NAME = ServiceConstants.REDIS_STREAM_MARKET
        last_flush_time = time.time()  # Initialize flush timer
        MAX_BATCH_ITEMS = 5000  # Hard limit of 5000 messages
        BATCH_SIZE = 50
        batch = []
        
        try:
            async for message in self.websocket_client:
                current_time = time.time()
                # Calculate message size (accurate for strings/bytes)

                current_batch_size += msg_size                
                self.last_message_time = current_time

                try:
                    message_dict = orjson.loads(message)

                    # Handle heartbeat notifications
                    if message_dict.get("method") == "heartbeat":
                        if message_dict.get("params", {}).get("type") == "test_request":
                            log.info("Responding to test_request heartbeat")
                            await self.test_request_response()
                        continue

                    # Handle authentication responses
                    if message_dict.get("id") == 9929:
                        self.handle_auth_response(message_dict)
                        continue

                    # Handle heartbeat setup responses
                    if message_dict.get("id") == 0:
                        if message_dict.get("result") == "ok":
                            log.info("Heartbeat established successfully")
                        continue

                    # Process data messages
                    if "params" in message_dict and "channel" in message_dict["params"]:
                        channel = message_dict["params"]["channel"]
                        data = message_dict["params"]["data"]
                        serialized_data = orjson.dumps(data).decode("utf-8")
                        timestamp = str(int(current_time * 1000))

                        # Add to batch
                        batch.append({
                            "channel": channel,
                            "data": serialized_data,
                            "timestamp": timestamp,
                            "exchange": exchange,
                        })
                        
                except Exception as e:
                    log.error(f"Message processing failed: {e}")
                
                # Check if we should send batch (size or time-based)
                max_batch_age = 10  # Max seconds to hold batch
                delta_flush_time = current_time - last_flush_time
                

                # Cap batch size
                if len(batch) > MAX_BATCH_ITEMS:
                    keep_count = len(batch) // 2
                    batch = batch[-keep_count:]
                    log.warning(f"Batch overflow - trimmed to {keep_count} messages")
                
                # Check if we should send batch
                should_send = (
                    len(batch) >= BATCH_SIZE or 
                    (batch and delta_flush_time > 5) or
                    (batch and delta_flush_time > max_batch_age)
                )
                
                if should_send:
                    send_success = False
                    max_retries = 5
                    
                    for attempt in range(max_retries):
                        try:
                            await client_redis.xadd_bulk(STREAM_NAME, batch)
                            batch = []
                            last_flush_time = current_time
                            send_success = True
                            break
                        except (ConnectionError, TimeoutError) as e:
                            log.warning(f"Redis error ({attempt+1}/{max_retries}): {e}")
                            await asyncio.sleep(min(2 ** attempt, 10))
                        except Exception as e:
                            log.error(f"Redis batch send failed: {e}")
                            break
                            
                    if not send_success:
                        log.error(f"Failed to send batch after {max_retries} attempts")                            
        finally:
            # Send any remaining messages on disconnect
            if batch:
                try:
                    log.debug(f"Sending final batch of {len(batch)} messages")
                    await client_redis.xadd_bulk(STREAM_NAME, batch)
                except Exception as e:
                    log.error(f"Failed to send final batch: {e}")

    async def manage_connection(
        self,
        client_redis: Any,
        exchange: str,
        futures_instruments: Dict[str, Any],
        resolutions: List[str],
    ) -> None:
        """Main connection loop with maintenance detection"""
        while True:
            try:
                # Reset state for new connection
                self.connection_active = True
                self.reconnect_attempts = 0
                self.maintenance_mode = False

                async with websockets.connect(
                    self.ws_connection_url,
                    ping_interval=None,
                    ping_timeout=None,
                    close_timeout=60,
                    compression=None,
                ) as self.websocket_client:
                    log.info("WebSocket connection established")
                    self.last_message_time = time.time()

                    # Create background tasks
                    self.heartbeat_task = asyncio.create_task(
                        self.monitor_heartbeat(client_redis)
                    )
                    self.refresh_task = asyncio.create_task(self.ws_refresh_auth())

                    # Setup subscriptions
                    await self.authenticate_and_setup(
                        client_redis, exchange, futures_instruments, resolutions
                    )

                    # Process incoming messages
                    await self.process_messages(client_redis, exchange)

            except (websockets.ConnectionClosed, ConnectionError) as e:
                log.warning(f"Connection closed: {e}")
            except Exception as e:
                log.error(f"Unexpected connection error: {e}")
            finally:
                self.connection_active = False
                await self.cancel_background_tasks()
                await self.handle_reconnect()

    def handle_auth_response(self, message: Dict) -> None:
        """Handle authentication responses"""
        try:
            result = message["result"]
            self.refresh_token = result["refresh_token"]

            # Calculate token expiration time
            expires_in = (
                300 if message.get("testnet", False) else result["expires_in"] - 240
            )
            now_utc = datetime.now(timezone.utc)
            self.refresh_token_expiry_time = now_utc + timedelta(seconds=expires_in)

            if not self.refresh_token:
                log.info("WebSocket authentication successful")
            else:
                log.info("Authentication refreshed successfully")

        except KeyError as e:
            log.error(f"Missing key in auth response: {e}")

    async def test_request_response(self) -> None:
        """Respond to Deribit test_request messages"""

        if not self.websocket_client:
            return

        response = {"jsonrpc": "2.0", "id": 0, "method": "public/test", "params": {}}
        try:
            await self.websocket_client.send(json.dumps(response))
            log.debug("Responded to test_request")
        except Exception as error:
            log.error(f"Test request response failed: {error}")

    def generate_subscription_list(
        self, instruments_name: List[str], resolutions: List[str]
    ) -> List[str]:
        """Generate list of channels to subscribe to"""
        ws_instruments = []

        # Add account-related channels
        instrument_kinds = ["future", "future_combo"]
        for kind in instrument_kinds:
            ws_instruments.append(f"user.changes.{kind}.any.raw")

        ws_instruments.extend(["user.orders.any.any.raw", "user.trades.any.any.raw"])

        # Add instrument-specific channels
        for instrument in instruments_name:
            if "PERPETUAL" in instrument:
                currency = str_mod.extract_currency_from_text(instrument)
                ws_instruments.append(f"user.portfolio.{currency}")

                # Add chart subscriptions for all resolutions
                for resolution in resolutions:
                    ws_instruments.append(f"chart.trades.{instrument}.{resolution}")

            # Add ticker subscription for all instruments
            ws_instruments.append(f"incremental_ticker.{instrument}")

        return ws_instruments

    async def ws_operation(
        self,
        operation: str,
        ws_channel: List[str],
        source: str = "ws-single",
    ) -> None:
        """
        Subscribe or unsubscribe to WebSocket channels

        Args:
            operation: 'subscribe' or 'unsubscribe'
            ws_channel: List of channels to operate on
            source: Source of operation (ws-single/ws-combination/rest)
        """
        if not self.websocket_client:
            log.error(f"Cannot {operation} - WebSocket not connected")
            return

        await asyncio.sleep(0.05)  # Small delay to prevent flooding

        id = end_point_params_template.id_numbering(operation, ws_channel)

        msg = {
            "jsonrpc": "2.0",
            "id": id,
            "method": f"private/{operation}",
            "params": {"channels": ws_channel},
        }

        try:
            await self.websocket_client.send(json.dumps(msg))
            log.debug(f"Sent {operation} for {len(ws_channel)} channels")
        except Exception as e:
            log.error(f"Error in {operation} operation: {e}")
