"""
receiver/deribit/deribit_ws.py
Optimized WebSocket client for Deribit exchange with enhanced maintenance handling
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

# Third-party imports
import orjson
import websockets
from dataclassy import dataclass
from websockets import WebSocketClientProtocol

# Application imports
from restful_api.deribit import end_point_params_template
from shared.utils import error_handling, string_modification as str_mod
from shared.config.settings import DERIBIT_CURRENCIES

# Configure logger
log = logging.getLogger(__name__)

@dataclass(unsafe_hash=True, slots=True)
class StreamingAccountData:
    """Optimized WebSocket manager with robust maintenance handling"""
    sub_account_id: str
    client_id: str
    client_secret: str
    reconnect_base_delay: int = 5
    max_reconnect_delay: int = 300
    maintenance_threshold: int = 900
    websocket_timeout: int = 900
    heartbeat_interval: int = 30
    ws_connection_url: str = "wss://www.deribit.com/ws/api/v2"
    
    # Internal state
    _websocket_client: Optional[WebSocketClientProtocol] = None
    _refresh_token: Optional[str] = None
    _refresh_token_expiry_time: Optional[datetime] = None
    _last_message_time: float = 0.0
    _reconnect_attempts: int = 0
    _connection_active: bool = False
    _maintenance_mode: bool = False
    _background_tasks: List[asyncio.Task] = None

    def __post_init__(self):
        """Initialize internal state"""
        self._background_tasks = []
        self._loop = asyncio.get_event_loop()

    @property
    def connection_active(self) -> bool:
        return self._connection_active

    @property
    def maintenance_mode(self) -> bool:
        return self._maintenance_mode

    async def manage_connection(
        self,
        client_redis: Any,
        exchange: str,
        queue_general: asyncio.Queue,
        futures_instruments: Dict[str, Any],
        resolutions: List[int],
    ) -> None:
        """Main connection loop with enhanced maintenance handling"""
        while True:
            try:
                # Reset state for new connection attempt
                self._reset_connection_state()
                
                async with websockets.connect(
                    self.ws_connection_url,
                    ping_interval=20,
                    ping_timeout=60,
                    close_timeout=60,
                ) as self._websocket_client:
                    log.info("WebSocket connection established")
                    self._update_connection_state(True)
                    
                    # Setup background tasks
                    self._start_background_tasks(client_redis)
                    
                    # Initialize connection
                    await self._initialize_connection(
                        client_redis, 
                        exchange, 
                        queue_general,
                        futures_instruments, 
                        resolutions
                    )
                    
                    # Process messages
                    await self._process_messages(client_redis, exchange, queue_general)
                    
            except websockets.ConnectionClosed as e:
                log.warning(f"Connection closed: {e}")
            except Exception as e:
                log.error(f"Unexpected connection error: {e}")
            finally:
                await self._handle_connection_cleanup(client_redis)

    def _reset_connection_state(self):
        """Reset all connection-related state"""
        self._connection_active = False
        self._reconnect_attempts = 0
        self._maintenance_mode = False
        self._last_message_time = 0.0

    def _update_connection_state(self, active: bool):
        """Update connection state and timestamp"""
        self._connection_active = active
        if active:
            self._last_message_time = time.time()

    async def _handle_connection_cleanup(self, client_redis: Any):
        """Clean up connection resources"""
        self._connection_active = False
        await self._cancel_background_tasks()
        
        if self._websocket_client:
            await self._websocket_client.close()
            self._websocket_client = None
            
        await self._handle_reconnect()

    def _start_background_tasks(self, client_redis: Any):
        """Start necessary background tasks"""
        self._background_tasks = [
            asyncio.create_task(self._monitor_heartbeat(client_redis)),
            asyncio.create_task(self._ws_refresh_auth())
        ]

    async def _cancel_background_tasks(self) -> None:
        """Safely cancel all background tasks"""
        for task in self._background_tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    log.debug("Background task cancelled")
                except Exception as e:
                    log.error(f"Error cancelling task: {e}")
        self._background_tasks.clear()

    async def _monitor_heartbeat(self, client_redis: Any) -> None:
        """Enhanced heartbeat monitoring with maintenance detection"""
        while self._connection_active:
            await asyncio.sleep(self.heartbeat_interval)
            
            time_since_last = time.time() - self._last_message_time
            if time_since_last > self.maintenance_threshold:
                if not self._maintenance_mode:
                    log.warning("Exchange maintenance detected")
                    self._maintenance_mode = True
                    await client_redis.publish("system_status", "maintenance")
            elif time_since_last > self.websocket_timeout:
                log.warning(f"No messages for {time_since_last:.0f} seconds. Reconnecting...")
                if self._websocket_client:
                    await self._websocket_client.close()
                break

    async def _handle_reconnect(self) -> None:
        """Handle reconnection with exponential backoff"""
        self._reconnect_attempts += 1
        delay = min(
            self.reconnect_base_delay * (2 ** self._reconnect_attempts),
            self.max_reconnect_delay
        )
        
        log.info(f"Reconnecting attempt {self._reconnect_attempts} in {delay} seconds...")
        await asyncio.sleep(delay)

    async def _process_messages(
        self, 
        client_redis: Any, 
        exchange: str, 
        queue_general: asyncio.Queue
    ) -> None:
        """Process incoming messages with maintenance recovery"""
        if not self._websocket_client:
            log.error("WebSocket client not initialized")
            return
            
        async for message in self._websocket_client:
            current_time = time.time()
            time_since_last = current_time - self._last_message_time
            
            # Check for maintenance recovery
            if time_since_last > self.maintenance_threshold and self._maintenance_mode:
                log.info("Exiting maintenance mode. Exchange is back online")
                self._maintenance_mode = False
                await client_redis.publish("system_status", "operational")
            
            self._last_message_time = current_time
            
            try:
                message_dict = orjson.loads(message)
                await self._handle_message(message_dict, client_redis, exchange, queue_general)
            except orjson.JSONDecodeError as e:
                log.error(f"JSON decode error: {e}")
            except Exception as e:
                log.error(f"Error processing message: {e}")
                await error_handling.parse_error_message_with_redis(client_redis, e)

    async def _handle_message(
        self,
        message_dict: Dict,
        client_redis: Any,
        exchange: str,
        queue_general: asyncio.Queue
    ) -> None:
        """Handle individual WebSocket messages"""
        # Handle authentication responses
        if "id" in message_dict and message_dict["id"] == 9929:
            self._handle_auth_response(message_dict)
        
        # Handle heartbeat requests
        elif message_dict.get("method") == "heartbeat":
            await self._heartbeat_response(client_redis)
        
        # Process market data messages
        if "params" in message_dict and message_dict["method"] != "heartbeat":
            message_params = message_dict["params"]
            if message_params:
                message_params.update({
                    "exchange": exchange,
                    "account_id": self.sub_account_id
                })
                await queue_general.put(message_params)

    def _handle_auth_response(self, message: Dict) -> None:
        """Process authentication responses"""
        try:
            result = message["result"]
            self._refresh_token = result["refresh_token"]
            
            # Calculate token expiration time
            expires_in = 300 if message.get("testnet", False) else result["expires_in"] - 240
            now_utc = datetime.now(timezone.utc)
            self._refresh_token_expiry_time = now_utc + timedelta(seconds=expires_in)
            
            log.info("Authentication successful" if not self._refresh_token 
                   else "Authentication refreshed")
                
        except KeyError as e:
            log.error(f"Missing key in auth response: {e}")

    async def _heartbeat_response(self, client_redis: Any) -> None:
        """Respond to Deribit heartbeat requests"""
        if not self._websocket_client:
            log.error("Cannot send heartbeat - WebSocket not connected")
            return
            
        msg = {
            "jsonrpc": "2.0",
            "id": 8212,
            "method": "public/test",
            "params": {},
        }

        try:
            await self._websocket_client.send(json.dumps(msg))
        except Exception as error:
            log.error(f"Heartbeat response failed: {error}")
            await error_handling.parse_error_message_with_redis(client_redis, error)

    async def _ws_auth(self, client_redis: Any) -> None:
        """Authenticate WebSocket connection"""
        if not self._websocket_client:
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
            await self._websocket_client.send(json.dumps(msg))
        except Exception as error:
            log.error(f"Authentication failed: {error}")
            await error_handling.parse_error_message_with_redis(client_redis, error)

    async def _ws_refresh_auth(self) -> None:
        """Refresh authentication token periodically"""
        while self._connection_active:
            try:
                if not self._refresh_token_expiry_time:
                    await asyncio.sleep(30)
                    continue
                    
                now_utc = datetime.now(timezone.utc)
                if now_utc >= self._refresh_token_expiry_time:
                    if not self._websocket_client:
                        log.warning("Skipping refresh - WebSocket not connected")
                        await asyncio.sleep(30)
                        continue
                        
                    msg = {
                        "jsonrpc": "2.0",
                        "id": 9929,
                        "method": "public/auth",
                        "params": {
                            "grant_type": "refresh_token",
                            "refresh_token": self._refresh_token,
                        },
                    }
                    await self._websocket_client.send(json.dumps(msg))
                    log.debug("Authentication refresh sent")
                
                await asyncio.sleep(30)  # Check every 30 seconds
            except Exception as e:
                log.error(f"Error in auth refresh: {e}")
                await asyncio.sleep(60)

    async def _initialize_connection(
        self,
        client_redis: Any,
        exchange: str,
        queue_general: asyncio.Queue,
        futures_instruments: Dict[str, Any],
        resolutions: List[int],
    ) -> None:
        """Initialize connection and subscriptions"""
        await self._ws_auth(client_redis)
        await self._establish_heartbeat(client_redis)
        
        instruments_name = futures_instruments["instruments_name"]
        ws_instruments = self._generate_subscription_list(instruments_name, resolutions)
        
        await self._ws_operation(
            operation="subscribe",
            ws_channel=ws_instruments,
            source="ws-combination",
        )

    def _generate_subscription_list(
        self, 
        instruments_name: List[str], 
        resolutions: List[int]
    ) -> List[str]:
        """Generate list of channels to subscribe to"""
        ws_instruments = []
        
        # Add account-related channels
        instrument_kinds = ["future", "future_combo"]
        for kind in instrument_kinds:
            ws_instruments.append(f"user.changes.{kind}.any.raw")
        
        ws_instruments.extend([
            "user.orders.any.any.raw",
            "user.trades.any.any.raw"
        ])
        
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

    async def _establish_heartbeat(self, client_redis: Any) -> None:
        """Establish heartbeat with Deribit"""
        if not self._websocket_client:
            log.error("Cannot establish heartbeat - WebSocket not connected")
            return
            
        msg = {
            "jsonrpc": "2.0",
            "id": 9098,
            "method": "public/set_heartbeat",
            "params": {"interval": 10},
        }

        try:
            await self._websocket_client.send(json.dumps(msg))
        except Exception as error:
            log.error(f"Heartbeat setup failed: {error}")
            await error_handling.parse_error_message_with_redis(client_redis, error)

    async def _ws_operation(
        self,
        operation: str,
        ws_channel: List[str],
        source: str = "ws-single",
    ) -> None:
        """Subscribe or unsubscribe to WebSocket channels"""
        if not self._websocket_client:
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
            await self._websocket_client.send(json.dumps(msg))
            log.debug(f"Sent {operation} for {len(ws_channel)} channels")
        except Exception as e:
            log.error(f"Error in {operation} operation: {e}")