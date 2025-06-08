# src\services\receiver\deribit\deribit_ws.py

"""
receiver/deribit/deribit_ws.py
WebSocket client for Deribit exchange with enhanced maintenance handling
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, cast

# Third-party imports
import orjson
import websockets
from dataclassy import dataclass
from websockets import WebSocketClientProtocol

# Application imports
from src.scripts.deribit.restful_api import end_point_params_template
from src.shared.utils import error_handling, string_modification as str_mod
from src.shared.config.settings import REDIS_URL, DERIBIT_CURRENCIES

# Configure logger
log = logging.getLogger(__name__)

@dataclass(unsafe_hash=True, slots=True)
class StreamingAccountData:
    """Enhanced WebSocket manager with maintenance detection and recovery"""
    sub_account_id: str
    client_id: str
    client_secret: str
    reconnect_base_delay: int = 5
    max_reconnect_delay: int = 300
    maintenance_threshold: int = 900
    websocket_timeout: int = 900
    heartbeat_interval: int = 30
    loop: asyncio.AbstractEventLoop = cast(asyncio.AbstractEventLoop, None)
    ws_connection_url: str = "wss://www.deribit.com/ws/api/v2"
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
                    ping_interval=20,
                    ping_timeout=60,
                    close_timeout=60,
                ) as self.websocket_client:
                    log.info("WebSocket connection established")
                    self.last_message_time = time.time()
                    
                    # Create background tasks
                    self.heartbeat_task = asyncio.create_task(
                        self.monitor_heartbeat(client_redis)
                    )
                    self.refresh_task = asyncio.create_task(
                        self.ws_refresh_auth()
                    )

                    # Setup subscriptions
                    await self.authenticate_and_setup(
                        client_redis, exchange,  
                        futures_instruments, resolutions
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
                    "reason": f"No messages for {time_since_last:.0f} seconds"
                }
                await client_redis.publish("system_alerts", json.dumps(alert))
                
            # Normal timeout handling
            elif time_since_last > self.websocket_timeout:  # Use instance variable
                log.warning(f"No messages for {time_since_last:.0f} seconds. Reconnecting...")
                if self.websocket_client:
                    await self.websocket_client.close()
                break

    async def handle_reconnect(self) -> None:
        """Handle reconnection with exponential backoff"""
        self.reconnect_attempts += 1
        delay = min(
            self.reconnect_base_delay * (2 ** self.reconnect_attempts),  # Use instance variable
            self.max_reconnect_delay  # Use instance variable
        )
        
        log.info(f"Reconnecting attempt {self.reconnect_attempts} in {delay} seconds...")
        await asyncio.sleep(delay)

    async def process_messages(self, client_redis, exchange):
        """Process incoming messages with state recovery"""
        async for message in self.websocket_client:
            current_time = time.time()
            time_since_last = current_time - self.last_message_time
            
            # Detect recovery from silence
            if time_since_last > self.maintenance_threshold:
                alert = {
                    "component": "deribit_ws",
                    "event": "heartbeat_resumed",
                    "reason": "Message flow restored"
                }
                await client_redis.publish("system_alerts", json.dumps(alert))
                
            self.last_message_time = current_time
            
            try:
                message_dict = orjson.loads(message)
                
                # Handle authentication responses
                if "id" in message_dict and message_dict["id"] == 9929:
                    self.handle_auth_response(message_dict)
                
                # Handle heartbeat requests
                elif message_dict.get("method") == "heartbeat":
                    await self.heartbeat_response(client_redis)
                
                # Process market data messages
                if "params" in message_dict and message_dict["method"] != "heartbeat":
                    # Validate message structure
                    if "channel" not in message_dict["params"]:
                        log.error(f"Invalid message format: {message_dict}")
                        continue
                        
                    try:
                        print(message_dict)
                        await client_redis.xadd(
                            "stream:market_data",
                            {"data": orjson.dumps(message_dict["params"]).decode()},
                            maxlen=10000
                            )
                        log.debug(f"Sent to stream: {message_dict['params']['channel']}")
                        
                    except Exception as e:
                        log.error(f"XADD failed: {e}")  

            except Exception as e:
                log.error(f"Failed to add to stream: {e}")    

    def handle_auth_response(self, message: Dict) -> None:
        """Handle authentication responses"""
        try:
            result = message["result"]
            self.refresh_token = result["refresh_token"]
            
            # Calculate token expiration time
            expires_in = 300 if message.get("testnet", False) else result["expires_in"] - 240
            now_utc = datetime.now(timezone.utc)
            self.refresh_token_expiry_time = now_utc + timedelta(seconds=expires_in)
            
            if not self.refresh_token:
                log.info("WebSocket authentication successful")
            else:
                log.info("Authentication refreshed successfully")
                
        except KeyError as e:
            log.error(f"Missing key in auth response: {e}")

    async def heartbeat_response(self, client_redis: Any) -> None:
        """Respond to Deribit heartbeat requests"""
        if not self.websocket_client:
            log.error("Cannot send heartbeat - WebSocket not connected")
            return
            
        msg = {
            "jsonrpc": "2.0",
            "id": 8212,
            "method": "public/test",
            "params": {},
        }

        try:
            await self.websocket_client.send(json.dumps(msg))
        except Exception as error:
            log.error(f"Heartbeat response failed: {error}")
            await error_handling.parse_error_message_with_redis(client_redis, error)

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

    def generate_subscription_list(
        self, 
        instruments_name: List[str], 
        resolutions: List[str]
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

    async def establish_heartbeat(self, client_redis: Any) -> None:
        """Establish heartbeat with Deribit"""
        if not self.websocket_client:
            log.error("Cannot establish heartbeat - WebSocket not connected")
            return
            
        msg = {
            "jsonrpc": "2.0",
            "id": 9098,
            "method": "public/set_heartbeat",
            "params": {"interval": 10},
        }

        try:
            await self.websocket_client.send(json.dumps(msg))
        except Exception as error:
            log.error(f"Heartbeat setup failed: {error}")
            await error_handling.parse_error_message_with_redis(client_redis, error)

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