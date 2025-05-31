# trading_app/receiver/src/deribit_ws.py
"""WebSocket client for Deribit exchange with maintenance handling"""

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any

# Third-party imports
import orjson
import websockets
from dataclassy import dataclass

# Application imports
from trading_app.restful_api.deribit import end_point_params_template
from trading_app.shared import error_handling, string_modification as str_mod

# Configure logger
log = logging.getLogger(__name__)

# Configuration - Should be moved to config later
RECONNECT_BASE_DELAY = 5  # Initial reconnect delay in seconds
MAX_RECONNECT_DELAY = 300  # Max 5 minutes between retries
WEBSOCKET_TIMEOUT = 900  # 15 minutes without data (matches exchange maintenance)
HEARTBEAT_INTERVAL = 30  # Seconds between heartbeat checks
MAINTENANCE_THRESHOLD = 900  # 15 minutes maintenance threshold

@dataclass(unsafe_hash=True, slots=True)
class StreamingAccountData:
    """Enhanced WebSocket manager with maintenance detection and recovery"""
    sub_account_id: str
    client_id: str
    client_secret: str
    loop = asyncio.get_event_loop()
    ws_connection_url: str = "wss://www.deribit.com/ws/api/v2"
    websocket_client: Optional[websockets.WebSocketClientProtocol] = None
    refresh_token: Optional[str] = None
    refresh_token_expiry_time: Optional[datetime] = None
    last_message_time: float = 0.0
    reconnect_attempts: int = 0
    connection_active: bool = False
    maintenance_mode: bool = False  # Tracks maintenance state

    async def manage_connection(
        self,
        client_redis: Any,
        exchange: str,
        queue_general: asyncio.Queue,
        futures_instruments: Dict,
        resolutions: List[str],
    ) -> None:
        """Main connection loop with maintenance detection"""
        while True:
            heartbeat_task = None
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
                    
                    # Start monitoring tasks
                    heartbeat_task = asyncio.create_task(
                        self.monitor_heartbeat(client_redis)
                    )
                    
                    # Setup subscriptions
                    await self.authenticate_and_setup(
                        client_redis, exchange, queue_general, 
                        futures_instruments, resolutions
                    )
                    
                    # Process incoming messages
                    await self.process_messages(client_redis, exchange, queue_general)
                    
            except (websockets.ConnectionClosed, ConnectionError) as e:
                log.warning(f"Connection closed: {e}")
            except Exception as e:
                log.error(f"Unexpected connection error: {e}")
            finally:
                self.connection_active = False
                await self.handle_reconnect()
                
                # Clean up tasks
                if heartbeat_task and not heartbeat_task.done():
                    heartbeat_task.cancel()

    async def monitor_heartbeat(self, client_redis: Any) -> None:
        """Monitor connection health with maintenance detection"""
        while self.connection_active:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            time_since_last = time.time() - self.last_message_time
            
            # Detect extended silence (maintenance period)
            if time_since_last > MAINTENANCE_THRESHOLD and not self.maintenance_mode:
                log.warning("Exchange maintenance detected. Entering maintenance mode")
                self.maintenance_mode = True
                await client_redis.publish("system_status", "maintenance")
            
            # Normal timeout handling
            elif time_since_last > WEBSOCKET_TIMEOUT:
                log.warning(f"No messages for {time_since_last:.0f} seconds. Reconnecting...")
                if self.websocket_client:
                    await self.websocket_client.close()
                break

    async def process_messages(
        self, 
        client_redis: Any, 
        exchange: str, 
        queue_general: asyncio.Queue
    ) -> None:
        """Process incoming messages with maintenance recovery"""
        async for message in self.websocket_client:
            # Update last message time and check maintenance state
            current_time = time.time()
            time_since_last = current_time - self.last_message_time
            
            # Exit maintenance mode if we receive data after long silence
            if time_since_last > MAINTENANCE_THRESHOLD and self.maintenance_mode:
                log.info("Exiting maintenance mode. Exchange is back online")
                self.maintenance_mode = False
                await client_redis.publish("system_status", "operational")
            
            self.last_message_time = current_time
            
            try:
                message_dict = orjson.loads(message)
                
                # Handle authentication responses
                if "id" in message_dict:
                    if message_dict["id"] == 9929:
                        self.handle_auth_response(message_dict)
                
                # Handle heartbeat requests
                elif "method" in message_dict:
                    if message_dict["method"] == "heartbeat":
                        await self.heartbeat_response(client_redis)
                
                # Process market data messages
                if "params" in message_dict and message_dict["method"] != "heartbeat":
                    message_params = message_dict["params"]
                    if message_params:
                        message_params.update({
                            "exchange": exchange,
                            "account_id": self.sub_account_id
                        })
                        await queue_general.put(message_params)
                        
            except Exception as e:
                log.error(f"Error processing message: {e}")
                await error_handling.parse_error_message_with_redis(client_redis, e)

    async def handle_reconnect(self) -> None:
        """Handle reconnection with exponential backoff"""
        self.reconnect_attempts += 1
        delay = min(RECONNECT_BASE_DELAY * (2 ** self.reconnect_attempts), 
                    MAX_RECONNECT_DELAY)
        
        log.info(f"Reconnecting attempt {self.reconnect_attempts} in {delay} seconds...")
        await asyncio.sleep(delay)

    def handle_auth_response(self, message: Dict) -> None:
        """Handle authentication responses"""
        if self.refresh_token is None:
            log.info("Successfully authenticated WebSocket Connection")
        else:
            log.info("Successfully refreshed the authentication")

        self.refresh_token = message["result"]["refresh_token"]
        expires_in = 300 if message.get("testnet", False) else message["result"]["expires_in"] - 240
        now_utc = datetime.now(timezone.utc)
        self.refresh_token_expiry_time = now_utc + timedelta(seconds=expires_in)

    async def heartbeat_response(
        self,
        client_redis: Any,
    ) -> None:
        """
        Sends the required WebSocket response to
        the Deribit API Heartbeat message.
        """
        msg: Dict = {
            "jsonrpc": "2.0",
            "id": 8212,
            "method": "public/test",
            "params": {},
        }

        try:
            await self.websocket_client.send(json.dumps(msg))
        except Exception as error:
            await error_handling.parse_error_message_with_redis(client_redis, error)

    async def ws_auth(
        self,
        client_redis: Any,
    ) -> None:
        """
        Requests DBT's `public/auth` to
        authenticate the WebSocket Connection.
        """
        msg: Dict = {
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
            await error_handling.parse_error_message_with_redis(client_redis, error)

    async def ws_refresh_auth(self) -> None:
        """
        Requests DBT's `public/auth` to refresh
        the WebSocket Connection's authentication.
        """
        while True:
            now_utc = datetime.now(timezone.utc)

            if self.refresh_token_expiry_time is not None:
                if now_utc > self.refresh_token_expiry_time:
                    msg: Dict = {
                        "jsonrpc": "2.0",
                        "id": 9929,
                        "method": "public/auth",
                        "params": {
                            "grant_type": "refresh_token",
                            "refresh_token": self.refresh_token,
                        },
                    }

                    try:
                        await self.websocket_client.send(json.dumps(msg))
                    except Exception as e:
                        log.error(f"Error refreshing auth: {e}")
                        break

            await asyncio.sleep(150)

    async def authenticate_and_setup(
        self,
        client_redis: Any,
        exchange: str,
        queue_general: asyncio.Queue,
        futures_instruments: Dict,
        resolutions: List[str],
    ) -> None:
        """Authenticate and setup subscriptions"""
        # Authenticate WebSocket Connection
        await self.ws_auth(client_redis)

        # Establish Heartbeat
        await self.establish_heartbeat(client_redis)

        # Start Authentication Refresh Task
        self.loop.create_task(self.ws_refresh_auth())

        # Prepare and subscribe to instruments
        instruments_name = futures_instruments["instruments_name"]
        ws_instruments = []
        # Fixed: instrument kinds should be separate strings
        instrument_kinds = ["future", "future_combo"]

        for kind in instrument_kinds:
            user_changes = f"user.changes.{kind}.any.raw"
            ws_instruments.append(user_changes)

        orders = "user.orders.any.any.raw"
        ws_instruments.append(orders)
        trades = "user.trades.any.any.raw"
        ws_instruments.append(trades)

        for instrument in instruments_name:
            if "PERPETUAL" in instrument:
                currency = str_mod.extract_currency_from_text(instrument)
                portfolio = f"user.portfolio.{currency}"
                ws_instruments.append(portfolio)

                for resolution in resolutions:
                    ws_chart = f"chart.trades.{instrument}.{resolution}"
                    ws_instruments.append(ws_chart)

            incremental_ticker = f"incremental_ticker.{instrument}"
            ws_instruments.append(incremental_ticker)

        await self.ws_operation(
            operation="subscribe",
            ws_channel=ws_instruments,
            source="ws-combination",
        )

    async def establish_heartbeat(
        self,
        client_redis: Any,
    ) -> None:
        """
        Establish heartbeat connection with Deribit
        """
        msg: Dict = {
            "jsonrpc": "2.0",
            "id": 9098,
            "method": "public/set_heartbeat",
            "params": {"interval": 10},
        }

        try:
            await self.websocket_client.send(json.dumps(msg))
        except Exception as error:
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
        sleep_time: int = 0.05
        await asyncio.sleep(sleep_time)

        id = end_point_params_template.id_numbering(
            operation,
            ws_channel,
        )

        msg: Dict = {
            "jsonrpc": "2.0",
        }

        if "ws" in source:
            if "single" in source:
                ws_channel = [ws_channel]

            extra_params: Dict = dict(
                id=id,
                method=f"private/{operation}",
                params={"channels": ws_channel},
            )

            msg.update(extra_params)

            if msg["params"]["channels"]:
                try:
                    await self.websocket_client.send(json.dumps(msg))
                except Exception as e:
                    log.error(f"Error in ws_operation: {e}")