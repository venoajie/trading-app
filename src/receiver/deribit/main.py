"""
receiver/deribit/main.py
Optimized core application entry point with enhanced maintenance handling
"""

import os
import asyncio
import logging
from asyncio import Queue
from loguru import logger as log
# Third-party imports
import uvloop
import orjson

# Application imports
from shared.config.settings import (
    REDIS_URL, REDIS_DB,
    DERIBIT_SUBACCOUNT, DERIBIT_CURRENCIES,
    DERIBIT_MAINTENANCE_THRESHOLD, DERIBIT_HEARTBEAT_INTERVAL
)
from shared.db.redis import redis_client as global_redis_client
from receiver.deribit import deribit_ws, distributing_ws_data, get_instrument_summary, starter
from shared.utils import error_handling, system_tools, template
from restful_api.deribit import end_point_params_template

from shared.db.redis import redis_client as global_redis_client


class ApplicationState:
    """Centralized state management with Redis synchronization"""
    def __init__(self):
        self._maintenance_mode = False
        self.connection_active = False
        self.last_successful_connection = 0

    @property
    def maintenance_mode(self):
        return self._maintenance_mode

    @maintenance_mode.setter
    def maintenance_mode(self, value):
        """Update state and publish to Redis when changed"""
        if self._maintenance_mode != value:
            self._maintenance_mode = value
            asyncio.create_task(self.publish_state())

    async def publish_state(self):
        """Publish state changes to Redis"""
        try:
            redis = await global_redis_client.get_pool()
            status = "maintenance" if self._maintenance_mode else "operational"
            await redis.publish("system_status", status)
            log.info(f"Published system status: {status}")
        except Exception as e:
            log.error(f"Failed to publish state: {str(e)}")

app_state = ApplicationState()

async def enter_maintenance_mode(reason: str):
    """Handle transition to maintenance mode"""
    if not app_state.maintenance_mode:
        log.critical(f"Entering maintenance mode: {reason}")
        app_state.maintenance_mode = True

async def exit_maintenance_mode():
    """Handle transition out of maintenance mode"""
    if app_state.maintenance_mode:
        log.info("Exiting maintenance mode")
        app_state.maintenance_mode = False

async def monitor_system_alerts():
    """Listen for system alerts from all components"""
    try:
        redis = await global_redis_client.get_pool()
        pubsub = redis.pubsub()
        await pubsub.subscribe("system_alerts")
        
        async for message in pubsub.listen():
            if message["type"] == "message":
                try:
                    alert = orjson.loads(message["data"])
                    if alert["event"] == "heartbeat_timeout":
                        await enter_maintenance_mode(alert["reason"])
                    elif alert["event"] == "heartbeat_resumed":
                        await exit_maintenance_mode()
                except orjson.JSONDecodeError:
                    log.error("Invalid alert format")
    except Exception as e:
        log.error(f"Alert monitoring failed: {str(e)}")
        await enter_maintenance_mode("Alert system failure")
        

async def setup_redis():
    """Get Redis connection from global client with retry logic"""
    max_retries = 3
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            pool = await global_redis_client.get_pool()
            if await pool.ping():
                log.info("Redis connection validated")
                return pool
        except Exception as e:
            log.warning(f"Redis connection attempt {attempt + 1} failed: {str(e)}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
    
    log.critical("All Redis connection attempts failed")
    raise ConnectionError("Redis connection failed after retries")

async def clear_queue(queue: Queue):
    """Safely clear a queue"""
    while not queue.empty():
        try:
            queue.get_nowait()
            queue.task_done()
        except asyncio.QueueEmpty:
            break


async def trading_main() -> None:
    """Core trading workflow with enhanced error handling and maintenance awareness"""
    log.info("Initializing trading system")
    
    try:
        # Initialize Redis with retry logic
        client_redis = await setup_redis()
        alert_monitor_task = asyncio.create_task(monitor_system_alerts())
    except ConnectionError:
        await enter_maintenance_mode("Redis connection failed")
        return
    
    # Configuration setup
    exchange = "deribit"
    config_path = "/app/config/strategies.toml"
    
    try:
        # Load credentials from environment
        client_id = os.getenv("DERIBIT_CLIENT_ID")
        client_secret = os.getenv("DERIBIT_CLIENT_SECRET")
        
        if not client_id or not client_secret:
            await enter_maintenance_mode("Deribit credentials not configured")
            return
        
        # Instantiate private connection
        api_request = end_point_params_template.SendApiRequest(client_id, client_secret)
        
        # Extract configuration
        currencies = DERIBIT_CURRENCIES
        resolutions = [1, 5, 15, 60]  # Default resolutions
        
        # Prepare instruments
        futures_instruments = await get_instrument_summary.get_futures_instruments(
            currencies, ["perpetual"]  # Default to perpetual contracts
        )
        
        # Initialize components with backpressure control
        data_queue = Queue(maxsize=1000)
        
        # Create connection manager with configurable parameters
        stream = deribit_ws.StreamingAccountData(
            sub_account_id=DERIBIT_SUBACCOUNT,
            client_id=client_id,
            client_secret=client_secret,
            reconnect_base_delay=5,
            max_reconnect_delay=300,
            maintenance_threshold=DERIBIT_MAINTENANCE_THRESHOLD,
            websocket_timeout=900,
            heartbeat_interval=DERIBIT_HEARTBEAT_INTERVAL
        )
        
        # Load TOML configuration
        try:
            config_app = system_tools.get_config_tomli(config_path)
            log.info(f"Successfully loaded configuration from {config_path}")
            
            redis_channels = config_app.get("redis_channels", [{}])[0]
            redis_keys = config_app.get("redis_keys", [{}])[0]
            strategy_config = config_app.get("strategies", [])
            ws_config = config_app.get("ws", {})
            
        except Exception as e:
            log.error(f"Failed to load configuration: {str(e)}")
            # Fallback to default values
            redis_channels = {}
            redis_keys = {}
            strategy_config = []
            ws_config = {}
        
        sub_account_cached_channel = redis_channels.get("sub_account_cache_updating", "default_channel")
        
        # Get subaccounts data
        sub_accounts = []
        for currency in currencies:
            try:
                account_data = await api_request.get_subaccounts_details(currency)
                sub_accounts.append(account_data)
            except Exception as e:
                log.error(f"Failed to get subaccount details for {currency}: {str(e)}")
                continue
        
        result_template = template.redis_message_template()
        
        initial_data_subaccount = starter.sub_account_combining(
            sub_accounts,
            sub_account_cached_channel,
            result_template,
        )
        
        # Create tasks
        producer_task = asyncio.create_task(
            stream.manage_connection(
                client_redis,
                exchange,
                data_queue,
                futures_instruments,
                resolutions
            )
        )
        
        
        distributor_task = asyncio.create_task(
            distributing_ws_data.caching_distributing_data(
                client_redis,
                currencies,
                initial_data_subaccount,
                redis_channels,
                redis_keys,
                strategy_config,
                data_queue
            )
        )
        
        
        # Main loop
        while True:
            if app_state.maintenance_mode:
                await clear_queue(data_queue)
                await asyncio.sleep(60)
            else:
                await asyncio.sleep(5)
                
    except Exception as error:
        log.exception("Critical error in trading system")
        await enter_maintenance_mode(f"Critical error: {str(error)}")

    finally:

        if 'alert_monitor_task' in locals():
            alert_monitor_task.cancel()
            try:
                await alert_monitor_task
            except asyncio.CancelledError:
                log.info("Alert monitoring task cancelled")

async def run_services() -> None:
    """Orchestrate concurrent service execution without web components"""
    log.info("Starting service orchestration")
    
    try:
        while True:
            if app_state.maintenance_mode:
                log.warning("Application in maintenance mode - waiting")
                await asyncio.sleep(60)
                continue
                
            await trading_main()
            
    except (KeyboardInterrupt, SystemExit):
        log.info("Service shutdown requested")
    except Exception as error:
        log.exception("Unhandled error in service orchestration")
        await enter_maintenance_mode("Unhandled error in service orchestration")

if __name__ == "__main__":
    # Configure structured logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    try:
        log.info("Starting application")
        asyncio.run(run_services())
    except Exception as error:
        log.exception("Fatal error during application startup")
        raise SystemExit(1)