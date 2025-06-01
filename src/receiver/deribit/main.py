"""
receiver/deribit/main.py
Optimized core application entry point with enhanced maintenance handling
"""

import os
import asyncio
import logging
from asyncio import Queue

# Third-party imports
import uvloop

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

# Configure uvloop for better async performance
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
log = logging.getLogger(__name__)

class ApplicationState:
    """Centralized application state management"""
    def __init__(self):
        self.maintenance_mode = False
        self.connection_active = False
        self.last_successful_connection = 0

app_state = ApplicationState()

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

async def enter_maintenance_mode(reason: str):
    """Handle transition to maintenance mode"""
    log.critical(f"Entering maintenance mode: {reason}")
    app_state.maintenance_mode = True
    # Add any cleanup or notification logic here

async def clear_queue(queue: Queue):
    """Safely clear a queue"""
    while not queue.empty():
        try:
            queue.get_nowait()
            queue.task_done()
        except asyncio.QueueEmpty:
            break

async def monitor_connection_health(stream):
    """Continuously monitor connection health"""
    while True:
        app_state.connection_active = stream.connection_active
        
        # Check if we need to enter maintenance
        if (not stream.connection_active and 
            time.time() - stream.last_successful_connection > DERIBIT_MAINTENANCE_THRESHOLD):
            await enter_maintenance_mode("Connection lost for too long")
        
        await asyncio.sleep(5)

async def trading_main() -> None:
    """Core trading workflow with enhanced error handling and maintenance awareness"""
    log.info("Initializing trading system")
    
    try:
        # Initialize Redis with retry logic
        client_redis = await setup_redis()
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
        
        distributor = distributing_ws_data.DataDistributor()
        
        distributor_task = asyncio.create_task(
            distributor.caching_distributing_data(
                client_redis,
                currencies,
                initial_data_subaccount,
                redis_channels,
                redis_keys,
                strategy_config,
                data_queue
            )
        )
        
        # Start monitoring tasks
        monitor_task = asyncio.create_task(monitor_connection_health(stream))
        
        # Main loop
        while True:
            if app_state.maintenance_mode:
                await clear_queue(data_queue)
                await asyncio.sleep(60)  # Longer sleep in maintenance
                continue
            
            await asyncio.sleep(5)

    except Exception as error:
        log.exception("Critical error in trading system")
        await enter_maintenance_mode(f"Critical error: {str(error)}")

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