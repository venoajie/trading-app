"""
receiver/deribit/main.py
Core application entry point with robust error handling
"""

import os
import asyncio
import logging
import time
from asyncio import Queue

# Third-party imports
import uvloop
from aiohttp import web
import redis.asyncio as aioredis

# Application imports
from config import config
from restful_api.deribit import end_point_params_template
from receiver.deribit import deribit_ws, distributing_ws_data, get_instrument_summary, starter
from shared import error_handling, string_modification as str_mod, system_tools, template
from shared.db_management.redis_client import create_redis_pool
from shared.db_management.sqlite_management import set_redis_client
from shared.config import CONFIG

# Configure uvloop for better async performance
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
log = logging.getLogger(__name__)

# Create web application
app = web.Application()

# State tracking for health checks
app.connection_active = False
app.maintenance_mode = False
app.start_time = time.time()

async def health_check(request: web.Request) -> web.Response:
    """Service health endpoint with maintenance status"""
    status = "operational"
    if app.maintenance_mode:
        status = "maintenance"
    elif not app.connection_active:
        status = "disconnected"
    
    return web.json_response({
        "status": status,
        "service": "receiver",
        "exchange": "deribit",
        "uptime_seconds": int(time.time() - app.start_time)
    })

app.router.add_get("/health", health_check)

async def detailed_status(request: web.Request) -> web.Response:
    """Detailed system status report"""
    return web.json_response({
        "websocket_connected": app.connection_active,
        "maintenance_mode": app.maintenance_mode,
        "start_time": app.start_time,
        "redis_url": os.getenv("REDIS_URL", "not_configured")
    })

app.router.add_get("/status", detailed_status)

async def setup_redis() -> aioredis.Redis:
    """Create and validate Redis connection"""
    client = await create_redis_pool()
    if not await client.ping():
        raise ConnectionError("Redis connection failed")
    log.info("Redis connection validated")
    return client

async def trading_main() -> None:
    """Core trading workflow with maintenance awareness"""
    log.info("Initializing trading system")
    
    # Initialize Redis
    client_redis = await setup_redis()
    set_redis_client(client_redis)
    
    # Configuration setup
    exchange = "deribit"
    sub_account_id = "deribit-148510"
    
    try:
        # Load credentials
        config_path = system_tools.provide_path_for_file(".env")
        parsed = config.main_dotenv(sub_account_id, config_path)
        client_id = parsed["client_id"]
        client_secret = parsed["client_secret"]
        
        # Extract configuration
        tradable_config = CONFIG["tradable"][0]
        currencies = tradable_config["spot"]
        resolutions = tradable_config["resolutions"]
        redis_channels = CONFIG["redis_channels"][0]
        strategy_config = CONFIG["strategies"]
        ws_config = CONFIG.get("ws", {})
        
        # Prepare instruments
        settlement_periods = str_mod.remove_redundant_elements(
            str_mod.remove_double_brackets_in_list(
                [s["settlement_period"] for s in strategy_config]
            )
        )
        
        futures_instruments = await get_instrument_summary.get_futures_instruments(
            currencies, settlement_periods
        )
        
        # Initialize components
        data_queue = Queue(maxsize=1000)  # Backpressure control
        
        # Initialize API client
        api_request = end_point_params_template.SendApiRequest(client_id, client_secret)
        
        # Fetch account data
        sub_accounts = [await api_request.get_subaccounts_details(c) for c in currencies]
        initial_data = starter.sub_account_combining(
            sub_accounts,
            redis_channels["sub_account_cache_updating"],
            template.redis_message_template()
        )
        
        # Create connection manager
        stream = deribit_ws.StreamingAccountData(
            sub_account_id=sub_account_id,
            client_id=client_id,
            client_secret=client_secret,
            reconnect_base_delay=ws_config.get("reconnect_base_delay", 5),
            max_reconnect_delay=ws_config.get("max_reconnect_delay", 300),
            maintenance_threshold=ws_config.get("maintenance_threshold", 900),
            heartbeat_interval=ws_config.get("heartbeat_interval", 30)
        )
        
        # Create processing tasks
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
                initial_data,
                redis_channels,
                CONFIG["redis_keys"][0],
                strategy_config,
                data_queue
            )
        )
        
        # State monitoring loop
        while True:
            app.connection_active = stream.connection_active
            app.maintenance_mode = stream.maintenance_mode
            
            # Clear queue during maintenance to prevent backpressure
            if app.maintenance_mode and not data_queue.empty():
                log.warning("Clearing queue during maintenance")
                while not data_queue.empty():
                    data_queue.get_nowait()
                    data_queue.task_done()
            
            await asyncio.sleep(5)

    except Exception as error:
        log.exception("Critical error in trading system")
        await error_handling.handle_error(
            client_redis, 
            error, 
            "CRITICAL: Receiver service failure"
        )
        raise

async def run_services() -> None:
    """Orchestrate concurrent service execution"""
    log.info("Starting service orchestration")
    
    # Web server setup
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8000)
    await site.start()
    log.info("Health check server running on port 8000")
    
    # Start trading system
    await trading_main()

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    try:
        log.info("Starting application")
        asyncio.run(run_services())
    except (KeyboardInterrupt, SystemExit):
        log.info("Service shutdown requested")
    except Exception as error:
        log.exception("Unhandled error in main")
        raise SystemExit(1)