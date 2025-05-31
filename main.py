# trading_app/receiver/src/main.py
"""Core application entry point with enhanced maintenance handling"""

import os
import asyncio
import logging
import time
from asyncio import Queue
from typing import Dict, List, Optional, Any

# Third-party imports
import uvloop
from aiohttp import web
import redis.asyncio as aioredis

# Application imports
from trading_app.configuration import config, config_oci
from trading_app.restful_api.deribit import end_point_params_template
from trading_app.receiver.src import deribit_ws, distributing_ws_data, get_instrument_summary, starter
from trading_app.shared import (
    error_handling,
    string_modification as str_mod,
    system_tools,
    template
)
from trading_app.shared.db_management.sqlite_management import (
    get_db_path, 
    set_redis_client
)

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
    """Enhanced health check with maintenance status"""
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
    """Configure and test Redis connection"""
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    log.info(f"Connecting to Redis at {redis_url}")
    
    redis_pool = aioredis.ConnectionPool.from_url(
        redis_url,
        db=0,
        protocol=3,
        encoding="utf-8",
        decode_responses=True
    )
    client = aioredis.Redis.from_pool(redis_pool)
    
    # Test connection
    if not await client.ping():
        raise ConnectionError("Redis connection failed")
    
    log.info("Redis connection successful")
    return client

async def trading_main() -> None:
    """Core trading workflow with maintenance awareness"""
    log.info("Starting trading system initialization")
    
    # Initialize Redis
    client_redis = await setup_redis()
    set_redis_client(client_redis)
    
    exchange = "deribit"
    sub_account_id = "deribit-148510"
    config_file = os.getenv("CONFIG_PATH", "/app/config/config_strategies.toml")

    try:
        # Load configurations
        config_path = system_tools.provide_path_for_file(".env")
        parsed = config.main_dotenv(sub_account_id, config_path)
        client_id = parsed["client_id"]
        client_secret = parsed["client_secret"]
        
        config_app = system_tools.get_config_tomli(config_file)
        tradable_config = config_app["tradable"]
        currencies = [o["spot"] for o in tradable_config][0]
        strategy_config = config_app["strategies"]
        redis_channels = config_app["redis_channels"][0]
        
        # Prepare instruments
        settlement_periods = str_mod.remove_redundant_elements(
            str_mod.remove_double_brackets_in_list(
                [o["settlement_period"] for o in strategy_config]
            )
        )
        
        futures_instruments = await get_instrument_summary.get_futures_instruments(
            currencies, settlement_periods
        )
        
        # Initialize components
        resolutions = [o["resolutions"] for o in tradable_config][0]
        data_queue = Queue(maxsize=1000)  # Backpressure control
        
        # Create Redis connection pool
        redis_pool = aioredis.ConnectionPool.from_url(
            os.getenv("REDIS_URL", "redis://localhost:6379"),
            db=0,
            protocol=3,
            encoding="utf-8",
            decode_responses=True
        )
        client_redis = aioredis.Redis.from_pool(redis_pool)
        
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
            sub_account_id, client_id, client_secret
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
                config_app["redis_keys"][0],
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
        await error_handling.parse_error_message_with_redis(
            client_redis, error, "CRITICAL: Receiver service failure"
        )
        raise

async def run_services() -> None:
    """Orchestrates concurrent execution of services"""
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
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    try:
        log.info("Starting application")
        asyncio.run(run_services())
    except (KeyboardInterrupt, SystemExit):
        log.info("Service shutdown requested")
    except Exception as error:
        error_handling.parse_error_message(error)
        raise SystemExit(1)