"""
receiver/deribit/main.py
Core application entry point with security enhancements
"""

import os
import asyncio
import logging
import time
from asyncio import Queue

# Third-party imports
import uvloop
from aiohttp import web

# Application imports

from shared.config.settings import (
    DERIBIT_SUBACCOUNT, DERIBIT_CURRENCIES,
    SECURITY_BLOCKED_SCANNERS, SECURITY_RATE_LIMIT, SECURITY_HEADERS
)
from shared.db.redis import redis_client as global_redis_client
from shared.security import security_middleware_factory
from receiver.deribit import deribit_ws, distributing_ws_data, get_instrument_summary

# Create security middleware with application settings
security_middleware = security_middleware_factory({
    "blocked_scanners": SECURITY_BLOCKED_SCANNERS,
    "rate_limit": SECURITY_RATE_LIMIT,
    "security_headers": SECURITY_HEADERS
})

# Create web application with security middleware
app = web.Application(middlewares=[security_middleware])

# State tracking for health checks
app.connection_active = False
app.maintenance_mode = False
app.start_time = time.time()

# Configure uvloop for better async performance
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
log = logging.getLogger(__name__)

async def health_check(request: web.Request) -> web.Response:
    """Secure health endpoint"""
    status = "operational"
    if app.maintenance_mode:
        status = "maintenance"
    elif not app.connection_active:
        status = "disconnected"
    
    return web.json_response({
        "status": status,
        "service": "trading-app",
        "uptime_seconds": int(time.time() - app.start_time)
    })

app.router.add_get("/health", health_check)

async def detailed_status(request: web.Request) -> web.Response:
    """
    Authenticated system status report
    - Requires API key for access
    - Provides detailed system metrics
    """
    # Validate API key configuration
    status_api_key = os.getenv("STATUS_API_KEY")
    if not status_api_key:
        return web.Response(
            status=500,
            text="STATUS_API_KEY not configured in environment"
        )
    
    # Validate API key for sensitive information
    if request.headers.get("X-API-KEY") != status_api_key:
        return web.Response(status=401, text="Unauthorized")
    
    return web.json_response({
        "websocket_connected": app.connection_active,
        "maintenance_mode": app.maintenance_mode,
        "start_time": app.start_time,
        "redis_url": os.getenv("REDIS_URL", "not_configured")[:15] + "..."  # Partial exposure
    })

app.router.add_get("/status", detailed_status)

async def setup_redis():
    """Get Redis connection from global client"""
    try:
        pool = await global_redis_client.get_pool()
        if await pool.ping():
            log.info("Redis connection validated")
            return pool
    except Exception as e:
        log.error(f"Redis connection failed: {str(e)}")
    raise ConnectionError("Redis connection failed")

async def trading_main() -> None:
    """Core trading workflow with enhanced error handling"""
    log.info("Initializing trading system")
    
    try:
        # Initialize Redis
        client_redis = await setup_redis()
    except ConnectionError:
        log.critical("Failed to connect to Redis. Entering maintenance mode.")
        app.maintenance_mode = True
        while True:
            await asyncio.sleep(60)
        return
    
    # Configuration setup
    exchange = "deribit"
    
    try:
        # Load credentials from environment
        client_id = os.getenv("DERIBIT_CLIENT_ID")
        client_secret = os.getenv("DERIBIT_CLIENT_SECRET")
        
        if not client_id or not client_secret:
            log.critical("Deribit credentials not configured in environment")
            app.maintenance_mode = True  # Enter maintenance mode
            while True:
                # Keep application running but in maintenance state
                await asyncio.sleep(60)
            return
        
        # Extract configuration
        currencies = DERIBIT_CURRENCIES
        resolutions = [1, 5, 15, 60]  # Default resolutions
        
        # Prepare instruments
        futures_instruments = await get_instrument_summary.get_futures_instruments(
            currencies, ["perpetual"]  # Default to perpetual contracts
        )
        
        # Initialize components
        data_queue = Queue(maxsize=1000)  # Backpressure control
        
        # Create connection manager
        stream = deribit_ws.StreamingAccountData(
            sub_account_id=DERIBIT_SUBACCOUNT,
            client_id=client_id,
            client_secret=client_secret,
            reconnect_base_delay=5,      # Default values
            max_reconnect_delay=300,      # 5 minutes
            maintenance_threshold=900,    # 15 minutes
            websocket_timeout=900,        # 15 minutes
            heartbeat_interval=30         # 30 seconds
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
                {},  # Initial data placeholder
                {},  # Redis channels placeholder
                {},  # Redis keys placeholder
                [],  # Strategy config placeholder
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
                    await data_queue.get()
                    data_queue.task_done()
            
            await asyncio.sleep(5)

    except Exception as error:
        log.exception("Critical error in trading system")
        app.maintenance_mode = True  # Enter maintenance mode on critical error
        while True:
            await asyncio.sleep(60)  # Keep process alive

async def run_services() -> None:
    """Orchestrate concurrent service execution"""
    log.info("Starting service orchestration")
    
    # Web server setup
    runner = web.AppRunner(app)
    await runner.setup()
    
    # Bind to localhost only for security
    site = web.TCPSite(runner, "127.0.0.1", 8000)
    await site.start()
    log.info("Health check server running on 127.0.0.1:8000")
    
    # Start trading system
    await trading_main()

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
    except (KeyboardInterrupt, SystemExit):
        log.info("Service shutdown requested")
    except Exception as error:
        log.exception("Unhandled error in main")
        raise SystemExit(1)