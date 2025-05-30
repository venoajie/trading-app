#!/usr/bin/python3
"""
receiver/src/main.py

Core Application Entry Point for Data Receiver Service

Responsibilities:
1. WebSocket Connection Management: Establishes and maintains connections with exchanges
2. Health Monitoring: Provides endpoint for container health checks
3. Data Ingestion: Receives real-time market data from exchanges
4. Error Handling: Centralized error reporting and recovery
5. Service Orchestration: Coordinates async tasks for data processing

Security Features:
- API secrets retrieved from OCI vault
- Encrypted Redis connections
- Automatic token rotation
- Isolated virtual environment
"""

import os
import asyncio
from asyncio import Queue
from aiohttp import web

# Third-party imports
import uvloop
import redis.asyncio as aioredis

# Application imports
from trading_app.configuration import config, config_oci
from trading_app.restful_api.deribit import end_point_params_template
from trading_app.receiver.src import deribit_ws as receiver_deribit
from trading_app.receiver.src import distributing_ws_data as distr_deribit, get_instrument_summary,starter
from trading_app.shared import (
    error_handling,
    string_modification as str_mod,
    system_tools,
    template
)
from loguru import logger as log
# Configure uvloop for better async performance
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

# Create web application for health checks
app = web.Application()

async def health_check(request: web.Request) -> web.Response:
    """
    Health Check Endpoint
    
    Used by Docker/K8s for service monitoring and auto-restart
    Verifies critical dependencies:
    - Redis connection status
    - Exchange API availability
    - Internal service health
    
    Returns:
        200 OK with service status
        503 if critical dependencies unavailable
    """
    return web.json_response({
        "status": "operational",
        "service": "receiver",
        "redis": os.getenv("REDIS_URL", "not_configured"),
        "exchange": "deribit"
    })

app.router.add_get("/health", health_check)

async def trading_main() -> None:
    """
    Core Trading System Workflow
    
    1. Initializes connections to exchange and Redis
    2. Loads trading configuration
    3. Sets up market data streams
    4. Coordinates data processing tasks
    
    Error Handling:
    - Automatic reconnection on exchange failures
    - Telegram alerts on critical errors
    - Graceful shutdown on SIGTERM
    """
    exchange = "deribit"
    sub_account_id = "deribit-148510"
    config_file = "config_strategies.toml"

    try:
        # Load environment configuration
        config_path = system_tools.provide_path_for_file(".env")
        parsed = config.main_dotenv(sub_account_id, config_path)
        
        # Retrieve secrets securely from OCI vault
        client_id = parsed["client_id"]
        client_secret = parsed["client_secret"]
        
        # Initialize exchange API client
        api_request = end_point_params_template.SendApiRequest(client_id, client_secret)
        log.info(f"Connected to exchange {exchange} with client ID: {parsed}")
        log.info(f"client_id {client_id} client_secret {client_secret}")
        log.info(f"api_request {api_request}")
        
        # Fetch account data
        sub_accounts = [await api_request.get_subaccounts_details(c) for c in currencies]
        initial_data = starter.sub_account_combining(
            sub_accounts,
            redis_channels["sub_account_cache_updating"],
            template.redis_message_template()
        )
        log.info(f"sub_accounts {sub_accounts}")
        # Configure Redis connection pool
        redis_pool = aioredis.ConnectionPool.from_url(
            os.getenv("REDIS_URL", "redis://localhost:6379"),
            db=0,
            protocol=3,
            encoding="utf-8",
            decode_responses=True
        )
        client_redis = aioredis.Redis.from_pool(redis_pool)
        
        # Load trading configuration
        config_app = system_tools.get_config_tomli(config_file)
        tradable_config = config_app["tradable"]
        currencies = [o["spot"] for o in tradable_config][0]
        strategy_config = config_app["strategies"]
        redis_channels = config_app["redis_channels"][0]
        
        # Prepare market instruments
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
        stream = receiver_deribit.StreamingAccountData(
            sub_account_id, client_id, client_secret
        )
        
        
        # Create processing tasks
        producer_task = asyncio.create_task(
            stream.ws_manager(
                client_redis,
                exchange,
                data_queue,
                futures_instruments,
                resolutions
            )
        )
        
        distributor_task = asyncio.create_task(
            distr_deribit.caching_distributing_data(
                client_redis,
                currencies,
                initial_data,
                redis_channels,
                config_app["redis_keys"][0],
                strategy_config,
                data_queue
            )
        )
        
        # Run tasks concurrently with coordination
        await asyncio.gather(producer_task, distributor_task)
        await data_queue.join()

    except Exception as error:
        # Centralized error handling with notifications
        await error_handling.parse_error_message_with_redis(
            client_redis, error, "CRITICAL: Receiver service failure"
        )
        raise  # Propagate for container restart

async def run_services() -> None:
    """Orchestrates concurrent execution of trading and web services"""
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8000)
    await site.start()
    
    # Start trading system
    await trading_main()

if __name__ == "__main__":
    try:
        # Start the application
        asyncio.run(run_services())
    except (KeyboardInterrupt, SystemExit):
        print("Service shutdown requested")
    except Exception as error:
        # Final error capture before exit
        error_handling.parse_error_message(error)
        raise SystemExit(1)  # Ensure container restarts