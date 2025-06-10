# src\services\receiver\deribit\main.py

"""Decoupled receiver service focused on Redis Stream production"""
import os
import asyncio
import uvloop
import logging
from loguru import logger as log

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

# Application imports
from core.db.redis import redis_client as global_redis_client
from core.error_handler import error_handler
from src.scripts.deribit import get_instrument_summary
from src.scripts.deribit.restful_api import end_point_params_template
from core.security import get_secret
from src.services.receiver.deribit import deribit_ws
from src.shared.config.settings import (
    REDIS_URL, REDIS_DB,
    DERIBIT_SUBACCOUNT, DERIBIT_CURRENCIES,
    DERIBIT_MAINTENANCE_THRESHOLD, DERIBIT_HEARTBEAT_INTERVAL
)
from src.shared.utils import system_tools, template

async def setup_redis():
    """Robust Redis connection setup with health checks"""
    max_retries = 5
    retry_delay = 3
    
    for attempt in range(max_retries):
        try:
            pool = await global_redis_client.get_pool()
            if await pool.ping():
                log.info("Redis connection validated")
                return pool
        except Exception as e:
            log.warning(f"Redis connection attempt {attempt+1} failed: {str(e)}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
    
    log.critical("All Redis connection attempts failed")
    raise ConnectionError("Redis connection failed after retries")

async def run_receiver():
    """Core receiver workflow with isolated error handling"""
    log.info("Starting Deribit receiver service")
    
    try:
        # Get Redis client wrapper instance
        client_redis = global_redis_client
        
        # Load configuration
        client_id = get_secret("deribit_client_id")
        client_secret = get_secret("deribit_client_secret")
        if not client_id or not client_secret:
            log.error("Deribit credentials not configured")
            return

        # Get instruments
        currencies = DERIBIT_CURRENCIES
        resolutions = [1, 5, 15, 60]
        futures_instruments = await get_instrument_summary.get_futures_instruments(
            currencies, ["perpetual"]
        )

        # Initialize WebSocket client
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

        await stream.manage_connection(
            client_redis,  # Directly use the connection pool
            "deribit",
            futures_instruments,
            resolutions
        )
        
    except Exception as error:
        log.exception(f"Receiver service failed: {error}")
        raise

async def main():
    """Service entry point with graceful shutdown"""
    try:
        await run_receiver()
    except (KeyboardInterrupt, SystemExit):
        log.info("Receiver service shutdown requested")
    except Exception as error:
        log.exception(f"Fatal error in receiver service: {error}")
        raise SystemExit(1)

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    uvloop.run(main())