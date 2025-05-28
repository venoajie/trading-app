#!/usr/bin/env python3
"""
Main entry point for Deribit receiver service
Handles WebSocket connection, data reception, and queue distribution
"""

import asyncio
import os
import uvloop
import aioredis
from loguru import logger as log
from dotenv import load_dotenv

from shared.configuration.settings import get_config
from shared.streaming_helper.data_receiver.deribit import StreamingAccountData

# Apply uvloop for better async performance
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

async def main():
    """Main application coroutine"""
    # Load configuration
    config = get_config()
    
    log.info("Starting Deribit receiver service")
    log.debug(f"Configuration: {config}")
    
    # Create Redis connection pool
    redis_pool = aioredis.ConnectionPool.from_url(
        f"redis://{config.redis_host}",
        port=6379,
        db=0,
        max_connections=20,
        decode_responses=True
    )
    
    # Create async Redis client
    async with aioredis.Redis(connection_pool=redis_pool) as client_redis:
        try:
            # Create data queue
            data_queue = asyncio.Queue(maxsize=1000)
            
            # Initialize and start WebSocket manager
            stream = StreamingAccountData(
                sub_account_id=config.deribit_sub_account_id,
                client_id=config.deribit_client_id,
                client_secret=config.deribit_client_secret
            )
            
            await stream.ws_manager(
                client_redis=client_redis,
                exchange="deribit",
                queue_general=data_queue,
                futures_instruments=config.futures_instruments,
                resolutions=config.resolutions
            )
        except asyncio.CancelledError:
            log.info("Application shutdown requested")
        except Exception as e:
            log.critical(f"Critical failure: {e}")
            raise
        finally:
            await redis_pool.disconnect()
            log.info("Redis connection pool closed")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Application terminated by user")
    except Exception as e:
        log.critical(f"Unhandled exception: {e}")
        raise