"""
Deribit WebSocket receiver - Main entry point
Handles connection, authentication, and data distribution
"""

import asyncio
import uvloop
from loguru import logger as log
from shared.config.settings import get_config
from shared.streaming.deribit import DeribitReceiver

# Apply optimized async policy
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

async def run_receiver():
    """Main application runner"""
    config = get_config()
    log.info("Starting Deribit receiver service")
    
    receiver = DeribitReceiver(
        client_id=config.deribit_client_id,
        client_secret=config.deribit_client_secret,
        sub_account_id=config.deribit_sub_account_id,
        redis_host=config.redis_host
    )
    
    try:
        await receiver.connect()
    except asyncio.CancelledError:
        log.info("Application shutdown requested")
    finally:
        await receiver.disconnect()

if __name__ == "__main__":
    try:
        asyncio.run(run_receiver())
    except KeyboardInterrupt:
        log.info("Application terminated by user")