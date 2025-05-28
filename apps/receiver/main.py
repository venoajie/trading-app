"""
Deribit WebSocket receiver - Main entry point
Simplified version for immediate functionality
"""

import asyncio
import uvloop
from loguru import logger as log
import os

# Apply optimized async policy
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

async def run_receiver():
    """Main application runner"""
    log.info("Starting Deribit receiver service")
    # Add your receiver logic here
    while True:
        log.debug("Receiver running...")
        await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        asyncio.run(run_receiver())
    except KeyboardInterrupt:
        log.info("Application terminated by user")