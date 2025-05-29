#!/usr/bin/env python3
"""receiver/main.py: Deribit WebSocket data receiver"""

import asyncio
import logging
import signal
from contextlib import AsyncExitStack
from shared.config import load_config, get_env
from shared.logging import setup_logging
from .deribit_client import DeribitClient
from .redis_publisher import RedisPublisher

# Configuration
CONFIG_PATH = "receiver/config.toml"
config = load_config(CONFIG_PATH)
deribit_cfg = config["deribit"]
redis_cfg = config["redis"]
log_cfg = config["logging"]

# Initialize logging
logger = setup_logging("receiver", log_cfg)

class Receiver:
    def __init__(self):
        self.exit_stack = AsyncExitStack()
        self.is_running = True
        signal.signal(signal.SIGTERM, self.handle_shutdown)
        signal.signal(signal.SIGINT, self.handle_shutdown)

    def handle_shutdown(self, signum, frame):
        logger.info("Shutdown signal received")
        self.is_running = False

    async def run(self):
        """Main application loop"""
        async with self.exit_stack:
            # Initialize clients
            deribit = DeribitClient(
                deribit_cfg["ws_url"],
                deribit_cfg["channels"]
            )
            redis_pub = RedisPublisher(
                redis_cfg["stream_name"],
                redis_cfg["max_queue_size"]
            )
            
            # Connect to services
            await deribit.connect()
            await redis_pub.connect()
            
            logger.info("Receiver started successfully")
            
            # Main processing loop
            async for message in deribit.receive_messages():
                if not self.is_running:
                    break
                
                # Process and publish message
                if "params" in message:  # Filter actual data messages
                    await redis_pub.publish(message)
                    logger.debug(f"Published message: {message['params']['channel']}")

if __name__ == "__main__":
    app = Receiver()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(app.run())
    logger.info("Receiver shutdown complete")