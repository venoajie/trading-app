"""Distributor service consuming from Redis Stream"""
import asyncio
import uvloop
import logging
from loguru import logger as log

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

# Application imports
from core.db.redis import redis_client
from src.services.distributor.deribit import distributing_ws_data
from src.shared.config.settings import REDIS_URL, REDIS_DB

async def stream_consumer():
    """Main stream processing loop"""
    redis = await redis_client.get_pool()
    
    # Ensure consumer group exists
    try:
        await redis.xgroup_create(
            "stream:market_data:deribit", 
            "dispatcher_group", 
            id="0", 
            mkstream=True
        )
    except aioredis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            log.error(f"Error creating consumer group: {e}")
            raise

    while True:
        try:
            # Read messages from stream
            messages = await redis.xreadgroup(
                groupname="dispatcher_group",
                consumername="dispatcher_consumer",
                streams={"stream:market_data:deribit": ">"},
                count=100,
                block=5000
            )
            
            if messages:
                await distributing_ws_data.process_batch(messages[0][1])
                
        except aioredis.ConnectionError:
            log.error("Redis connection lost, reconnecting...")
            await asyncio.sleep(5)
        except Exception as error:
            log.error(f"Stream processing error: {error}")
            await asyncio.sleep(1)

async def main():
    """Service entry point"""
    log.info("Starting distributor service")
    try:
        await stream_consumer()
    except (KeyboardInterrupt, SystemExit):
        log.info("Distributor service shutdown requested")
    except Exception as error:
        log.exception(f"Fatal error in distributor: {error}")
        raise SystemExit(1)

if __name__ == "__main__":
    uvloop.run(main())