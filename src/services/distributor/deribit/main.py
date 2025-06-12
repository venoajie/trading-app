# src\services\distributor\deribit\main.py

"""Distributor service consuming from Redis Stream"""
import asyncio
import uvloop
import logging
from collections import defaultdict

from loguru import logger as log

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

# Application imports
from core.db.redis import redis_client
from core.error_handler import error_handler
from src.services.distributor.deribit import distributing_ws_data
from src.shared.config.constants import ServiceConstants

async def stream_consumer():
    """Main stream processing loop"""
    redis = await redis_client.get_pool()
    stream_name = ServiceConstants.REDIS_STREAMS["MARKET_DATA"]
    group_name = ServiceConstants.REDIS_GROUP_DISPATCHER
    consumer_name = f"{config['services']['name']}_consumer"

    # Ensure consumer group exists
    try:
        await redis.xgroup_create(
            stream_name,  # Use constant from module
            group_name,
            id="0",
            mkstream=True,
        )
        log.info(
            f"Created consumer group '{group_name}' for stream '{stream_name}'"
        )
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.error(f"Error creating consumer group: {e}")
            raise
        else:
            log.info(
                f"Consumer group '{group_name}' already exists"
            )

    log.info("Starting stream processing...")

    while True:
        try:
            # Read messages from stream
            messages = await redis.xreadgroup(
                groupname=group_name,
                consumername==consumer_name,
                streams={stream_name: ">"},
                count=batch_size,
                block=5000,
            )

            if messages:
                stream_name, message_list = messages[0]
                log.info(
                    f"Received {len(message_list)} messages from stream '{stream_name}'"
                )

                # Log first message details
                if message_list:
                    first_msg_id, first_msg_data = message_list[0]
                    payload = {
                        k.decode(): v.decode() for k, v in first_msg_data.items()
                    }
                    log.info(
                        f"First message ID: {first_msg_id} | Channel: {payload.get('channel')}"
                    )

                # Process messages
                await distributing_ws_data.stream_consumer(
                    redis,
                    {
                        "locks": defaultdict(asyncio.Lock),
                        "caches": {"portfolio": {}, "ticker": {}},
                    },
                )

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
