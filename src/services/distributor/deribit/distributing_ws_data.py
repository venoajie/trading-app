# src\services\distributor\deribit\distributing_ws_data.py

"""Data distribution service with enhanced stream processing"""
import asyncio
import orjson
from collections import defaultdict
from typing import Dict, List, Any, Tuple

# Application imports
from core.db import postgres as pg
from core.db.redis import redis_client
from core.error_handler import error_handler
from src.scripts.deribit import caching
from src.shared.config.constants import ServiceConstants
from src.shared.utils import error_handling, string_modification as str_mod

# Configure logger
from loguru import logger as log


def parse_redis_message(message_data: dict) -> dict:
    """Efficient parser for Redis stream messages"""
    result = {}

    for key, value in message_data.items():

        k = key.decode("utf-8")

        try:
            # Handle 'data' field differently
            if k == "data":
                # Check if it's already a string before parsing
                if isinstance(value, bytes):
                    result[k] = orjson.loads(value)
                else:
                    result[k] = value
            else:
                result[k] = value.decode("utf-8")
        except Exception as e:
            log.warning(f"Error parsing field {k}: {e}")
            result[k] = value

    return result


@error_handler.wrap_async
async def process_message(
    message_id: str, message_data: Dict[bytes, bytes], state: Dict[str, Any]
) -> bool:
    """Process single message with error handling and retries"""
    try:
        # Deserialize message
        payload = redis_client.parse_stream_message(message_data)
        channel = payload["channel"]
        data = payload["data"]
        currency = str_mod.extract_currency_from_text(channel)

        # Add detailed logging
        log.debug(f"Message payload: {payload}")

        # Get currency-specific lock
        async with state["locks"][currency]:
            # Route message to appropriate handler
            if "user.portfolio" in channel:
                await handle_portfolio(currency, data, state)
            elif "incremental_ticker" in channel:
                await handle_ticker(currency, data, state)
            elif "chart.trades" in channel:
                pass
                #await handle_chart(currency, data, state)
            # Add other handlers as needed

        return True
    except Exception as error:
        log.error(f"Message processing failed: {error}")
        return False


async def handle_portfolio(currency: str, data: Dict, state: Dict[str, Any]) -> None:
    """Handle portfolio updates"""
    # Update in-memory cache
    state["caches"]["portfolio"][currency] = data

    # Persist to PostgreSQL
    await pg.update_portfolio(currency, data)


async def handle_ticker(currency: str, data: Dict, state: Dict[str, Any]) -> None:
    """Handle ticker updates"""
    # Update in-memory cache
    state["caches"]["ticker"][currency] = data
    
    log.info(f"data {data}")

    # Update OHLC data
    #await pg.update_ohlc(currency, data)


async def handle_chart(currency: str, data: Dict, state: Dict[str, Any]) -> None:
    """Handle chart data updates"""
    # Process chart data
    await pg.insert_ohlc(currency, data)


async def stream_consumer(redis: Any, state: Dict[str, Any]) -> None:
    """Main stream consumption loop with error handling"""

    # Constants
    BATCH_SIZE = 100
    CONSUMER_NAME = "dispatcher_consumer"
    GROUP_NAME = "dispatcher_group"
    RETRY_COUNT = 0
    dead_letter_queue = []

    while True:

        STREAM_NAME = ServiceConstants.REDIS_STREAMS["MARKET_DATA"]

        try:
            # Read new messages
            messages = await redis.xreadgroup(
                groupname=GROUP_NAME,
                consumername=CONSUMER_NAME,
                streams={STREAM_NAME: ">"},
                count=BATCH_SIZE,
                block=5000,
            )

            # Process messages in parallel
            if messages:
                tasks = []
                for stream, message_list in messages:
                    for message_id, message_data in message_list:

                        tasks.append(process_message(message_id, message_data, state))

                results = await asyncio.gather(*tasks)

                # Acknowledge successful messages
                ack_ids = [
                    message_id
                    for i, message_id in enumerate(message_list)
                    if results[i]
                ]
                if ack_ids:
                    await redis.xack(STREAM_NAME, GROUP_NAME, *ack_ids)

                # Handle failed messages
                for i, success in enumerate(results):
                    if not success:
                        dead_letter_queue.append(message_list[i])
                        if len(dead_letter_queue) > 100:
                            await handle_dead_letters(dead_letter_queue)
                            dead_letter_queue = []

            # Reset retry count on successful cycle
            RETRY_COUNT = 0
            await asyncio.sleep(0.01)

        except (ConnectionError, TimeoutError):
            log.warning("Redis connection error, retrying...")
            await asyncio.sleep(min(2**RETRY_COUNT, 30))
            RETRY_COUNT += 1
        except Exception as error:
            
            
            import traceback

            info = f"{error} \n \n {traceback.format_exc()}"
            
            log.error(f"Unexpected error in consumer: {info}")
            await asyncio.sleep(5)


async def handle_dead_letters(messages: List) -> None:
    """Process dead-letter messages with exponential backoff"""
    # Implement your dead-letter handling strategy here
    # For example: write to PostgreSQL, S3, or separate Redis stream
    log.warning(f"Moving {len(messages)} to dead-letter storage")
