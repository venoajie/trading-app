"""Data distribution service with enhanced stream processing"""
import asyncio
import orjson
from collections import defaultdict
from typing import Dict, List, Any, Tuple

# Application imports
from core.db import postgres as pg
from src.scripts.deribit import caching
from src.shared.utils import error_handling, string_modification as str_mod

# Configure logger
from loguru import logger as log

# Constants
STREAM_NAME = "market_data:deribit"
GROUP_NAME = "dispatcher_group"
CONSUMER_NAME = "dispatcher_consumer"
BATCH_SIZE = 100
MAX_RETRIES = 3

async def process_message(
    message_id: str,
    message_data: Dict[bytes, bytes],
    state: Dict[str, Any]
) -> bool:
    """Process single message with error handling and retries"""
    try:
        # Deserialize message
        payload = {k.decode(): v.decode() for k, v in message_data.items()}
        channel = payload["channel"]
        data = orjson.loads(payload["data"])  # Deserialize JSON data
        currency = str_mod.extract_currency_from_text(channel)
        
        # Get currency-specific lock
        async with state['locks'][currency]:
            # Route message to appropriate handler
            if "user.portfolio" in channel:
                await handle_portfolio(currency, data, state)
            elif "incremental_ticker" in channel:
                await handle_ticker(currency, data, state)
            elif "chart.trades" in channel:
                await handle_chart(currency, data, state)
            # Add other handlers as needed
        
        return True
    except Exception as error:
        log.error(f"Message processing failed: {error}")
        return False

async def handle_portfolio(
    currency: str,
    data: Dict,
    state: Dict[str, Any]
) -> None:
    """Handle portfolio updates"""
    # Update in-memory cache
    state['caches']['portfolio'][currency] = data
    
    # Persist to PostgreSQL
    await pg.update_portfolio(currency, data)

async def handle_ticker(
    currency: str,
    data: Dict,
    state: Dict[str, Any]
) -> None:
    """Handle ticker updates"""
    # Update in-memory cache
    state['caches']['ticker'][currency] = data
    
    # Update OHLC data
    await pg.update_ohlc(currency, data)

async def handle_chart(
    currency: str,
    data: Dict,
    state: Dict[str, Any]
) -> None:
    """Handle chart data updates"""
    # Process chart data
    await pg.insert_chart_data(currency, data)

async def stream_consumer(
    redis: Any,
    state: Dict[str, Any]
) -> None:
    """Main stream consumption loop with error handling"""
    retry_count = 0
    dead_letter_queue = []
    
    while True:
        try:
            # Claim pending messages first
            pending = await redis.xpending_range(
                STREAM_NAME,
                GROUP_NAME,
                min_idle_time=30000  # 30 seconds
            )
            if pending:
                await redis.xclaim(
                    STREAM_NAME,
                    GROUP_NAME,
                    CONSUMER_NAME,
                    30000,
                    [msg.id for msg in pending]
                )
            
            # Read new messages
            messages = await redis.xreadgroup(
                groupname=GROUP_NAME,
                consumername=CONSUMER_NAME,
                streams={STREAM_NAME: ">"},
                count=BATCH_SIZE,
                block=5000
            )
            
            # Process messages in parallel
            if messages:
                tasks = []
                for stream, message_list in messages:
                    for message_id, message_data in message_list:
                        tasks.append(
                            process_message(message_id, message_data, state)
                        )
                
                results = await asyncio.gather(*tasks)
                
                # Acknowledge successful messages
                ack_ids = [
                    message_id for i, message_id in enumerate(message_list)
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
            retry_count = 0
            await asyncio.sleep(0.01)
            
        except (ConnectionError, TimeoutError):
            log.warning("Redis connection error, retrying...")
            await asyncio.sleep(min(2 ** retry_count, 30))
            retry_count += 1
        except Exception as error:
            log.error(f"Unexpected error in consumer: {error}")
            await asyncio.sleep(5)

async def handle_dead_letters(messages: List) -> None:
    """Process dead-letter messages with exponential backoff"""
    # Implement your dead-letter handling strategy here
    # For example: write to PostgreSQL, S3, or separate Redis stream
    log.warning(f"Moving {len(messages)} to dead-letter storage")