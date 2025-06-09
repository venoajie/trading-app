# receiver/src/distributing_ws_data.py

"""Data distribution service with maintenance awareness"""

import asyncio
import logging
import orjson
from typing import Dict, List, Any, Optional, Tuple

# Third-party imports
import redis.asyncio as aioredis

# Application imports
from core.db import redis as redis_publish
from core.db.postgres import fetch, schema

from src.scripts.deribit.restful_api import end_point_params_template as end_point
from src.scripts.deribit import caching, get_instrument_summary
from src.services.receiver.deribit import allocating_ohlc
from src.shared.utils import error_handling, pickling, string_modification as str_mod, system_tools, template

# Configure logger
log = logging.getLogger(__name__)
from loguru import logger as log

async def combining_ticker_data(instruments_name: List) -> List:
    """Combine ticker data from cache or API with error handling"""

    result = []
    for instrument_name in instruments_name:
        try:
            result_instrument = reading_from_pkl_data("ticker", instrument_name)
            if result_instrument:
                result_instrument = result_instrument[0]
            else:
                result_instrument = await end_point.get_ticker(instrument_name)
            result.append(result_instrument)
        except Exception as e:
            log.error(f"Error loading ticker for {instrument_name}: {str(e)}")
    return result

def reading_from_pkl_data(
    end_point: str,
    currency: str,
    status: Optional[str] = None,
) -> Any:
    """Read pickled data from file system"""
    path: str = system_tools.provide_path_for_file(end_point, currency, status)
    return pickling.read_data(path)  


async def process_message_batch(
    client_redis: aioredis.Redis,
    messages: List,
    processing_state: Dict[str, Any]
) -> None:
    """
    Process batch of messages from Redis stream
    
    Args:
        client_redis: Redis connection
        messages: Batch of (message_id, message_data) tuples
        processing_state: Shared processing context
    """
        
    pending = await client_redis.xpending_range(
        "stream:market_data",
        "dispatcher_group",
        min_idle_time=30000  # 30 seconds
    )
    if pending:
        await client_redis.xclaim(
            "stream:market_data",
            "dispatcher_group",
            "dispatcher_consumer",
            30000,
            [msg.id for msg in pending]
        )
        
    for message_id, message_data in messages:
        try:
            # Parse message payload
            
            payload = orjson.loads(message_data[b'data'])
            channel = payload["channel"]
            data = payload["data"]
            timestamp = payload["timestamp"]
            
            # Extract processing parameters
            currency = str_mod.extract_currency_from_text(channel)
            currency_upper = currency.upper()
            instrument_name = channel.split('.')[-1] if '.' in channel else ""
                                
            current_server_time = (timestamp + server_time if server_time == 0 else timestamp)
            # updating current server time
            server_time = (
                current_server_time if server_time < current_server_time else server_time
            )

            pub_message = dict(
                data=data,
                server_time=server_time,
                currency_upper=currency_upper,
                currency=currency,
            )

            log.info(f"Processing message from channel: {channel} {currency}")
            
            if "user." in channel:
                await handle_user_message(
                    channel,
                    data,
                    portfolio_lock,
                    portfolio,  # Pass portfolio storage
                    redis_channels,
                    result_template,
                    query_trades,
                    orders_cached,
                    positions_cached
                )

            # Handle ticker data
            elif "incremental_ticker" in channel:
                
                instrument_name_future = channel[len("incremental_ticker."):]
                await handle_incremental_ticker(
                    currency,
                    data,
                    instrument_name_future,
                    result_template,
                    pub_message,
                    ticker_all_cached,
                    redis_channels["ticker_cache_updating"],
                    ticker_lock
                )

            # Handle chart data
            elif "chart.trades" in channel:
                await handle_chart_trades(
                    redis_channels["chart_low_high_tick"],
                    channel,
                    pub_message,
                    result_template
                )

            
            # Acknowledge successful processing
            await client_redis.acknowledge_message(
                stream_name="stream:market_data",
                group_name="dispatcher_group",
                message_id=message_id
            )
            
        except Exception as error:
            log.error(f"Message processing failed: {error}", exc_info=True)
            # Consider dead-letter queue for failed messages
            

async def stream_consumer(
    client_redis: aioredis.Redis,
    processing_state: Dict[str, Any]
) -> None:
    """
    Main stream consumer loop with backpressure management
    
    Args:
        client_redis: Redis connection
        processing_state: Shared processing context
    """
    # Ensure consumer group exists
    await client_redis.ensure_consumer_group(
        stream="stream:market_data",
        group_name="dispatcher_group"
    )
    
    while True:
        try:
            # Trim stream periodically to prevent memory issues
            stream_length = await client_redis.xlen("stream:market_data")
            if stream_length > 15000:  # Trim when 50% over maxlen
                await client_redis.trim_stream("stream:market_data", maxlen=10000)
            
            # Fetch message batch
            messages = await client_redis.xreadgroup(
                group_name=group_name,
                consumername=consumer_name,
                streams={stream_name: "0-0"},
                count=50,
                block=0  # Non-blocking
            )
            if not messages:
                await asyncio.sleep(0.05)  # Cooperative yield
                
            # Process batch asynchronously
            await process_message_batch(client_redis, messages, processing_state)
            
        except aioredis.ConnectionError:
            log.error("Redis connection lost, reconnecting...")
            await asyncio.sleep(5)
        except Exception as error:
            log.error(f"Consumer loop error: {error}", exc_info=True)
            await asyncio.sleep(1)
            
            
async def caching_distributing_data(
    client_redis: aioredis.Redis,
    currencies: List[str],
    initial_data_subaccount: Dict,
    redis_channels: Dict,
    redis_keys: Dict,
    strategy_attributes: List,
) -> None:
    """Distributes WebSocket data with maintenance handling"""
    state_task = None
        # Initialize processing state
    
    processing_state = {
        'currencies': currencies,
        'redis_channels': redis_channels,
        # ... other state ...,
        'str_mod': string_modification  # Utility instance
    }
    
    # Start stream consumer
    consumer_task = asyncio.create_task(
        stream_consumer(client_redis, processing_state)
    )
        
    
    try:
        await asyncio.gather(consumer_task)
    except asyncio.CancelledError:
        log.info("Data distribution stopped")
    except Exception as error:
        log.error(f"Distribution failed: {error}", exc_info=True)
        raise
    
# 4. Keep other functions below
async def handle_user_message(
    message_channel: str,
    data: Dict,
    portfolio_lock: asyncio.Lock,
    portfolio: List[Dict],  # Portfolio storage
    redis_channels: Dict,
    result_template: Dict,
    query_trades: str,
    orders_cached: List,
    positions_cached: List
) -> None:
    """Handle user-related messages"""
    if "portfolio" in message_channel:
        async with portfolio_lock:
            await updating_portfolio(
                portfolio,  # Pass portfolio storage
                redis_channels["portfolio"],
                result_template,
                data
            )
            
    elif "changes" in message_channel:
        log.info(f"Account changes: {message_channel} {data}")
        
        if "open_orders" in data["orders"]:
            log.info(data["orders"])
        if "positions" in data["positions"]:
            log.info(data["positions"])
        if "trade" in data["trades"]:
            log.info(data["trades"])
    else:
        if "trades" in message_channel:
            await trades_in_message_channel(
                data,
                redis_channels["my_trade_receiving"],
                orders_cached,
                result_template,
            )
        if "order" in message_channel:
            await order_in_message_channel(
                data,
                redis_channels["order_cache_updating"],
                orders_cached,
                result_template,
            )
        
        # Update trades cache
        try:
            
            my_trades_active_all = await fetch(query_trades)
        
        except Exception as e:
            log.error(f"Error updating trades cache: {str(e)}")

async def handle_incremental_ticker(
    currency: str,
    data: Dict,
    instrument_name_future: str,
    result_template: Dict,
    pub_message: Dict,
    ticker_all_cached: List,
    ticker_channel: str,
    ticker_lock: asyncio.Lock
) -> None:
    """Handle incremental ticker updates"""
    current_time = data.get("timestamp", 0)
    pub_message["server_time"] = current_time
    
    pub_message.update({
        "instrument_name": instrument_name_future,
        "currency_upper": currency.upper()
    })
    
    log.info(f"Processing ticker update for {data} at {instrument_name_future}")
    
    async with ticker_lock:
        for ticker in ticker_all_cached:
            if instrument_name_future == ticker["instrument_name"]:
                # Update regular fields
                for key in data:
                    if key not in ["stats", "instrument_name", "type"]:
                        ticker[key] = data[key]
                
                # Update stats fields
                if "stats" in data:
                    ticker_stats = ticker.setdefault("stats", {})
                    for stat_key in data["stats"]:
                        ticker_stats[stat_key] = data["stats"][stat_key]

    # Handle perpetual instruments
    if "PERPETUAL" in instrument_name_future:
        try:

            await allocating_ohlc.inserting_open_interest(
                currency,
                "tick",
                f"ohlc1_{currency}_perp_json",
                data,
            )
        except Exception as e:
            error_handling.parse_error_message(e)

async def handle_chart_trades(
    chart_channel: str,
    message_channel: str,
    pub_message: Dict,
    result_template: Dict
) -> None:
    """Handle chart trade messages"""
    try:
        parts = message_channel.split(".")
        resolution = int(parts[3]) if len(parts) > 3 else 1
        instrument_name = parts[2]
    except (IndexError, ValueError):
        resolution = 1
        instrument_name = message_channel.split(".")[2]

async def updating_portfolio(
    portfolio: List[Dict],  # Portfolio storage
    portfolio_channel: str,
    result_template: Dict,
    new_data: Dict
) -> None:
    """Update portfolio data in Redis with accumulation"""
    currency = new_data.get("currency")
    if not currency:
        log.warning("Missing currency in portfolio data")
        return
    
    # Update portfolio storage
    found = False
    for i, item in enumerate(portfolio):
        if item.get("currency") == currency:
            portfolio[i] = new_data
            found = True
            break
    
    if not found:
        portfolio.append(new_data)
    
    # Prepare data to publish
    data_to_publish = {"cached_portfolio": portfolio}
    
async def updating_sub_account(
    orders_cached: List,
    positions_cached: List,
    query_trades: str,
    subaccounts_details_result: List,
    sub_account_channel: str,
    result_template: Dict,
) -> None:
    
    """Update sub-account data in cache"""
    if subaccounts_details_result:
        # Update orders cache
        for o in subaccounts_details_result:
            if "open_orders" in o:
                caching.update_cached_orders(orders_cached, o["open_orders"], "rest")
                break

        # Update positions cache
        log.info(f"subaccounts_details_result {subaccounts_details_result}")
        for o in subaccounts_details_result:
            if "positions" in o:
                positions = [o["positions"] for o in subaccounts_details_result]
                log.info(f"positions_cached {positions_cached}")
                caching.positions_updating_cached(positions_cached, positions[0], "rest")
                break

    # Update trades
    try:
        my_trades_active_all = await fetch(query_trades)
        
        data = {
            "positions": positions_cached,
            "open_orders": orders_cached,
            "my_trades": my_trades_active_all,
        }
    
    except Exception as e:
        log.error(f"Error updating sub-account: {str(e)}")

async def trades_in_message_channel(
    data: List,
    my_trade_receiving_channel: str,
    orders_cached: List,
    result_template: Dict,
) -> None:
    """Process trade messages and update cache"""

    for trade in data:
        try:
            log.info(f"Trade update: {trade}")
            caching.update_cached_orders(orders_cached, trade)
        except Exception as e:
            log.error(f"Error updating trade cache: {str(e)}")

async def order_in_message_channel(
    data: Dict,
    order_update_channel: str,
    orders_cached: List,
    result_template: Dict,
) -> None:
    """Process order messages and update cache"""
    try:
        currency: str = str_mod.extract_currency_from_text(data["instrument_name"])
        caching.update_cached_orders(orders_cached, data)

        order_data = {
            "current_order": data,
            "open_orders": orders_cached,
            "currency": currency,
            "currency_upper": currency.upper(),
        }

    except Exception as e:
        log.error(f"Error processing order: {str(e)}")