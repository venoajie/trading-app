# receiver/src/distributing_ws_data.py
"""Data distribution service with maintenance awareness"""

import asyncio
import logging
from typing import Dict, List, Any, Optional

# Third-party imports
import redis.asyncio as aioredis

# Application imports
from shared.db import redis as redis_publish
from shared.db import sqlite as db_mgt
from restful_api.deribit import end_point_params_template as end_point
from receiver.deribit import get_instrument_summary, allocating_ohlc
from shared.utils import caching, error_handling, pickling, string_modification as str_mod, system_tools, template

# Configure logger
log = logging.getLogger(__name__)

# 1. Move combining_ticker_data to top level
async def combining_ticker_data(instruments_name: List) -> List:
    """Combine ticker data from cache or API"""
    result = []
    for instrument_name in instruments_name:
        result_instrument = reading_from_pkl_data("ticker", instrument_name)
        if result_instrument:
            result_instrument = result_instrument[0]
        else:
            result_instrument = await end_point.get_ticker(instrument_name)
        result.append(result_instrument)
    return result

def reading_from_pkl_data(
    end_point: str,
    currency: str,
    status: Optional[str] = None,
) -> Any:
    """Read pickled data from file system"""
    path: str = system_tools.provide_path_for_file(end_point, currency, status)
    return pickling.read_data(path)  

# 2. Add maintenance handler
async def maintenance_handler(queue_general: asyncio.Queue) -> None:
    """Handle maintenance mode operations"""
    while True:
        try:
            # Clear queue to prevent backpressure
            while not queue_general.empty():
                queue_general.get_nowait()
                queue_general.task_done()
            await asyncio.sleep(1)
        except asyncio.QueueEmpty:
            await asyncio.sleep(0.1)

async def caching_distributing_data(
    client_redis: aioredis.Redis,
    currencies: List[str],
    initial_data_subaccount: Dict,
    redis_channels: Dict,
    redis_keys: Dict,
    strategy_attributes: List,
    queue_general: asyncio.Queue,
) -> None:
    """Distributes WebSocket data with maintenance handling"""
    try:
        pubsub = client_redis.pubsub()
        await pubsub.subscribe("system_status")
        
        maintenance_active = asyncio.Event()
        
        async def state_listener():
            """React to centralized state changes"""
            try:
                async for message in pubsub.listen():
                    if message["type"] == "message" and message["channel"] == b"system_status":
                        status = message["data"].decode()
                        if status == "maintenance":
                            log.warning("System entering maintenance mode")
                            maintenance_active.set()
                        else:
                            maintenance_active.clear()
            except asyncio.CancelledError:
                log.info("State listener cancelled")
            except Exception as e:
                log.error(f"State listener failed: {str(e)}")
                await error_handling.parse_error_message_with_redis(client_redis, e)
                
        state_task = asyncio.create_task(state_listener())
        maintenance_task = asyncio.create_task(maintenance_handler(queue_general))

        # Prepare Redis channels
        channels = [
            redis_channels["order_cache_updating"],
            redis_channels["sqlite_record_updating"]
        ]
        for channel in channels:
            await pubsub.subscribe(channel)

        settlement_periods = [
            attr["settlement_period"] 
            for attr in strategy_attributes
            if "settlement_period" in attr
        ]
        settlement_periods = str_mod.remove_double_brackets_in_list(settlement_periods)
        settlement_periods = str_mod.remove_redundant_elements(settlement_periods)

        futures_instruments = await get_instrument_summary.get_futures_instruments(
            currencies, settlement_periods
        )
        instruments_name = futures_instruments["instruments_name"]
        # 3. Now combining_ticker_data is defined
        ticker_all_cached = await combining_ticker_data(instruments_name)

        # Initialize account data
        sub_account_cached = initial_data_subaccount["params"]["data"]
        orders_cached = sub_account_cached["orders_cached"]
        positions_cached = sub_account_cached["positions_cached"]

        query_trades = "SELECT * FROM v_trading_all_active"
        result_template = template.redis_message_template()

        portfolio_lock = asyncio.Lock()
        ticker_lock = asyncio.Lock()

        while True:
            if maintenance_active.is_set():
                await asyncio.sleep(0.1)
                continue

            message_params: Dict = await queue_general.get()
            async with client_redis.pipeline() as pipe:
                try:
                    data: Dict = message_params["data"]
                    message_channel: str = message_params["channel"]
                    currency: str = str_mod.extract_currency_from_text(message_channel)
                    currency_upper = currency.upper()

                    pub_message = {
                        "data": data,
                        "server_time": 0,
                        "currency_upper": currency_upper,
                        "currency": currency,
                    }

                    if "user." in message_channel:
                        await handle_user_message(
                            message_channel,
                            data,
                            pipe,
                            portfolio_lock,
                            redis_channels,
                            result_template,
                            query_trades,
                            orders_cached,
                            positions_cached
                        )

                    # Handle ticker data
                    elif message_channel.startswith("incremental_ticker."):
                        instrument_name_future = message_channel[len("incremental_ticker."):]
                        await handle_incremental_ticker(
                            pipe,
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
                    elif "chart.trades" in message_channel:
                        await handle_chart_trades(
                            pipe,
                            redis_channels["chart_low_high_tick"],
                            message_channel,
                            pub_message,
                            result_template
                        )

                except Exception as error:
                    log.error(f"Error processing message: {error}")
                    await error_handling.parse_error_message_with_redis(client_redis, error)

                await pipe.execute()

    except Exception as error:
        await error_handling.parse_error_message_with_redis(client_redis, error)

# 4. Keep other functions below
async def handle_user_message(
    message_channel: str,
    data: Dict,
    pipe: aioredis.Redis,
    portfolio_lock: asyncio.Lock,
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
                pipe,
                redis_channels["portfolio"],
                result_template,
                data
            )
    elif "changes" in message_channel:
        log.info(f"Account changes: {message_channel}")
        await updating_sub_account(
            pipe,
            orders_cached,
            positions_cached,
            query_trades,
            data,
            redis_channels["sub_account_cache_updating"],
            result_template,
        )
    else:
        if "trades" in message_channel:
            await trades_in_message_channel(
                pipe,
                data,
                redis_channels["my_trade_receiving"],
                orders_cached,
                result_template,
            )
        if "order" in message_channel:
            await order_in_message_channel(
                pipe,
                data,
                redis_channels["order_cache_updating"],
                orders_cached,
                result_template,
            )
        
        # Update trades cache
        my_trades_active_all = await db_mgt.executing_query_with_return(query_trades)
        result_template["params"].update({
            "channel": redis_channels["my_trades_cache_updating"],
            "data": my_trades_active_all
        })
        await redis_publish.publishing_result(pipe, result_template)

async def handle_incremental_ticker(
    pipe: aioredis.Redis,
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

    async with ticker_lock:
        for ticker in ticker_all_cached:
            if instrument_name_future == ticker["instrument_name"]:
                for key in data:
                    if key not in ["stats", "instrument_name", "type"]:
                        ticker[key] = data[key]
                if "stats" in data:
                    for stat_key in data["stats"]:
                        ticker["stats"][stat_key] = data["stats"][stat_key]

    pub_message["data"] = ticker_all_cached
    result_template["params"].update({
        "channel": ticker_channel,
        "data": pub_message
    })
    await redis_publish.publishing_result(pipe, result_template)

    if "PERPETUAL" in instrument_name_future:
        await allocating_ohlc.inserting_open_interest(
            currency,
            "tick",
            f"ohlc1_{currency}_perp_json",
            data,
        )

async def handle_chart_trades(
    pipe: aioredis.Redis,
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

    pub_message.update({
        "instrument_name": instrument_name,
        "resolution": resolution
    })

    result_template["params"].update({
        "channel": chart_channel,
        "data": pub_message
    })
    await redis_publish.publishing_result(pipe, result_template)

async def updating_portfolio(
    pipe: aioredis.Redis,
    portfolio_channel: str,
    result_template: Dict,
    new_data: Dict
) -> None:
    """Update portfolio data in Redis"""
    result_template["params"].update({
        "channel": portfolio_channel,
        "data": new_data
    })
    await redis_publish.publishing_result(pipe, result_template)

async def updating_sub_account(
    pipe: aioredis.Redis,
    orders_cached: List,
    positions_cached: List,
    query_trades: str,
    subaccounts_details_result: List,
    sub_account_channel: str,
    result_template: Dict,
) -> None:
    """Update sub-account data in cache"""
    if subaccounts_details_result:
        open_orders = [o["open_orders"] for o in subaccounts_details_result if "open_orders" in o]
        if open_orders:
            caching.update_cached_orders(orders_cached, open_orders[0], "rest")

        positions = [o["positions"] for o in subaccounts_details_result if "positions" in o]
        if positions:
            caching.positions_updating_cached(positions_cached, positions[0], "rest")

    my_trades_active_all = await db_mgt.executing_query_with_return(query_trades)
    data = {
        "positions": positions_cached,
        "open_orders": orders_cached,
        "my_trades": my_trades_active_all,
    }
    
    result_template["params"].update({
        "channel": sub_account_channel,
        "data": data
    })
    await redis_publish.publishing_result(pipe, result_template)

async def trades_in_message_channel(
    pipe: aioredis.Redis,
    data: List,
    my_trade_receiving_channel: str,
    orders_cached: List,
    result_template: Dict,
) -> None:
    """Process trade messages and update cache"""
    result_template["params"].update({"channel": my_trade_receiving_channel})
    await redis_publish.publishing_result(pipe, result_template)

    for trade in data:
        log.info(f"Trade update: {trade}")
        caching.update_cached_orders(orders_cached, trade)

async def order_in_message_channel(
    pipe: aioredis.Redis,
    data: Dict,
    order_update_channel: str,
    orders_cached: List,
    result_template: Dict,
) -> None:
    """Process order messages and update cache"""
    currency: str = str_mod.extract_currency_from_text(data["instrument_name"])
    caching.update_cached_orders(orders_cached, data)

    order_data = {
        "current_order": data,
        "open_orders": orders_cached,
        "currency": currency,
        "currency_upper": currency.upper(),
    }

    result_template["params"].update({
        "channel": order_update_channel,
        "data": order_data
    })
    await redis_publish.publishing_result(pipe, result_template)