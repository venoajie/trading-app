# receiver/src/distributing_ws_data.py
"""Data distribution service with maintenance awareness"""

import asyncio
import logging
from typing import Dict, List, Any, Optional

# Third-party imports
import redis.asyncio as aioredis

# Application imports
from shared.db import redis as redis_publish
from shared.db_management import sqlite_management as db_mgt
from restful_api.deribit import end_point_params_template as end_point
from receiver.deribit import get_instrument_summary, allocating_ohlc
from shared.utils import caching, error_handling, pickling,string_modification as str_mod, system_tools, template

# Configure logger
log = logging.getLogger(__name__)

def reading_from_pkl_data(
    end_point: str,
    currency: str,
    status: Optional[str] = None,
) -> Any:
    """Read pickled data from file system"""
    path: str = system_tools.provide_path_for_file(end_point, currency, status)
    return pickling.read_data(path)  

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
        # Subscribe to system status channel
        pubsub = client_redis.pubsub()
        await pubsub.subscribe("system_status")
        
        async def maintenance_handler():
            """Handles maintenance mode events"""
            async for message in pubsub.listen():
                if message["type"] == "message":
                    status = message["data"]
                    if status == "maintenance":
                        log.warning("Entering maintenance handling mode")
                        # Clear queue to prevent backpressure
                        while not queue_general.empty():
                            queue_general.get_nowait()
                            queue_general.task_done()
        
        # Start maintenance handler
        asyncio.create_task(maintenance_handler())
        
        # Prepare Redis channels
        chart_low_high_tick_channel: str = redis_channels["chart_low_high_tick"]
        portfolio_channel: str = redis_channels["portfolio"]
        sub_account_cached_channel: str = redis_channels["sub_account_cache_updating"]
        sqlite_updating_channel: str = redis_channels["sqlite_record_updating"]
        order_update_channel: str = redis_channels["order_cache_updating"]
        my_trade_receiving_channel: str = redis_channels["my_trade_receiving"]
        my_trades_channel: str = redis_channels["my_trades_cache_updating"]
        ticker_cached_channel: str = redis_channels["ticker_cache_updating"]

        # Subscribe to additional channels
        channels = [order_update_channel, sqlite_updating_channel]
        for channel in channels:
            await pubsub.subscribe(channel)

        server_time = 0
        portfolio = []

        # Get settlement periods from strategy config
        settlement_periods = get_settlement_period(strategy_attributes)

        # Prepare futures instruments
        futures_instruments = await get_instrument_summary.get_futures_instruments(
            currencies, settlement_periods
        )
        instruments_name = futures_instruments["instruments_name"]
        ticker_all_cached = await combining_ticker_data(instruments_name)

        # Initialize account data
        sub_account_cached_params = initial_data_subaccount["params"]
        sub_account_cached = sub_account_cached_params["data"]
        orders_cached = sub_account_cached["orders_cached"]
        positions_cached = sub_account_cached["positions_cached"]

        query_trades = "SELECT * FROM v_trading_all_active"
        result = template.redis_message_template()

        while True:
            message_params: Dict = await queue_general.get()
            async with client_redis.pipeline() as pipe:
                try:
                    data: Dict = message_params["data"]
                    message_channel: str = message_params["channel"]
                    currency: str = str_mod.extract_currency_from_text(message_channel)
                    currency_upper = currency.upper()

                    pub_message = dict(
                        data=data,
                        server_time=server_time,
                        currency_upper=currency_upper,
                        currency=currency,
                    )

                    if "user." in message_channel:
                        if "portfolio" in message_channel:
                            result["params"].update({"channel": portfolio_channel})
                            result["params"].update({"data": pub_message})
                            await updating_portfolio(
                                pipe,
                                portfolio,
                                portfolio_channel,
                                result,
                            )

                        elif "changes" in message_channel:
                            log.info(f"Account changes: {message_channel}")
                            await updating_sub_account(
                                client_redis,
                                orders_cached,
                                positions_cached,
                                query_trades,
                                data,
                                sub_account_cached_channel,
                                result,
                            )

                        else:
                            if "trades" in message_channel:
                                await trades_in_message_channel(
                                    pipe,
                                    data,
                                    my_trade_receiving_channel,
                                    orders_cached,
                                    result,
                                )

                            if "order" in message_channel:
                                await order_in_message_channel(
                                    pipe,
                                    data,
                                    order_update_channel,
                                    orders_cached,
                                    result,
                                )

                            # Update trades cache
                            my_trades_active_all = await db_mgt.executing_query_with_return(query_trades)
                            result["params"].update({"channel": my_trades_channel})
                            result["params"].update({"data": my_trades_active_all})
                            await redis_publish.publishing_result(pipe, result)

                    # Handle ticker data
                    if message_channel.startswith("incremental_ticker."):
                        instrument_name_future = message_channel[len("incremental_ticker."):]
                        await incremental_ticker_in_message_channel(
                            pipe,
                            currency,
                            data,
                            instrument_name_future,
                            result,
                            pub_message,
                            server_time,
                            ticker_all_cached,
                            ticker_cached_channel,
                        )

                    # Handle chart data
                    if "chart.trades" in message_channel:
                        await chart_trades_in_message_channel(
                            pipe,
                            chart_low_high_tick_channel,
                            message_channel,
                            pub_message,
                            result,
                        )

                except Exception as error:
                    error_handling.parse_error_message(error)

                await pipe.execute()

    except Exception as error:
        await error_handling.parse_error_message_with_redis(client_redis, error)

def compute_notional_value(index_price: float, equity: float) -> float:
    """Compute notional value of equity"""
    return index_price * equity

def get_index(ticker: Dict) -> float:
    """Extract index price from ticker data"""
    try:
        return ticker["index_price"]
    except KeyError:
        return ticker.get("estimated_delivery_price", 0.0)

async def updating_portfolio(
    pipe: aioredis.Redis,
    portfolio: List,
    portfolio_channel: str,
    result: Dict,
) -> None:
    """Update portfolio data in Redis"""
    params = result["params"]
    data = params["data"]

    if not portfolio:
        portfolio.append(data["data"])
    else:
        data_currency = data["data"]["currency"]
        # Find existing portfolio for currency
        portfolio_currency = [p for p in portfolio if p["currency"] == data_currency]
        if portfolio_currency:
            portfolio.remove(portfolio_currency[0])
        portfolio.append(data["data"])

    result["params"]["data"].update({"cached_portfolio": portfolio})
    await redis_publish.publishing_result(pipe, result)

def get_settlement_period(strategy_attributes: List) -> List:
    """Extract settlement periods from strategy config"""
    return str_mod.remove_redundant_elements(
        str_mod.remove_double_brackets_in_list(
            [o["settlement_period"] for o in strategy_attributes]
        )
    )

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

async def trades_in_message_channel(
    pipe: aioredis.Redis,
    data: List,
    my_trade_receiving_channel: str,
    orders_cached: List,
    result: Dict,
) -> None:
    """Process trade messages and update cache"""
    result["params"].update({"channel": my_trade_receiving_channel})
    await redis_publish.publishing_result(pipe, result)

    for trade in data:
        log.info(f"Trade update: {trade}")
        caching.update_cached_orders(orders_cached, trade)

async def order_in_message_channel(
    pipe: aioredis.Redis,
    data: Dict,
    order_update_channel: str,
    orders_cached: List,
    result: Dict,
) -> None:
    """Process order messages and update cache"""
    currency: str = str_mod.extract_currency_from_text(data["instrument_name"])
    caching.update_cached_orders(orders_cached, data)

    order_data = dict(
        current_order=data,
        open_orders=orders_cached,
        currency=currency,
        currency_upper=currency.upper(),
    )

    result["params"].update({"channel": order_update_channel})
    result["params"].update({"data": order_data})
    await redis_publish.publishing_result(pipe, result)

async def incremental_ticker_in_message_channel(
    pipe: aioredis.Redis,
    currency: str,
    data: Dict,
    instrument_name_future: str,
    result: Dict,
    pub_message: Dict,
    server_time: int,
    ticker_all_cached: List,
    ticker_cached_channel: str,
) -> None:
    """Process ticker updates and publish to Redis"""
    current_server_time = data["timestamp"] + server_time if server_time == 0 else data["timestamp"]
    currency_upper = currency.upper()
    server_time = current_server_time if server_time < current_server_time else server_time

    pub_message.update({
        "instrument_name": instrument_name_future,
        "currency_upper": currency_upper
    })

    # Update ticker cache
    for ticker in ticker_all_cached:
        if instrument_name_future == ticker["instrument_name"]:
            # Update regular fields
            for key in data:
                if key not in ["stats", "instrument_name", "type"]:
                    ticker[key] = data[key]
            
            # Update stats fields
            if "stats" in data:
                for stat_key in data["stats"]:
                    ticker["stats"][stat_key] = data["stats"][stat_key]

    pub_message = dict(
        data=ticker_all_cached,
        server_time=server_time,
        instrument_name=instrument_name_future,
        currency_upper=currency_upper,
        currency=currency,
    )

    result["params"].update({"channel": ticker_cached_channel})
    result["params"].update({"data": pub_message})
    await redis_publish.publishing_result(pipe, result)

    # Handle perpetual instruments
    if "PERPETUAL" in instrument_name_future:
        await allocating_ohlc.inserting_open_interest(
            currency,
            "tick",  # WHERE filter
            f"ohlc1_{currency}_perp_json",  # Table name
            data,
        )

async def chart_trades_in_message_channel(
    pipe: aioredis.Redis,
    chart_low_high_tick_channel: str,
    message_channel: str,
    pub_message: Dict,
    result: Dict,
) -> None:
    """Process chart data and publish to Redis"""
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

    result["params"].update({"channel": chart_low_high_tick_channel})
    result["params"].update({"data": pub_message})
    await redis_publish.publishing_result(pipe, result)

async def updating_sub_account(
    client_redis: aioredis.Redis,
    orders_cached: List,
    positions_cached: List,
    query_trades: str,
    subaccounts_details_result: List,
    sub_account_cached_channel: str,
    message_byte_data: Dict,
) -> None:
    """Update sub-account data in cache"""
    if subaccounts_details_result:
        # Update orders cache
        open_orders = [o["open_orders"] for o in subaccounts_details_result if "open_orders" in o]
        if open_orders:
            caching.update_cached_orders(orders_cached, open_orders[0], "rest")

        # Update positions cache
        positions = [o["positions"] for o in subaccounts_details_result if "positions" in o]
        if positions:
            caching.positions_updating_cached(positions_cached, positions[0], "rest")

    # Update trades
    my_trades_active_all = await db_mgt.executing_query_with_return(query_trades)
    data = dict(
        positions=positions_cached,
        open_orders=orders_cached,
        my_trades=my_trades_active_all,
    )
    
    message_byte_data["params"].update({"channel": sub_account_cached_channel})
    message_byte_data["params"].update({"data": data})
    await redis_publish.publishing_result(client_redis, message_byte_data)