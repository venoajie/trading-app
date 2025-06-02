# -*- coding: utf-8 -*-

# built ins
import asyncio

# installed
import uvloop
from loguru import logger as log

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

from streaming_helper.db_management import redis_client, sqlite_management as db_mgt
from streaming_helper.restful_api.deribit import end_point_params_template as end_point
from streaming_helper.data_announcer.deribit import (
    get_instrument_summary,
    allocating_ohlc,
)
from streaming_helper.utilities import (
    caching,
    error_handling,
    pickling,
    string_modification as str_mod,
    system_tools,
    template,
)


async def caching_distributing_data(
    client_redis: object,
    currencies: list,
    initial_data_subaccount: dict,
    redis_channels: list,
    redis_keys: list,
    strategy_attributes,
    queue_general: object,
) -> None:
    """
    my_trades_channel:
    + send messages that "high probabilities" trade DB has changed
        sender: redis publisher + sqlite insert, update & delete
    + updating trading cache at end user
        consumer: fut spread, hedging, cancelling
    + checking data integrity
        consumer: app data cleaning/size reconciliation

    sub_account_channel:
    update method: REST
    + send messages that sub_account has changed
        sender: deribit API module
    + updating sub account cache at end user
        consumer: fut spread, hedging, cancelling
    + checking data integrity
        consumer: app data cleaning/size reconciliation

    sending_order_channel:
    + send messages that an order has allowed to submit
        sender: fut spread, hedging
    + send order to deribit
        consumer: processing order

    is_order_allowed_channel:
    + send messages that data has reconciled each other and order could be processed
        sender: app data cleaning/size reconciliation, check double ids
    + processing order
        consumer: fut spread, hedging

    """

    try:

        # preparing redis connection
        pubsub = client_redis.pubsub()

        chart_low_high_tick_channel: str = redis_channels["chart_low_high_tick"]
        portfolio_channel: str = redis_channels["portfolio"]
        sub_account_cached_channel: str = redis_channels["sub_account_cache_updating"]
        sqlite_updating_channel: str = redis_channels["sqlite_record_updating"]
        order_update_channel: str = redis_channels["order_cache_updating"]
        my_trade_receiving_channel: str = redis_channels["my_trade_receiving"]
        my_trades_channel: str = redis_channels["my_trades_cache_updating"]
        abnormal_trading_notices: str = redis_channels["abnormal_trading_notices"]

        ticker_cached_channel: str = redis_channels["ticker_cache_updating"]

        # prepare channels placeholders
        channels = [
            order_update_channel,
            sqlite_updating_channel,
        ]

        # subscribe to channels
        [await pubsub.subscribe(o) for o in channels]

        server_time = 0

        portfolio = []

        settlement_periods = get_settlement_period(strategy_attributes)

        futures_instruments = await get_instrument_summary.get_futures_instruments(
            currencies, settlement_periods
        )

        instruments_name = futures_instruments["instruments_name"]

        ticker_all_cached = await combining_ticker_data(instruments_name)

        sub_account_cached_params = initial_data_subaccount["params"]

        sub_account_cached = sub_account_cached_params["data"]

        orders_cached = sub_account_cached["orders_cached"]

        positions_cached = sub_account_cached["positions_cached"]

        query_trades = f"SELECT * FROM  v_trading_all_active"

        result = template.redis_message_template()

        while True:

            message_params: str = await queue_general.get()

            async with client_redis.pipeline() as pipe:

                try:

                    data: dict = message_params["data"]

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

                            log.critical(message_channel)
                            log.warning(data)

                            await updating_sub_account(
                                client_redis,
                                orders_cached,
                                positions_cached,
                                query_trades,
                                data,
                                sub_account_cached_channel,
                            )

                        else:

                            log.critical(message_channel)
                            log.warning(data)

                            result["params"].update({"data": data})

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

                            my_trades_active_all = (
                                await db_mgt.executing_query_with_return(query_trades)
                            )

                            result["params"].update({"channel": my_trades_channel})
                            result["params"].update({"data": my_trades_active_all})

                            await redis_client.publishing_result(
                                pipe,
                                result,
                            )

                    instrument_name_future = (message_channel)[19:]
                    if (
                        message_channel
                        == f"incremental_ticker.{instrument_name_future}"
                    ):

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

                    if "chart.trades" in message_channel:

                        await chart_trades_in_message_channel(
                            pipe,
                            chart_low_high_tick_channel,
                            message_channel,
                            pub_message,
                            result,
                        )

                except:

                    pass

                await pipe.execute()

    except Exception as error:

        await error_handling.parse_error_message_with_redis(
            client_redis,
            error,
        )


def compute_notional_value(
    index_price: float,
    equity: float,
) -> float:
    """ """
    return index_price * equity


def get_index(ticker: dict) -> float:

    try:

        index_price = ticker["index_price"]

    except:

        index_price = []

    if index_price == []:
        index_price = ticker["estimated_delivery_price"]

    return index_price


async def updating_portfolio(
    pipe: object,
    portfolio: list,
    portfolio_channel: str,
    result: dict,
) -> None:

    params = result["params"]

    data = params["data"]

    if portfolio == []:
        portfolio.append(data["data"])

    else:
        data_currency = data["data"]["currency"]
        portfolio_currency = [o for o in portfolio if data_currency in o["currency"]]

        if portfolio_currency:
            portfolio.remove(portfolio_currency[0])

        portfolio.append(data["data"])

    result["params"]["data"].update({"cached_portfolio": portfolio})

    await redis_client.publishing_result(
        pipe,
        result,
    )


def get_settlement_period(strategy_attributes: list) -> list:

    return str_mod.remove_redundant_elements(
        str_mod.remove_double_brackets_in_list(
            [o["settlement_period"] for o in strategy_attributes]
        )
    )


async def combining_ticker_data(instruments_name: str) -> list:
    """_summary_
    https://blog.apify.com/python-cache-complete-guide/]
    https://medium.com/@jodielovesmaths/memoization-in-python-using-cache-36b676cb21ef
    data caching
    https://medium.com/@ryan_forrester_/python-return-statement-complete-guide-138c80bcfdc7

    Args:
        instrument_ticker (_type_): _description_

    Returns:
        _type_: _description_
    """

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
    status: str = None,
) -> dict:
    """ """

    path: str = system_tools.provide_path_for_file(end_point, currency, status)
    return pickling.read_data(path)


async def trades_in_message_channel(
    pipe: object,
    data: list,
    my_trade_receiving_channel: str,
    orders_cached: list,
    result: dict,
) -> None:
    """

    #! result example

    [
        {
            'label': 'customShort-open-1743595398537',
            'timestamp': 1743595416236,
            'state': 'filled',
            'price': 1870.05,
            'direction': 'sell',
            'index_price': 1869.77,
            'profit_loss': 0.0,
            'instrument_name': 'ETH-PERPETUAL',
            'trade_seq': 175723279,
            'api': True,
            'mark_price': 1869.94,
            'amount': 1.0,
            'order_id': 'ETH-64159311162',
            'matching_id': None,
            'tick_direction': 0,
            'fee': 0.0,
            'mmp': False,
            'post_only': True,
            'reduce_only': False,
            'self_trade': False,
            'contracts': 1.0,
            'trade_id': 'ETH-242752309',
            'fee_currency': 'ETH',
            'order_type': 'limit',
            'risk_reducing': False,
            'liquidity': 'M'
        }
    ]

    """

    result["params"].update({"channel": my_trade_receiving_channel})

    await redis_client.publishing_result(
        pipe,
        result,
    )

    for trade in data:

        log.info(trade)

        caching.update_cached_orders(
            orders_cached,
            trade,
        )


async def order_in_message_channel(
    pipe: object,
    data: list,
    order_update_channel: str,
    orders_cached: list,
    result: dict,
) -> None:

    currency: str = str_mod.extract_currency_from_text(data["instrument_name"])

    caching.update_cached_orders(
        orders_cached,
        data,
    )

    data = dict(
        current_order=data,
        open_orders=orders_cached,
        currency=currency,
        currency_upper=currency.upper(),
    )

    result["params"].update({"channel": order_update_channel})
    result["params"].update({"data": data})

    await redis_client.publishing_result(
        pipe,
        result,
    )


async def incremental_ticker_in_message_channel(
    pipe: object,
    currency: str,
    data: list,
    instrument_name_future: str,
    result: dict,
    pub_message: dict,
    server_time: int,
    ticker_all_cached: list,
    ticker_cached_channel: str,
) -> None:

    # extract server time from data
    current_server_time = (
        data["timestamp"] + server_time if server_time == 0 else data["timestamp"]
    )

    currency_upper = currency.upper()

    # updating current server time
    server_time = (
        current_server_time if server_time < current_server_time else server_time
    )

    pub_message.update({"instrument_name": instrument_name_future})
    pub_message.update({"currency_upper": currency_upper})

    for item in data:

        if "stats" not in item and "instrument_name" not in item and "type" not in item:
            [
                o
                for o in ticker_all_cached
                if instrument_name_future in o["instrument_name"]
            ][0][item] = data[item]

        if "stats" in item:

            data_orders_stat = data[item]

            for item in data_orders_stat:
                [
                    o
                    for o in ticker_all_cached
                    if instrument_name_future in o["instrument_name"]
                ][0]["stats"][item] = data_orders_stat[item]

    pub_message = dict(
        data=ticker_all_cached,
        server_time=server_time,
        instrument_name=instrument_name_future,
        currency_upper=currency_upper,
        currency=currency,
    )

    result["params"].update({"channel": ticker_cached_channel})
    result["params"].update({"data": pub_message})

    await redis_client.publishing_result(
        pipe,
        result,
    )
    if "PERPETUAL" in instrument_name_future:

        WHERE_FILTER_TICK: str = "tick"

        resolution = 1

        TABLE_OHLC1: str = f"ohlc{resolution}_{currency}_perp_json"

        await allocating_ohlc.inserting_open_interest(
            currency,
            WHERE_FILTER_TICK,
            TABLE_OHLC1,
            data,
        )


async def chart_trades_in_message_channel(
    pipe: object,
    chart_low_high_tick_channel: str,
    message_channel: str,
    pub_message: list,
    result: dict,
) -> None:

    try:
        resolution = int(message_channel.split(".")[3])

    except:
        resolution = message_channel.split(".")[3]

    pub_message.update({"instrument_name": message_channel.split(".")[2]})
    pub_message.update({"resolution": resolution})

    result["params"].update({"channel": chart_low_high_tick_channel})
    result["params"].update({"data": pub_message})

    await redis_client.publishing_result(
        pipe,
        result,
    )


async def updating_sub_account(
    client_redis: object,
    orders_cached: list,
    positions_cached: list,
    query_trades: str,
    subaccounts_details_result: list,
    sub_account_cached_channel: str,
    message_byte_data: dict,
) -> None:

    if subaccounts_details_result:

        open_orders = [o["open_orders"] for o in subaccounts_details_result]

        if open_orders:
            caching.update_cached_orders(
                orders_cached,
                open_orders[0],
                "rest",
            )
        positions = [o["positions"] for o in subaccounts_details_result]

        if positions:
            caching.positions_updating_cached(
                positions_cached,
                positions[0],
                "rest",
            )

    my_trades_active_all = await db_mgt.executing_query_with_return(query_trades)

    data = dict(
        positions=positions_cached,
        open_orders=orders_cached,
        my_trades=my_trades_active_all,
    )

    message_byte_data["params"].update({"channel": sub_account_cached_channel})
    message_byte_data["params"].update({"data": data})

    await redis_client.publishing_result(
        client_redis,
        message_byte_data,
    )
