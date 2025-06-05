# src\services\executor\deribit\cancelling_active_orders.py

# built ins
import asyncio

# installed
from loguru import logger as log

# user defined formula
from core.db import sqlite as db_mgt
from src.scripts.deribit import get_instrument_summary, starter
from src.scripts.deribit import get_published_messages
from src.scripts.deribit import subscribing_to_channels
from src.scripts.deribit.restful_api import end_point_params_template
from src.scripts.deribit.strategies.cash_carry import combo_auto as combo
from src.scripts.deribit.strategies.hedging import hedging_spot
from src.shared.utils import error_handling, string_modification as str_mod, system_tools, template

async def cancelling_orders(
    client_id: str,
    client_secret: str,
    currencies: list,
    client_redis: object,
    config_app: list,
    initial_data_subaccount: dict,
    redis_channels: list,
    strategy_attributes: list,
) -> None:
    """ """

    try:

        # connecting to redis pubsub
        pubsub: object = client_redis.pubsub()

        # instantiate private connection
        api_request: object = end_point_params_template.SendApiRequest(
            client_id, client_secret
        )

        # subscribe to channels
        await subscribing_to_channels.redis_channels(
            pubsub,
            redis_channels,
            "cancelling_active_orders",
        )

        relevant_tables = config_app["relevant_tables"][0]

        order_db_table = relevant_tables["orders_table"]

        strategy_attributes_active = [
            o for o in strategy_attributes if o["is_active"] == True
        ]

        active_strategies = [o["strategy_label"] for o in strategy_attributes_active]

        relevant_tables = config_app["relevant_tables"][0]

        order_db_table = relevant_tables["orders_table"]

        settlement_periods = get_settlement_period(strategy_attributes)

        futures_instruments = await get_instrument_summary.get_futures_instruments(
            currencies,
            settlement_periods,
        )

        instrument_attributes_futures_all = futures_instruments["active_futures"]

        # get redis channels
        market_analytics_channel: str = redis_channels["market_analytics_update"]
        ticker_cached_channel: str = redis_channels["ticker_cache_updating"]
        portfolio_channel: str = redis_channels["portfolio"]
        my_trades_channel: str = redis_channels["my_trades_cache_updating"]
        order_update_channel: str = redis_channels["order_cache_updating"]
        sub_account_cached_channel: str = redis_channels["sub_account_cache_updating"]

        cached_ticker_all = []

        not_cancel = True

        sub_account_cached_params = initial_data_subaccount["params"]

        sub_account_cached = sub_account_cached_params["data"]

        cached_orders = sub_account_cached["orders_cached"]

        query_trades = f"SELECT * FROM  v_trading_all_active"

        result_template = template.redis_message_template()

        my_trades_active_from_db = await db_mgt.executing_query_with_return(
            query_trades
        )

        initial_data_my_trades_active = starter.my_trades_active_combining(
            my_trades_active_from_db,
            my_trades_channel,
            result_template,
        )

        my_trades_active_all = initial_data_my_trades_active["params"]["data"]

        # get portfolio from exchg
        portfolio_from_exchg = await api_request.get_subaccounts()

        initial_data_portfolio = starter.portfolio_combining(
            portfolio_from_exchg,
            portfolio_channel,
            result_template,
        )

        portfolio_all = initial_data_portfolio["params"]["data"]

        market_condition_all = []

        while not_cancel:

            try:

                message_byte = await pubsub.get_message()

                params = await get_published_messages.get_redis_message(message_byte)

                data, message_channel = params["data"], params["channel"]

                if market_analytics_channel in message_channel:

                    market_condition_all = data

                if portfolio_channel in message_channel:

                    portfolio_all = data["cached_portfolio"]

                if sub_account_cached_channel in message_channel:

                    cached_orders = data["open_orders"]

                    my_trades_active_all = data["my_trades"]

                if my_trades_channel in message_channel:

                    my_trades_active_all = data

                if order_update_channel in message_channel:

                    cached_orders = data["open_orders"]

                if ticker_cached_channel in message_channel and market_condition_all:

                    cached_ticker_all = data["data"]

                    server_time = data["server_time"]

                    currency_upper = data["currency_upper"]

                    currency_lower = currency_upper.lower()

                    instrument_name_perpetual = f"{currency_upper}-PERPETUAL"
                    
                    print(f"instrument_name_perpetual {instrument_name_perpetual}") 
                    print(f"market_condition_all {market_condition_all}")

                    market_condition = [
                        o
                        for o in market_condition_all
                        if instrument_name_perpetual in o["instrument_name"]
                    ][0]

                    portfolio = [
                        o
                        for o in portfolio_all
                        if currency_upper == o["currency"].upper()
                    ][0]
                    #! need to update currency to upper from starter

                    equity: float = portfolio["equity"]

                    ticker_perpetual_instrument_name = [
                        o
                        for o in cached_ticker_all
                        if instrument_name_perpetual in o["instrument_name"]
                    ][0]

                    index_price = get_index(ticker_perpetual_instrument_name)

                    my_trades_currency_all_transactions: list = (
                        []
                        if not my_trades_active_all
                        else [
                            o
                            for o in my_trades_active_all
                            if currency_upper in o["instrument_name"]
                        ]
                    )

                    my_trades_currency_all: list = (
                        []
                        if my_trades_currency_all_transactions == 0
                        else [
                            o
                            for o in my_trades_currency_all_transactions
                            if o["instrument_name"]
                            in [
                                o["instrument_name"]
                                for o in instrument_attributes_futures_all
                            ]
                        ]
                    )

                    orders_currency = (
                        []
                        if not cached_orders
                        else [
                            o
                            for o in cached_orders
                            if currency_upper in o["instrument_name"]
                        ]
                    )

                    if index_price is not None and equity > 0:

                        my_trades_currency: list = [
                            o for o in my_trades_currency_all if o["label"] is not None
                        ]

                        notional: float = compute_notional_value(index_price, equity)

                        for strategy in active_strategies:

                            strategy_params = [
                                o
                                for o in strategy_attributes
                                if o["strategy_label"] == strategy
                            ][0]

                            my_trades_currency_strategy = [
                                o
                                for o in my_trades_currency
                                if strategy in (o["label"])
                            ]

                            orders_currency_strategy = (
                                []
                                if not orders_currency
                                else [
                                    o
                                    for o in orders_currency
                                    if strategy in (o["label"])
                                ]
                            )

                            await cancelling_double_ids(
                                api_request,
                                client_secret,
                                order_db_table,
                                orders_currency_strategy,
                            )

                            if "futureSpread" in strategy:

                                strategy_params = [
                                    o
                                    for o in strategy_attributes
                                    if o["strategy_label"] == strategy
                                ][0]

                                combo_auto = combo.ComboAuto(
                                    strategy,
                                    strategy_params,
                                    orders_currency_strategy,
                                    server_time,
                                    market_condition,
                                    my_trades_currency_strategy,
                                )

                                if orders_currency_strategy:
                                    for order in orders_currency_strategy:
                                        cancel_allowed: dict = (
                                            await combo_auto.is_cancelling_orders_allowed(
                                                order,
                                                server_time,
                                            )
                                        )

                                        if cancel_allowed["cancel_allowed"]:
                                            await if_cancel_is_true(
                                                api_request,
                                                order_db_table,
                                                cancel_allowed,
                                            )

                                            not_cancel = False

                            if "hedgingSpot" in strategy:

                                max_position: int = notional * -1
                                
                                print(f"strategy {strategy}")                                
                                print(f"strategy_params {strategy_params}")
                                print(f"my_trades_currency_strategy {my_trades_currency_strategy}")

                                hedging = hedging_spot.HedgingSpot(
                                    strategy,
                                    strategy_params,
                                    max_position,
                                    my_trades_currency_strategy,
                                    market_condition,
                                    index_price,
                                    my_trades_currency_all,
                                )

                                if orders_currency_strategy:

                                    for order in orders_currency_strategy:
                                        cancel_allowed: dict = (
                                            await hedging.is_cancelling_orders_allowed(
                                                order,
                                                orders_currency_strategy,
                                                server_time,
                                            )
                                        )

                                        if cancel_allowed["cancel_allowed"]:
                                            await if_cancel_is_true(
                                                api_request,
                                                order_db_table,
                                                cancel_allowed,
                                            )

                                            not_cancel = False

            except Exception as error:

                await error_handling.parse_error_message_with_redis(
                    client_redis,
                    error,
                )

                continue

            finally:
                await asyncio.sleep(0.001)

    except Exception as error:

        await error_handling.parse_error_message_with_redis(
            client_redis,
            error,
        )


def get_settlement_period(strategy_attributes) -> list:

    return str_mod.remove_redundant_elements(
        str_mod.remove_double_brackets_in_list(
            [o["settlement_period"] for o in strategy_attributes]
        )
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


async def cancel_the_cancellables(
    api_request: object,
    order_db_table: str,
    currency: str,
    cancellable_strategies: list,
    open_orders_sqlite: list = None,
) -> None:

    log.critical(f" cancel_the_cancellables {currency}")

    where_filter = f"order_id"

    column_list = "label", where_filter

    if open_orders_sqlite is None:
        open_orders_sqlite: list = (
            await db_mgt.executing_query_based_on_currency_or_instrument_and_strategy(
                "orders_all_json", currency.upper(), "all", "all", column_list
            )
        )

    if open_orders_sqlite:

        for strategy in cancellable_strategies:
            open_orders_cancellables = [
                o for o in open_orders_sqlite if strategy in o["label"]
            ]

            if open_orders_cancellables:
                open_orders_cancellables_id = [
                    o["order_id"] for o in open_orders_cancellables
                ]

                for order_id in open_orders_cancellables_id:

                    await cancel_by_order_id(
                        api_request,
                        order_db_table,
                        order_id,
                    )


async def cancel_by_order_id(
    api_request: object,
    order_db_table: str,
    open_order_id: str,
) -> None:

    where_filter = f"order_id"

    await db_mgt.deleting_row(
        order_db_table,
        "databases/trading.sqlite3",
        where_filter,
        "=",
        open_order_id,
    )

    result = await api_request.get_cancel_order_byOrderId(open_order_id)

    """
    
    try:
        if (result["error"]["message"]) == "not_open_order":
            log.critical(f"CANCEL non-existing order_id {result} {open_order_id}")

    except:

        log.critical(f"CANCEL_by_order_id {result} {open_order_id}")

    """

    return result


async def if_cancel_is_true(
    api_request: str,
    order_db_table: str,
    order: dict,
) -> None:
    """ """

    if order["cancel_allowed"]:

        # get parameter orders
        await cancel_by_order_id(
            api_request,
            order_db_table,
            order["cancel_id"],
        )


async def cancelling_double_ids(
    api_request,
    client_redis,
    order_db_table: str,
    orders_currency_strategy: list,
) -> None:
    """ """

    if orders_currency_strategy:

        outstanding_order_id: list = str_mod.remove_redundant_elements(
            [o["label"] for o in orders_currency_strategy]
        )

        for label in outstanding_order_id:

            orders = [o for o in orders_currency_strategy if label in o["label"]]

            len_label = len(orders)

            if len_label > 1:

                for order in orders:
                    log.critical(f"double ids {label}")
                    log.critical(orders)

                    await cancel_by_order_id(
                        api_request,
                        order_db_table,
                        order["order_id"],
                    )

                    await error_handling.parse_error_message_with_redis(
                        client_redis,
                        f"avoiding double ids - {orders}",
                    )
