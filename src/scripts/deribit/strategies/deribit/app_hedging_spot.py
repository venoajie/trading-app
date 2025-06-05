# -*- coding: utf-8 -*-

# built ins
import asyncio
import math

# installed
import uvloop
import orjson
from loguru import logger as log

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

from db_management.redis_client import publishing_result
from messaging.telegram_bot import telegram_bot_sendtext
from messaging import get_published_messages, subscribing_to_channels
from strategies.hedging.hedging_spot import (
    HedgingSpot,
    modify_hedging_instrument,
)
from utilities.number_modification import get_closest_value
from utilities.pickling import read_data
from utilities.string_modification import (
    message_template,
    parsing_label,
    remove_redundant_elements,
)
from utilities.system_tools import (
    parse_error_message,
    provide_path_for_file,
)


async def hedging_spot(
    client_redis: object,
    config_app: list,
    futures_instruments: list,
    initial_data: dict,
    redis_channels: list,
    strategy_attributes: list,
) -> None:
    """ """

    strategy = "hedgingSpot"

    try:

        # connecting to redis pubsub
        pubsub: object = client_redis.pubsub()

        strategy_attributes_active = [
            o for o in strategy_attributes if o["is_active"] == True
        ]

        active_strategies = [o["strategy_label"] for o in strategy_attributes_active]

        # get strategies that have not short/long attributes in the label
        non_checked_strategies = [
            o["strategy_label"]
            for o in strategy_attributes_active
            if o["non_checked_for_size_label_consistency"] == True
        ]

        contribute_to_hedging_strategies = [
            o["strategy_label"]
            for o in strategy_attributes_active
            if o["contribute_to_hedging"] == True
        ]

        instrument_attributes_futures_all = futures_instruments["active_futures"]

        strategy_params = [
            o for o in strategy_attributes if o["strategy_label"] == strategy
        ][0]

        instrument_attributes_futures_for_hedging = [
            o
            for o in instrument_attributes_futures_all
            if o["settlement_period"] != "month" and o["kind"] == "future"
        ]

        relevant_tables = config_app["relevant_tables"][0]

        trade_db_table = relevant_tables["my_trades_table"]

        # get redis channels
        market_analytics_channel: str = redis_channels["market_analytics_update"]
        portfolio_channel: str = redis_channels["portfolio"]
        my_trades_channel: str = redis_channels["my_trades_cache_updating"]
        sending_order_channel: str = redis_channels["order_rest"]
        order_allowed_channel: str = redis_channels["order_is_allowed"]
        order_update_channel: str = redis_channels["order_cache_updating"]
        ticker_cached_channel: str = redis_channels["ticker_cache_updating"]
        sub_account_cached_channel: str = redis_channels["sub_account_cache_updating"]

        # subscribe to channels
        await subscribing_to_channels.redis_channels(
            pubsub,
            redis_channels,
            "hedging_spot",
        )

        cached_ticker_all = []

        not_cancel = True

        market_condition_all = []

        query_trades = f"SELECT * FROM  v_trading_all_active"

        order_allowed = []

        allowed_instruments = []

        sub_account_cached_params = initial_data["sub_account_combined_all"]["params"]

        sub_account_cached = sub_account_cached_params["data"]

        cached_orders = sub_account_cached["orders_cached"]

        my_trades_active_all = initial_data["my_trades_active_all"]["params"]["data"]

        portfolio_all = initial_data["portfolio_all"]["params"]["data"]

        result = message_template()

        ordered = []

        status_transaction = [
            "open",  # normal transactions
            "closed",  # abnormal transactions
        ]

        ONE_PCT = 1 / 100

        INSTRUMENT_EXPIRATION_THRESHOLD = 60 * 8  # 8 hours

        ONE_SECOND = 1000

        ONE_MINUTE = ONE_SECOND * 60

        while not_cancel:

            try:

                message_byte = await pubsub.get_message()

                params = await get_published_messages.get_redis_message(message_byte)

                data, message_channel = params["data"], params["channel"]

                if order_allowed_channel in message_channel:

                    order_allowed = data

                    allowed_instruments = [
                        o for o in order_allowed if o["size_is_reconciled"] == 1
                    ]

                    log.info(f"allowed_instruments {allowed_instruments}")

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

                    log.debug(data)

                    cached_orders = data["open_orders"]

                if (
                    allowed_instruments
                    and ticker_cached_channel in message_channel
                    and market_condition_all
                    and portfolio_all
                    and strategy in active_strategies
                ):

                    # fetch details from message
                    cached_ticker_all, currency, currency_upper, server_time = (
                        data["data"],
                        data["currency"],
                        data["currency_upper"],
                        data["server_time"],
                    )

                    currency_lower: str = currency

                    size_is_reconciled_global = math.prod(
                        [
                            o["size_is_reconciled"]
                            for o in order_allowed
                            if currency_lower in o["currency"]
                        ]
                    )

                    log.debug(
                        f"{currency_upper} size_is_reconciled_global {size_is_reconciled_global}"
                    )

                    instrument_name_perpetual = f"{currency_upper}-PERPETUAL"

                    archive_db_table: str = f"my_trades_all_{currency_lower}_json"

                    # get portfolio data
                    portfolio = [
                        o for o in portfolio_all if currency_lower == o["currency"]
                    ][0]

                    equity: float = portfolio["equity"]

                    ticker_perpetual_instrument_name = [
                        o
                        for o in cached_ticker_all
                        if instrument_name_perpetual in o["instrument_name"]
                    ][0]

                    index_price = get_index(ticker_perpetual_instrument_name)

                    # sub_account_orders = sub_account["open_orders"]

                    my_trades_currency_all_transactions: list = (
                        get_transactions_under_same_currency(
                            my_trades_active_all, currency_upper
                        )
                    )

                    my_trades_currency_all: list = (
                        get_transactions_under_active_future_instruments(
                            my_trades_currency_all_transactions,
                            instrument_attributes_futures_all,
                        )
                    )

                    orders_currency: list = get_transactions_under_same_currency(
                        cached_orders,
                        currency_upper,
                    )

                    len_cleaned_orders = len(orders_currency)

                    if index_price is not None and equity > 0:

                        notional: float = compute_notional_value(index_price, equity)

                        market_condition = [
                            o
                            for o in market_condition_all
                            if instrument_name_perpetual in o["instrument_name"]
                        ][0]

                        strong_bearish = market_condition["strong_bearish"]

                        bearish = market_condition["bearish"]

                        instrument_ticker = await modify_hedging_instrument(
                            strong_bearish,
                            bearish,
                            instrument_attributes_futures_for_hedging,
                            cached_ticker_all,
                            ticker_perpetual_instrument_name,
                            currency_upper,
                        )

                        instrument_name = instrument_ticker["instrument_name"]

                        instrument_time_left = get_instrument_time_left_before_expired(
                            instrument_attributes_futures_all,
                            instrument_name,
                            server_time,
                            ONE_MINUTE,
                        )

                        instrument_time_left_exceed_threshold = (
                            instrument_time_left > INSTRUMENT_EXPIRATION_THRESHOLD
                        )

                        my_trades_currency: list = fetch_transactions_with_label_only(
                            my_trades_currency_all
                        )

                        my_trades_currency_contribute_to_hedging_sum: int = (
                            get_sum_of_transactions_contribute_to_hedging(
                                my_trades_currency,
                                contribute_to_hedging_strategies,
                            )
                        )

                        my_trades_currency_strategy = (
                            get_transactions_under_same_strategy(
                                my_trades_currency,
                                strategy,
                            )
                        )

                        orders_currency_strategy = get_transactions_under_same_strategy(
                            orders_currency,
                            strategy,
                        )

                        log.info(f" {strategy} orders_currency {orders_currency}")

                        log.critical(
                            f" {currency} orders_currency_strategy {len(orders_currency_strategy)}  {(orders_currency_strategy)} "
                        )

                        log.warning(
                            f""" ordered {ordered} orders_currency_strategy {[o["label"] for o in orders_currency_strategy]}"""
                        )

                        last_order_has_executed = is_order_has_executed(
                            ordered, orders_currency_strategy
                        )
                        log.info(f" order_has_executed {last_order_has_executed}")

                        max_position: int = notional * -1

                        hedging = HedgingSpot(
                            strategy,
                            strategy_params,
                            max_position,
                            my_trades_currency_strategy,
                            market_condition,
                            index_price,
                            my_trades_currency_all,
                        )

                        # something was wrong because perpetuals were actively traded. cancell  orders
                        if (
                            order_allowed
                            and last_order_has_executed
                            and instrument_time_left_exceed_threshold
                            and len_cleaned_orders < 50
                        ):

                            best_ask_prc: float = instrument_ticker["best_ask_price"]

                            if size_is_reconciled_global:

                                send_order: dict = (
                                    await hedging.is_send_open_order_allowed(
                                        non_checked_strategies,
                                        instrument_name,
                                        instrument_attributes_futures_for_hedging,
                                        orders_currency_strategy,
                                        best_ask_prc,
                                        archive_db_table,
                                        trade_db_table,
                                    )
                                )

                                log.warning(f"send_order {send_order}")

                                if send_order["order_allowed"]:

                                    result["params"].update(
                                        {"channel": sending_order_channel}
                                    )

                                    ordered.append(send_order["order_parameters"])

                                    result["params"].update({"data": send_order})

                                    await publishing_result(
                                        client_redis,
                                        sending_order_channel,
                                        result,
                                    )

                                # not_order = False

                            #                                        break

                            if len_cleaned_orders < 50:

                                # log.error (f"{orders_currency_strategy} ")

                                for status in status_transaction:

                                    my_trades_currency_strategy_status = [
                                        o
                                        for o in my_trades_currency_strategy
                                        if status in (o["label"])
                                    ]

                                    orders_currency_strategy_label_contra_status = [
                                        o
                                        for o in orders_currency_strategy
                                        if status not in o["label"]
                                    ]

                                    # log.error (f"{status} ")

                                    if my_trades_currency_strategy_status:

                                        transaction_instrument_name = remove_redundant_elements(
                                            [
                                                o["instrument_name"]
                                                for o in my_trades_currency_strategy_status
                                            ]
                                        )

                                        for (
                                            instrument_name
                                        ) in transaction_instrument_name:

                                            instrument_ticker: list = [
                                                o
                                                for o in cached_ticker_all
                                                if instrument_name
                                                in o["instrument_name"]
                                            ]

                                            if instrument_ticker:

                                                instrument_ticker: dict = (
                                                    instrument_ticker[0]
                                                )

                                                log.error(
                                                    f"my_trades_currency_contribute_to_hedging_sum {my_trades_currency_contribute_to_hedging_sum}"
                                                )

                                                if (
                                                    not ordered
                                                    and status == "open"
                                                    and my_trades_currency_contribute_to_hedging_sum
                                                    <= 0
                                                ):

                                                    best_bid_prc: float = (
                                                        instrument_ticker[
                                                            "best_bid_price"
                                                        ]
                                                    )

                                                    nearest_transaction_to_index = get_nearest_transaction_to_index(
                                                        my_trades_currency_strategy_status,
                                                        instrument_name,
                                                        best_bid_prc,
                                                    )

                                                    send_closing_order: dict = (
                                                        await hedging.is_send_exit_order_allowed(
                                                            orders_currency_strategy_label_contra_status,
                                                            best_bid_prc,
                                                            nearest_transaction_to_index,
                                                            # orders_currency_strategy
                                                        )
                                                    )

                                                    if send_closing_order[
                                                        "order_allowed"
                                                    ]:

                                                        result["params"].update(
                                                            {
                                                                "channel": sending_order_channel
                                                            }
                                                        )
                                                        result["params"].update(
                                                            {"data": send_closing_order}
                                                        )

                                                        ordered.append(
                                                            send_closing_order[
                                                                "order_parameters"
                                                            ]
                                                        )

                                                        await publishing_result(
                                                            client_redis,
                                                            sending_order_channel,
                                                            result,
                                                        )

                                                        # not_order = False

                                                        # break

                                                if not ordered and status == "closed":

                                                    best_ask_prc: float = (
                                                        instrument_ticker[
                                                            "best_ask_price"
                                                        ]
                                                    )

                                                    nearest_transaction_to_index = get_nearest_transaction_to_index(
                                                        my_trades_currency_strategy_status,
                                                        instrument_name,
                                                        best_ask_prc,
                                                    )

                                                    send_closing_order: dict = (
                                                        await hedging.send_contra_order_for_orphaned_closed_transctions(
                                                            orders_currency_strategy_label_contra_status,
                                                            best_ask_prc,
                                                            nearest_transaction_to_index,
                                                            # orders_currency_strategy
                                                        )
                                                    )

                                                    if send_order["order_allowed"]:

                                                        result["params"].update(
                                                            {
                                                                "channel": sending_order_channel
                                                            }
                                                        )
                                                        result["params"].update(
                                                            {"data": send_order}
                                                        )

                                                        ordered.append(
                                                            send_closing_order[
                                                                "order_parameters"
                                                            ]
                                                        )

                                                        await publishing_result(
                                                            client_redis,
                                                            sending_order_channel,
                                                            result,
                                                        )

                                                        # not_order = False
                                                        # )

                                                        # break

            except Exception as error:

                parse_error_message(error)

                AAAAA

                continue

            finally:
                await asyncio.sleep(0.001)

    except Exception as error:

        parse_error_message(f"app hedging spot {error}")

        await telegram_bot_sendtext(f"app hedging spot-{error}", "general_error")


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


def get_transactions_under_same_currency(
    transactions: list,
    currency_upper: str,
) -> list:

    return (
        []
        if not transactions
        else [o for o in transactions if currency_upper in o["instrument_name"]]
    )


def get_transactions_under_same_strategy(
    transactions: list,
    strategy: str,
) -> list:

    return (
        [] if not transactions else [o for o in transactions if strategy in o["label"]]
    )


def get_transactions_under_active_future_instruments(
    my_trades_currency_all_transactions: list,
    instrument_attributes_futures_all: list,
) -> list:

    return (
        []
        if my_trades_currency_all_transactions == 0
        else [
            o
            for o in my_trades_currency_all_transactions
            if o["instrument_name"]
            in [o["instrument_name"] for o in instrument_attributes_futures_all]
        ]
    )


def get_transactions_contribute_to_hedging(
    my_trades_currency: list,
    contribute_to_hedging_strategies: list,
) -> list:

    return [
        o
        for o in my_trades_currency
        if (parsing_label(o["label"])["main"]) in contribute_to_hedging_strategies
    ]


def get_sum_of_transactions_contribute_to_hedging(
    my_trades_currency: list,
    contribute_to_hedging_strategies: list,
) -> int:

    my_trades_currency_contribute_to_hedging = get_transactions_contribute_to_hedging(
        my_trades_currency,
        contribute_to_hedging_strategies,
    )

    return (
        0
        if not my_trades_currency_contribute_to_hedging
        else sum([o["amount"] for o in my_trades_currency_contribute_to_hedging])
    )


def get_instrument_time_left_before_expired(
    instrument_attributes_futures_all: list,
    instrument_name: str,
    server_time: int,
    basis_time: float,
) -> list:

    return (
        max(
            [
                o["expiration_timestamp"]
                for o in instrument_attributes_futures_all
                if o["instrument_name"] == instrument_name
            ]
        )
        - server_time
    ) / basis_time


def get_data_and_channel_from_message(message_byte: dict) -> dict:

    if message_byte and message_byte["type"] == "message":

        message_byte_data = orjson.loads(message_byte["data"])

        params = message_byte_data["params"]

        return dict(
            data=params["data"],
            channel=params["channel"],
        )

    else:

        return dict(
            data=[],
            channel=[],
        )


def fetch_transactions_with_label_only(transactions: list) -> list:

    return [o for o in transactions if o["label"] is not None]


def is_order_has_executed(
    ordered: list,
    orders_currency_strategy: list,
) -> bool:

    order_has_executed = [
        o
        for o in ordered
        if o["label"] in [o["label"] for o in orders_currency_strategy]
    ]

    if order_has_executed:
        ordered = []

    return True if order_has_executed else False


def get_nearest_transaction_to_index(
    transactions: list,
    instrument_name: str,
    control_price: float,
) -> list:

    get_prices_in_label_transaction_main = [
        o["price"] for o in transactions if instrument_name in o["instrument_name"]
    ]

    closest_price = get_closest_value(
        get_prices_in_label_transaction_main,
        control_price,
    )

    return [o for o in transactions if o["price"] == closest_price]
