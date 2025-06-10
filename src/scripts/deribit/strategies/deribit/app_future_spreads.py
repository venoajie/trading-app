# -*- coding: utf-8 -*-

# built ins
import asyncio
import math
from random import sample

# installed
from loguru import logger as log
import orjson

# user defined formulas
from streaming_helper.db_management import redis_client, sqlite_management as db_mgt
from streaming_helper.restful_api.deribit import end_point_params_template
from streaming_helper.channel_management.deribit import subscribing_to_channels
from streaming_helper.channel_management import get_published_messages

from strategies import basic_strategy
from strategies.cash_carry import combo_auto as combo

from streaming_helper.utilities import (
    string_modification as str_mod,
    error_handling,
    system_tools,
    time_modification as time_mod,
    template,
)


async def future_spreads(
    client_id,
    client_secret,
    client_redis: object,
    config_app: list,
    futures_instruments: list,
    initial_data: dict,
    redis_channels: list,
    strategy_attributes: list,
) -> None:
    """ """

    strategy = "futureSpread"

    try:

        # subscribe to channels
        await subscribing_to_channels.redis_channels(
            pubsub,
            redis_channels,
            "future_spread",
        )

        # connecting to redis pubsub
        pubsub: object = client_redis.pubsub()

        # instantiate api for private connection
        api_request: object = end_point_params_template.SendApiRequest(
            client_id, client_secret
        )

        strategy_attributes_active = [
            o for o in strategy_attributes if o["is_active"] == True
        ]

        active_strategies = [o["strategy_label"] for o in strategy_attributes_active]

        instrument_attributes_futures_all = futures_instruments["active_futures"]

        relevant_tables = config_app["relevant_tables"][0]

        trade_db_table = relevant_tables["my_trades_table"]

        instruments_name = futures_instruments["instruments_name"]
        instrument_attributes_combo_all = futures_instruments["active_combo"]

        # get redis channels
        order_receiving_channel: str = redis_channels["order_receiving"]
        market_analytics_channel: str = redis_channels["market_analytics_update"]
        portfolio_channel: str = redis_channels["portfolio"]
        my_trades_channel: str = redis_channels["my_trades_cache_updating"]
        sending_order_channel: str = redis_channels["order_rest"]
        order_allowed_channel: str = redis_channels["order_is_allowed"]
        positions_update_channel: str = redis_channels["position_cache_updating"]
        ticker_cached_channel: str = redis_channels["ticker_cache_updating"]

        cached_orders = initial_data["sub_account_combined"]

        my_trades_active_all = initial_data["my_trades_active_all"]

        cached_ticker_all = []

        not_cancel = True

        market_condition_all = []

        portfolio_all = []

        query_trades = f"SELECT * FROM  v_trading_all_active"

        order_allowed = []

        allowed_instruments = []

        result = str_mod.message_template()

        while not_cancel:

            try:

                message_byte = await pubsub.get_message(pubsub)

                params = await get_published_messages.get_redis_message(message_byte)

                data, message_channel = params["data"], params["channel"]

                if order_allowed_channel in message_channel:

                    allowed_instruments = [
                        o for o in order_allowed if o["size_is_reconciled"] == 1
                    ]

                    if market_analytics_channel in message_channel:

                        market_condition_all = data

                    if portfolio_channel in message_channel:

                        portfolio_all = data["cached_portfolio"]

                    if my_trades_channel in message_channel:

                        my_trades_active_all = await db_mgt.executing_query_with_return(
                            query_trades
                        )

                    if order_receiving_channel in message_channel:

                        cached_orders = data["cached_orders"]

                    if (
                        allowed_instruments
                        and ticker_cached_channel in message_channel
                        and market_condition_all
                        and portfolio_all
                        and strategy in active_strategies
                    ):

                        cached_ticker_all = data["data"]

                        server_time = data["server_time"]

                        currency, currency_upper = (
                            data["currency"],
                            data["currency_upper"],
                        )

                        currency_lower: str = currency

                        order_allowed_global = math.prod(
                            [
                                o["size_is_reconciled"]
                                for o in order_allowed
                                if currency_lower in o["currency"]
                            ]
                        )

                        log.debug(f"order_allowed_global {order_allowed_global}")

                        instrument_name_perpetual = f"{currency_upper}-PERPETUAL"

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

                        market_condition = [
                            o
                            for o in market_condition_all
                            if instrument_name_perpetual in o["instrument_name"]
                        ][0]

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

                        len_cleaned_orders = len(orders_currency)

                        if index_price is not None and equity > 0:

                            my_trades_currency: list = [
                                o
                                for o in my_trades_currency_all
                                if o["label"] is not None
                            ]

                            ONE_PCT = 1 / 100

                            THRESHOLD_DELTA_TIME_SECONDS = 120

                            THRESHOLD_MARKET_CONDITIONS_COMBO = 0.1 * ONE_PCT

                            INSTRUMENT_EXPIRATION_THRESHOLD = 60 * 8  # 8 hours

                            ONE_SECOND = 1000

                            ONE_MINUTE = ONE_SECOND * 60

                            notional: float = compute_notional_value(
                                index_price, equity
                            )

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

                            # log.info(
                            #    f"orders_currency_strategy {len (orders_currency_strategy)}"
                            # )

                            if order_allowed:

                                async with client_redis.pipeline() as pipe:

                                    # waiting minute before reorder  15 min
                                    extra = 3

                                    BASIC_TICKS_FOR_AVERAGE_MOVEMENT: int = (
                                        strategy_params[
                                            "waiting_minute_before_relabelling"
                                        ]
                                        + extra
                                    )

                                    AVERAGE_MOVEMENT: float = 0.15 / 100

                                    monthly_target_profit = strategy_params[
                                        "monthly_profit_pct"
                                    ]

                                    max_order_currency = 2

                                    random_instruments_name = sample(
                                        (
                                            [
                                                o
                                                for o in instruments_name
                                                if "-FS-" not in o
                                                and currency_upper in o
                                            ]
                                        ),
                                        max_order_currency,
                                    )

                                    combo_auto = combo.ComboAuto(
                                        strategy,
                                        strategy_params,
                                        orders_currency_strategy,
                                        server_time,
                                        market_condition,
                                        my_trades_currency_strategy,
                                        ticker_perpetual_instrument_name,
                                    )

                                    my_trades_currency_strategy_labels: list = [
                                        o["label"] for o in my_trades_currency_strategy
                                    ]

                                    # send combo orders
                                    future_control = []

                                    for (
                                        instrument_attributes_combo
                                    ) in instrument_attributes_combo_all:

                                        try:
                                            instrument_name_combo = (
                                                instrument_attributes_combo[
                                                    "instrument_name"
                                                ]
                                            )

                                        except:
                                            instrument_name_combo = None

                                        if (
                                            instrument_name_combo
                                            and currency_upper in instrument_name_combo
                                        ):

                                            instrument_name_future = (
                                                f"{currency_upper}-{instrument_name_combo[7:][:7]}"
                                            ).strip("_")

                                            expiration_timestamp = [
                                                o["expiration_timestamp"]
                                                for o in instrument_attributes_futures_all
                                                if instrument_name_future
                                                in o["instrument_name"]
                                            ][0]

                                            instrument_time_left = (
                                                expiration_timestamp - server_time
                                            ) / ONE_MINUTE

                                            instrument_time_left_exceed_threshold = (
                                                instrument_time_left
                                                > INSTRUMENT_EXPIRATION_THRESHOLD
                                            )

                                            ticker_combo = [
                                                o
                                                for o in cached_ticker_all
                                                if instrument_name_combo
                                                in o["instrument_name"]
                                            ]

                                            ticker_future = [
                                                o
                                                for o in cached_ticker_all
                                                if instrument_name_future
                                                in o["instrument_name"]
                                            ]

                                            # log.debug(
                                            #    f"future_control {future_control} instrument_name_combo {instrument_name_combo} instrument_name_future {instrument_name_future}"
                                            # )

                                            instrument_name_future_in_control = (
                                                False
                                                if future_control == []
                                                else [
                                                    o
                                                    for o in future_control
                                                    if instrument_name_future in o
                                                ]
                                            )

                                            # log.debug(
                                            #    f"instrument_name_future_not_in_control {instrument_name_future_in_control} {not instrument_name_future_in_control}"
                                            # )

                                            if (
                                                not instrument_name_future_in_control
                                                and len_cleaned_orders < 50
                                                and ticker_future
                                                and ticker_combo
                                            ):
                                                # and not reduce_only \

                                                ticker_combo, ticker_future = (
                                                    ticker_combo[0],
                                                    ticker_future[0],
                                                )

                                                if (
                                                    instrument_time_left_exceed_threshold
                                                    and instrument_name_future
                                                    in random_instruments_name
                                                ):

                                                    send_order: dict = (
                                                        await combo_auto.is_send_open_order_constructing_manual_combo_allowed(
                                                            ticker_future,
                                                            instrument_attributes_futures_all,
                                                            notional,
                                                            monthly_target_profit,
                                                            AVERAGE_MOVEMENT,
                                                            BASIC_TICKS_FOR_AVERAGE_MOVEMENT,
                                                            min(
                                                                1,
                                                                max_order_currency,
                                                            ),
                                                            market_condition,
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

                                                        await redis_client.publishing_result(
                                                            pipe,
                                                            sending_order_channel,
                                                            result,
                                                        )

                                                        # not_order = False

                                                        break

                                            future_control.append(
                                                instrument_name_future
                                            )
                                    # get labels from active trades
                                    labels = str_mod.remove_redundant_elements(
                                        my_trades_currency_strategy_labels
                                    )

                                    filter = "label"

                                    #! closing active trades
                                    for label in labels:

                                        label_integer: int = (
                                            basic_strategy.get_label_integer(label)
                                        )
                                        selected_transaction = [
                                            o
                                            for o in my_trades_currency_strategy
                                            if str(label_integer) in o["label"]
                                        ]

                                        selected_transaction_amount = [
                                            o["amount"] for o in selected_transaction
                                        ]
                                        sum_selected_transaction = sum(
                                            selected_transaction_amount
                                        )
                                        len_selected_transaction = len(
                                            selected_transaction_amount
                                        )

                                        #! closing combo auto trading
                                        if "Auto" in label and len_cleaned_orders < 50:

                                            if sum_selected_transaction == 0:

                                                abnormal_transaction = [
                                                    o
                                                    for o in selected_transaction
                                                    if "closed" in o["label"]
                                                ]

                                                if not abnormal_transaction:
                                                    send_order: dict = (
                                                        await combo_auto.is_send_exit_order_allowed_combo_auto(
                                                            label,
                                                            instrument_attributes_combo_all,
                                                            THRESHOLD_MARKET_CONDITIONS_COMBO,
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

                                                        await redis_client.publishing_result(
                                                            pipe,
                                                            sending_order_channel,
                                                            result,
                                                        )

                                                        # not_order = False

                                                        break

                                                else:
                                                    log.critical(
                                                        f"abnormal_transaction {abnormal_transaction}"
                                                    )

                                                    break

                                        else:

                                            #! closing unpaired transactions
                                            log.critical(
                                                f"sum_selected_transaction {sum_selected_transaction}"
                                            )
                                            if sum_selected_transaction != 0:

                                                if (
                                                    len_selected_transaction == 1
                                                    and "closed" not in label
                                                ):

                                                    send_order = []

                                                    for (
                                                        transaction
                                                    ) in selected_transaction:

                                                        waiting_minute_before_ordering = (
                                                            strategy_params[
                                                                "waiting_minute_before_cancel"
                                                            ]
                                                            * ONE_MINUTE
                                                        )

                                                        timestamp: int = transaction[
                                                            "timestamp"
                                                        ]

                                                        waiting_time_for_selected_transaction: (
                                                            bool
                                                        ) = (
                                                            combo.check_if_minimum_waiting_time_has_passed(
                                                                waiting_minute_before_ordering,
                                                                timestamp,
                                                                server_time,
                                                            )
                                                            * 2
                                                        )

                                                        instrument_name = transaction[
                                                            "instrument_name"
                                                        ]

                                                        ticker_transaction = [
                                                            o
                                                            for o in cached_ticker_all
                                                            if instrument_name
                                                            in o["instrument_name"]
                                                        ]

                                                        if (
                                                            ticker_transaction
                                                            and len_cleaned_orders < 50
                                                        ):

                                                            TP_THRESHOLD = (
                                                                THRESHOLD_MARKET_CONDITIONS_COMBO
                                                                * 5
                                                            )

                                                            send_order: dict = (
                                                                await combo_auto.is_send_contra_order_for_unpaired_transaction_allowed(
                                                                    ticker_transaction[
                                                                        0
                                                                    ],
                                                                    instrument_attributes_futures_all,
                                                                    TP_THRESHOLD,
                                                                    transaction,
                                                                    waiting_time_for_selected_transaction,
                                                                    random_instruments_name,
                                                                )
                                                            )

                                                            if send_order[
                                                                "order_allowed"
                                                            ]:

                                                                result["params"].update(
                                                                    {
                                                                        "channel": sending_order_channel
                                                                    }
                                                                )
                                                                result["params"].update(
                                                                    {"data": send_order}
                                                                )

                                                                await redis_client.publishing_result(
                                                                    pipe,
                                                                    sending_order_channel,
                                                                    result,
                                                                )

                                                                # not_order = False

                                                                break

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


def reading_from_pkl_data(
    end_point,
    currency,
    status: str = None,
) -> dict:
    """ """

    path: str = system_tools.provide_path_for_file(end_point, currency, status)

    return pickling.read_data(path)


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
