# -*- coding: utf-8 -*-

# built ins
import asyncio

# installed
from loguru import logger as log

# user defined formulas
from src.scripts.restful_api.deribit import end_point_params_template
from streaming_helper.channel_management import get_published_messages
from streaming_helper.channel_management.deribit import subscribing_to_channels
from streaming_helper.data_cleaning import managing_closed_transactions, reconciling_db
from core.db import sqlite as db_mgt, redis as redis_client
from src.scripts.deribit import starter
from src.shared.utils import (
    string_modification as str_mod,
    error_handling,
    template,
    time_modification as time_mod,
)


async def reconciling_size(
    client_id: str,
    client_secret: str,
    client_redis: object,
    redis_channels: list,
    config_app: list,
    initial_data_subaccount: dict,
    futures_instruments: list,
) -> None:

    try:

        # connecting to redis pubsub
        pubsub: object = client_redis.pubsub()

        # instantiate api for private connection
        api_request: object = end_point_params_template.SendApiRequest(
            client_id, client_secret
        )

        # get tradable strategies
        tradable_config_app = config_app["tradable"]

        # get TRADABLE currencies
        currencies: list = [o["spot"] for o in tradable_config_app][0]

        # get redis channels
        order_allowed_channel: str = redis_channels["order_is_allowed"]
        positions_update_channel: str = redis_channels["position_cache_updating"]
        ticker_cached_channel: str = redis_channels["ticker_cache_updating"]
        sub_account_cached_channel: str = redis_channels["sub_account_cache_updating"]
        my_trade_receiving_channel: str = redis_channels["my_trade_receiving"]
        portfolio_channel: str = redis_channels["portfolio"]

        # subscribe to channels
        await subscribing_to_channels.redis_channels(
            pubsub,
            redis_channels,
            "reconciling_size",
        )

        server_time = time_mod.get_now_unix_time()

        ONE_SECOND = 1000

        one_minute = ONE_SECOND * 60

        min_expiration_timestamp = futures_instruments["min_expiration_timestamp"]

        active_futures = futures_instruments["active_futures"]

        all_instruments_name = futures_instruments["instruments_name"]

        futures_instruments_name = [o for o in all_instruments_name if "-FS-" not in o]

        result_template, trade_template = (
            template.redis_message_template(),
            template.trade_template(),
        )

        initial_data_order_allowed = starter.is_order_allowed_combining(
            all_instruments_name,
            order_allowed_channel,
            result_template,
        )

        combined_order_allowed = initial_data_order_allowed["params"]["data"]

        sub_account_cached = initial_data_subaccount["params"]["data"]

        positions_cached = sub_account_cached["positions_cached"]

        await redis_client.publishing_result(
            client_redis,
            initial_data_order_allowed,
        )

        query_variables = (
            f"instrument_name, label, amount_dir as amount, trade_id, timestamp"
        )

        while True:

            try:

                message_byte = await pubsub.get_message()

                params = await get_published_messages.get_redis_message(message_byte)

                data, message_channel = params["data"], params["channel"]

                if order_allowed_channel in message_channel:

                    not_allowed_instruments = [
                        o["instrument_name"]
                        for o in combined_order_allowed
                        if o["size_is_reconciled"] == 0
                    ]

                    log.info(f"not_allowed_instruments {not_allowed_instruments}")

                    if not_allowed_instruments:

                        for instrument_name in not_allowed_instruments:

                            currency_lower = str_mod.extract_currency_from_text(
                                instrument_name
                            ).lower()

                            archive_db_table = f"my_trades_all_{currency_lower}_json"

                            query_trades_basic = (
                                f"SELECT {query_variables}  FROM  {archive_db_table}"
                            )

                            query_trades_active_where = f"WHERE instrument_name LIKE '%{instrument_name}%' AND is_open = 1 ORDER BY timestamp DESC"

                            query_trades_active_currency = (
                                f"{query_trades_basic} {query_trades_active_where}"
                            )

                            my_trades_instrument_name = (
                                await db_mgt.executing_query_with_return(
                                    query_trades_active_currency
                                )
                            )

                            my_trades_and_sub_account_size_reconciled = reconciling_db.is_my_trades_and_sub_account_size_reconciled_each_other(
                                instrument_name,
                                my_trades_instrument_name,
                                positions_cached,
                            )

                            log.critical(
                                f"{instrument_name} {my_trades_and_sub_account_size_reconciled}"
                            )

                            if not my_trades_and_sub_account_size_reconciled:

                                query_trades_where = f"WHERE instrument_name LIKE '%{instrument_name}%' ORDER BY timestamp DESC LIMIT 2"

                                query_trades_instrument_name = (
                                    f"{query_trades_basic} {query_trades_where}"
                                )

                                my_trades_instrument_name = (
                                    await db_mgt.executing_query_with_return(
                                        query_trades_instrument_name
                                    )
                                )

                                log.info(
                                    f" my_trades_instrument_name {my_trades_instrument_name}"
                                )

                                if my_trades_instrument_name:

                                    min_timestamp = min(
                                        [
                                            o["timestamp"]
                                            for o in my_trades_instrument_name
                                        ]
                                    )

                                else:

                                    one_day_ago = server_time - (
                                        one_minute * 60 * 24 * 100
                                    )

                                    min_timestamp = one_day_ago

                                transaction_log = await api_request.get_transaction_log(
                                    currency_lower,
                                    min_timestamp,
                                    1000,
                                    "trade",
                                )

                                if transaction_log:

                                    await distributing_transaction_log_from_exchange(
                                        api_request,
                                        archive_db_table,
                                        instrument_name,
                                        transaction_log,
                                        trade_template,
                                    )

                if ticker_cached_channel in message_channel:

                    exchange_server_time = data["server_time"]

                    delta_time = (exchange_server_time - server_time) / ONE_SECOND

                    if delta_time > 5:

                        await rechecking_reconciliation_regularly(
                            client_redis,
                            combined_order_allowed,
                            futures_instruments_name,
                            currencies,
                            order_allowed_channel,
                            positions_cached,
                            result_template,
                        )

                        server_time = exchange_server_time

                if (
                    positions_update_channel in message_channel
                    or sub_account_cached_channel in message_channel
                    or my_trade_receiving_channel in message_channel
                    or portfolio_channel in message_channel
                ):

                    for currency in currencies:

                        currency_lower = currency.lower()

                        archive_db_table = f"my_trades_all_{currency_lower}_json"

                        await inserting_transaction_log_data(
                            api_request,
                            archive_db_table,
                            currency,
                        )

                    if sub_account_cached_channel in message_channel:
                        positions_cached = data["positions"]

                    else:
                        try:
                            positions_cached = data["positions"]

                        except:

                            positions_cached = positions_cached

                    await rechecking_reconciliation_regularly(
                        client_redis,
                        combined_order_allowed,
                        futures_instruments_name,
                        currencies,
                        order_allowed_channel,
                        positions_cached,
                        result_template,
                    )

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


async def rechecking_reconciliation_regularly(
    client_redis: object,
    combined_order_allowed: list,
    futures_instruments_name,
    currencies: list,
    order_allowed_channel: str,
    positions_cached: list,
    result,
) -> None:
    """ """

    positions_cached_all = str_mod.remove_redundant_elements(
        [o["instrument_name"] for o in positions_cached]
    )

    # eliminating combo transactions as they're not recorded in the book
    positions_cached_instrument = [o for o in positions_cached_all if "-FS-" not in o]

    futures_instruments_name_not_in_positions_cached_instrument = [
        list(set(futures_instruments_name).difference(positions_cached_instrument))
    ][0]

    await allowing_order_for_instrument_not_in_sub_account(
        client_redis,
        combined_order_allowed,
        order_allowed_channel,
        futures_instruments_name_not_in_positions_cached_instrument,
        result,
    )

    await rechecking_based_on_sub_account(
        client_redis,
        combined_order_allowed,
        order_allowed_channel,
        positions_cached,
        positions_cached_instrument,
        result,
    )

    await rechecking_based_on_data_in_sqlite(
        client_redis,
        combined_order_allowed,
        currencies,
        order_allowed_channel,
        positions_cached,
        result,
    )


async def allowing_order_for_instrument_not_in_sub_account(
    client_redis: object,
    combined_order_allowed: list,
    order_allowed_channel: str,
    futures_instruments_name_not_in_positions_cached_instrument: list,
    result: dict,
) -> None:
    """ """

    order_allowed = 1

    for instrument_name in futures_instruments_name_not_in_positions_cached_instrument:

        [o for o in combined_order_allowed if instrument_name in o["instrument_name"]][
            0
        ]["size_is_reconciled"] = order_allowed

    result["params"].update({"channel": order_allowed_channel})
    result["params"].update({"data": combined_order_allowed})

    await redis_client.publishing_result(
        client_redis,
        result,
    )


async def rechecking_based_on_sub_account(
    client_redis: object,
    combined_order_allowed: list,
    order_allowed_channel: str,
    positions_cached: list,
    positions_cached_instrument: list,
    result: dict,
) -> None:
    """ """

    # FROM sub account to other db's
    if positions_cached_instrument:

        # sub account instruments
        for instrument_name in positions_cached_instrument:

            currency: str = str_mod.extract_currency_from_text(instrument_name)

            currency_lower = currency.lower()

            archive_db_table = f"my_trades_all_{currency_lower}_json"

            query_trades = f"SELECT * FROM  v_{currency_lower}_trading_active"

            my_trades_currency_all_transactions: list = (
                await db_mgt.executing_query_with_return(query_trades)
            )

            my_trades_instrument_name = (
                []
                if my_trades_currency_all_transactions == []
                else [
                    o
                    for o in my_trades_currency_all_transactions
                    if instrument_name in o["instrument_name"]
                ]
            )

            await managing_closed_transactions.clean_up_closed_transactions(
                archive_db_table,
                my_trades_instrument_name,
            )

            my_trades_and_sub_account_size_reconciled = (
                reconciling_db.is_my_trades_and_sub_account_size_reconciled_each_other(
                    instrument_name,
                    my_trades_instrument_name,
                    positions_cached,
                )
            )

            log.critical(
                f"{instrument_name} {my_trades_and_sub_account_size_reconciled}"
            )

            updating_order_allowed_cache(
                combined_order_allowed,
                instrument_name,
                my_trades_and_sub_account_size_reconciled,
            )

        result["params"].update({"channel": order_allowed_channel})
        result["params"].update({"data": combined_order_allowed})

        await redis_client.publishing_result(
            client_redis,
            result,
        )


async def rechecking_based_on_data_in_sqlite(
    client_redis: object,
    combined_order_allowed: list,
    currencies: list,
    order_allowed_channel: str,
    positions_cached: list,
    result: dict,
) -> None:
    """ """

    for currency in currencies:

        currency_lower = currency.lower()

        archive_db_table = f"my_trades_all_{currency_lower}_json"

        query_variables = (
            f"instrument_name, label, amount_dir as amount, timestamp, trade_id"
        )

        query_trades_active_basic = (
            f"SELECT {query_variables}  FROM  {archive_db_table}"
        )

        query_trades_active_where = (
            f"WHERE instrument_name LIKE '%{currency}%' AND is_open = 1"
        )

        query_trades_active_currency = (
            f"{query_trades_active_basic} {query_trades_active_where}"
        )

        my_trades_active_currency = await db_mgt.executing_query_with_return(
            query_trades_active_currency
        )

        my_trades_active_instrument = str_mod.remove_redundant_elements(
            [o["instrument_name"] for o in my_trades_active_currency]
        )

        if my_trades_active_instrument:

            # sub account instruments
            for instrument_name in my_trades_active_instrument:

                my_trades_active = [
                    o
                    for o in my_trades_active_currency
                    if instrument_name in o["instrument_name"]
                ]

                if my_trades_active:

                    my_trades_and_sub_account_size_reconciled = reconciling_db.is_my_trades_and_sub_account_size_reconciled_each_other(
                        instrument_name,
                        my_trades_active,
                        positions_cached,
                    )

                    log.critical(
                        f"{instrument_name} {my_trades_and_sub_account_size_reconciled}"
                    )

                    updating_order_allowed_cache(
                        combined_order_allowed,
                        instrument_name,
                        my_trades_and_sub_account_size_reconciled,
                    )

                    await managing_closed_transactions.clean_up_closed_transactions(
                        archive_db_table,
                        my_trades_active,
                    )

    result["params"].update({"channel": order_allowed_channel})
    result["params"].update({"data": combined_order_allowed})

    await redis_client.publishing_result(
        client_redis,
        result,
    )


def updating_order_allowed_cache(
    combined_order_allowed: list,
    instrument_name: str,
    my_trades_and_sub_account_size_reconciled: bool,
) -> None:
    """ """

    if my_trades_and_sub_account_size_reconciled:

        order_allowed = 1

    else:

        order_allowed = 0

    instrument_exist = [
        o for o in combined_order_allowed if instrument_name in o["instrument_name"]
    ]

    if instrument_exist:

        [o for o in combined_order_allowed if instrument_name in o["instrument_name"]][
            0
        ]["size_is_reconciled"] = order_allowed


async def inserting_transaction_log_data(
    api_request: object,
    archive_db_table: str,
    currency: str,
) -> None:
    """ """

    query_trades_active_basic = f"SELECT instrument_name, user_seq, timestamp, trade_id  FROM  {archive_db_table}"

    query_trades_active_where = f"WHERE instrument_name LIKE '%{currency}%'"

    query_trades = f"{query_trades_active_basic} {query_trades_active_where}"

    my_trades_currency = await db_mgt.executing_query_with_return(query_trades)

    if my_trades_currency:

        my_trades_currency_with_blanks_user_seq = [
            o["timestamp"] for o in my_trades_currency if o["user_seq"] is None
        ]

        if my_trades_currency_with_blanks_user_seq:

            min_timestamp = min(my_trades_currency_with_blanks_user_seq) - 100000

            transaction_log = await api_request.get_transaction_log(
                currency,
                min_timestamp,
                100,
                "trade",
            )

            where_filter = f"trade_id"

            log.debug(
                f"my_trades_currency_with_blanks_user_seq {my_trades_currency_with_blanks_user_seq}"
            )
            log.warning(f"transaction_log {transaction_log}")
            log.info([o for o in my_trades_currency if o["user_seq"] is None])

            if transaction_log:
                for transaction in transaction_log:

                    trade_id = transaction["trade_id"]
                    user_seq = int(transaction["user_seq"])
                    side = transaction["side"]
                    timestamp = int(transaction["timestamp"])
                    position = transaction["position"]

                    components = ["user_seq", "side", "timestamp", "position"]

                    for component in components:

                        if component == "user_seq":
                            sub_component = user_seq

                        elif component == "side":
                            sub_component = side

                        elif component == "timestamp":
                            sub_component = timestamp

                        elif component == "position":
                            sub_component = position

                        await db_mgt.update_status_data(
                            archive_db_table,
                            component,
                            where_filter,
                            trade_id,
                            sub_component,
                            "=",
                        )


def get_custom_label(transaction: list) -> str:

    side = transaction["direction"]
    side_label = "Short" if side == "sell" else "Long"

    try:
        last_update = transaction["timestamp"]
    except:
        try:
            last_update = transaction["last_update_timestamp"]
        except:
            last_update = transaction["creation_timestamp"]

    return f"custom{side_label.title()}-open-{last_update}"


async def distributing_transaction_log_from_exchange(
    api_request,
    archive_db_table: str,
    instrument_name: str,
    transaction_log: list,
    trade_template: dict,
) -> None:

    if transaction_log and "too_many_requests" not in transaction_log:

        for transaction in transaction_log:

            transaction_instrument_name = transaction["instrument_name"]

            transaction_currency = transaction["currency"]

            transaction_currency_usd = f"{transaction_currency.upper()}_USD"

            log.info(f"transaction {transaction}")

            if (
                instrument_name in transaction_instrument_name
                and transaction_currency_usd not in transaction_instrument_name
            ):

                if "sell" in transaction["side"]:
                    direction = "sell"

                if "buy" in transaction["side"]:
                    direction = "buy"

                timestamp = transaction["timestamp"]
                trade_id = transaction["trade_id"]

                trade_template.update({"trade_id": trade_id})
                trade_template.update({"user_seq": transaction["user_seq"]})
                trade_template.update({"side": transaction["side"]})
                trade_template.update({"timestamp": timestamp})
                trade_template.update({"position": transaction["position"]})
                trade_template.update({"amount": transaction["amount"]})
                trade_template.update({"order_id": transaction["order_id"]})
                trade_template.update({"price": transaction["price"]})
                trade_template.update({"instrument_name": transaction_instrument_name})
                trade_template.update({"direction": direction})
                trade_template.update({"currency": transaction_currency})

                # just to ensure the time stamp is below the respective timestamp above
                ARBITRARY_NUMBER = 1000000
                timestamp_sometimes_ago = timestamp - ARBITRARY_NUMBER

                trades = await api_request.get_user_trades_by_instrument_and_time(
                    instrument_name,
                    timestamp_sometimes_ago,
                    False,
                    1000,
                )

                log.error(f"trades {instrument_name} {trades}")

                if trades:

                    trade_with_the_same_trade_id = [
                        o for o in trades if o["trade_id"] == trade_id
                    ]

                    log.warning(
                        f"trade_with_the_same_trade_id {trade_with_the_same_trade_id}"
                    )

                    if trade_with_the_same_trade_id:

                        transaction = trade_with_the_same_trade_id[0]

                        try:
                            label: str = transaction["label"]

                        except:
                            label: str = template.get_custom_label(transaction)

                    else:
                        label: str = template.get_custom_label(transaction)

                else:
                    label: str = template.get_custom_label(transaction)

                if label:
                    trade_template.update({"label": label})

                    await db_mgt.insert_tables(
                        archive_db_table,
                        trade_template,
                    )
