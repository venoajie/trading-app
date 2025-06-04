# # -*- coding: utf-8 -*-

# built ins
import asyncio

# installed
from loguru import logger as log

# user defined formula
from streaming_helper.db_management import sqlite_management as db_mgt
from streaming_helper.strategies.deribit.cash_carry import combo_auto
from streaming_helper.utilities import string_modification as str_mod


def waiting_time_has_expired(
    strategy_params: dict,
    future_trade: dict,
    perpetual_trade: dict,
    server_time: int,
) -> bool:
    """ """

    ONE_SECOND = 1000

    ONE_MINUTE = ONE_SECOND * 60

    waiting_minute_before_cancel = (
        strategy_params["waiting_minute_before_relabelling"] * ONE_MINUTE
    )

    # log.debug (f"waiting_minute_before_cancel {waiting_minute_before_cancel}")

    timestamp_perpetual: int = perpetual_trade["timestamp"]

    waiting_time_for_perpetual_order: bool = (
        combo_auto.check_if_minimum_waiting_time_has_passed(
            waiting_minute_before_cancel,
            timestamp_perpetual,
            server_time,
        )
    )

    timestamp_future: int = future_trade["timestamp"]

    waiting_time_for_future_order: bool = (
        combo_auto.check_if_minimum_waiting_time_has_passed(
            waiting_minute_before_cancel,
            timestamp_future,
            server_time,
        )
    )

    return waiting_time_for_perpetual_order and waiting_time_for_future_order


def my_trades_currency_strategy_with_no_blanks(
    my_trades_currency: list,
    strategy: str,
) -> list:
    """ """

    my_trades_currency_active_with_no_blanks = (
        []
        if my_trades_currency == []
        else [o for o in my_trades_currency if o["label"] is not None]
    )

    my_trades_currency_strategy = [
        o
        for o in my_trades_currency_active_with_no_blanks
        if strategy in o["label"]
        and "closed" not in o["label"]
        and "Auto" not in o["label"]
    ]
    return my_trades_currency_strategy


def get_redundant_ids(
    my_trades_currency: list,
    strategy: str,
) -> list:
    """ """

    my_trades_currency_strategy = my_trades_currency_strategy_with_no_blanks(
        my_trades_currency, strategy
    )

    if my_trades_currency_strategy:

        instrument_names = str_mod.remove_redundant_elements(
            [o["instrument_name"] for o in my_trades_currency_strategy]
        )

        if instrument_names:

            for instrument_name in instrument_names:

                my_trade_instrument_name = [
                    o
                    for o in my_trades_currency_strategy
                    if instrument_name in o["instrument_name"]
                ]

                if my_trade_instrument_name:

                    my_trades_label = str_mod.remove_redundant_elements(
                        [(o["label"]) for o in my_trade_instrument_name]
                    )

                    result = []
                    for label in my_trades_label:
                        len_label = len(
                            [
                                o["label"]
                                for o in my_trades_currency_strategy
                                if label in o["label"]
                            ]
                        )
                        if len_label > 1:
                            result.append(label)

                    return result


def get_single_transaction(
    my_trades_currency: list,
    strategy: str,
) -> list:
    """ """

    my_trades_currency_strategy = my_trades_currency_strategy_with_no_blanks(
        my_trades_currency, strategy
    )

    if my_trades_currency_strategy:

        my_trades_label = str_mod.remove_redundant_elements(
            [(o["label"]) for o in my_trades_currency_strategy]
        )

        result = []
        for label in my_trades_label:

            label_integer = str_mod.parsing_label(label)["int"]

            transaction_under_label_integer = [
                o for o in my_trades_currency_strategy if label_integer in o["label"]
            ]

            # additional filter
            sum_transaction_under_label_integer = sum(
                [o["amount"] for o in transaction_under_label_integer]
            )

            transaction_under_label_integer_len = len(transaction_under_label_integer)
            # log.info (f" {transaction_under_label_integer} sum_transaction_under_label_integer {sum_transaction_under_label_integer} transaction_under_label_integer_len {transaction_under_label_integer_len}")
            if transaction_under_label_integer_len == 1:

                result.append(transaction_under_label_integer[0])

        return result


async def updating_db_with_new_label(
    trade_db_table: str,
    archive_db_table: str,
    trade_id: str,
    filter: str,
    new_label: str,
) -> None:
    """ """

    await db_mgt.update_status_data(
        archive_db_table, "label", filter, trade_id, new_label, "="
    )

    await db_mgt.update_status_data(
        trade_db_table, "label", filter, trade_id, new_label, "="
    )


async def relabelling_double_ids(
    trade_db_table: str,
    archive_db_table: str,
    my_trades_currency_active: list,
) -> None:
    """ """

    from strategies.basic_strategy import get_label

    strategy = "futureSpread"

    relabelling = False

    my_trades_currency_strategy = my_trades_currency_strategy_with_no_blanks(
        my_trades_currency_active, strategy
    )

    if my_trades_currency_strategy:

        instrument_names = str_mod.remove_redundant_elements(
            [o["instrument_name"] for o in my_trades_currency_strategy]
        )

        if instrument_names:

            for instrument_name in instrument_names:

                my_trade_instrument_name = [
                    o
                    for o in my_trades_currency_strategy
                    if instrument_name in o["instrument_name"]
                ]

                redundant_ids = get_redundant_ids(
                    my_trade_instrument_name,
                    strategy,
                )

                if redundant_ids:

                    log.error(f"redundant_ids {redundant_ids}")

                    for label in redundant_ids:
                        log.error(f"label {label}")

                        trade_ids = [
                            o["trade_id"]
                            for o in my_trade_instrument_name
                            if label in o["label"]
                        ]

                        if trade_ids:

                            for trade_id in trade_ids:

                                log.warning(f"trade_id {trade_id}")

                                filter = "trade_id"

                                new_label: str = get_label("open", strategy)

                                await updating_db_with_new_label(
                                    trade_db_table,
                                    archive_db_table,
                                    trade_id,
                                    filter,
                                    new_label,
                                )

                                relabelling = True

                                break
    return relabelling


async def pairing_single_label(
    strategy_attributes: list,
    archive_db_table: str,
    my_trades_currency_active: dict,
    server_time: int,
) -> None:
    """ """

    paired_success = False

    strategy = "futureSpread"

    single_label_transaction = get_single_transaction(
        my_trades_currency_active, strategy
    )

    # log.warning (f"single_label_transaction {single_label_transaction}")

    if single_label_transaction:

        my_trades_amount = str_mod.remove_redundant_elements(
            [abs(o["amount"]) for o in single_label_transaction]
        )

        strategy_params = strategy_params = [
            o for o in strategy_attributes if o["strategy_label"] == strategy
        ][0]

        # log.error(f"my_trades_amount {my_trades_amount}")

        for amount in my_trades_amount:

            # log.error(f"amount {amount}")

            my_trades_with_the_same_amount = [
                o for o in single_label_transaction if amount == abs(o["amount"])
            ]

            my_trades_with_the_same_amount_label_perpetual = [
                o
                for o in my_trades_with_the_same_amount
                if "PERPETUAL" in o["instrument_name"]
            ]

            my_trades_with_the_same_amount_label_non_perpetual = [
                o
                for o in my_trades_with_the_same_amount
                if "PERPETUAL" not in o["instrument_name"]
            ]

            my_trades_with_the_same_amount_label_non_perpetual_instrument_name = (
                str_mod.remove_redundant_elements(
                    [
                        o["instrument_name"]
                        for o in my_trades_with_the_same_amount_label_non_perpetual
                    ]
                )
            )

            # log.error(
            #    f"my_trades_with_the_same_amount_label_non_perpetual_instrument_name {my_trades_with_the_same_amount_label_non_perpetual_instrument_name}"
            # )

            for (
                instrument_name_future
            ) in my_trades_with_the_same_amount_label_non_perpetual_instrument_name:

                my_trades_with_the_same_amount_label_future = [
                    o
                    for o in my_trades_with_the_same_amount_label_non_perpetual
                    if instrument_name_future in o["instrument_name"]
                ]

                my_trades_future_sorted = str_mod.sorting_list(
                    my_trades_with_the_same_amount_label_future, "price", True
                )

                if my_trades_future_sorted:

                    future_trade = my_trades_future_sorted[0]
                    price_future = future_trade["price"]

                    my_trades_perpetual_with_lower_price = [
                        o
                        for o in my_trades_with_the_same_amount_label_perpetual
                        if o["price"] < price_future and o["amount"] > 0
                    ]

                    my_trades_perpetual_with_lower_price_sorted = str_mod.sorting_list(
                        my_trades_perpetual_with_lower_price, "price", False
                    )

                    # log.error(f"my_trades_future_sorted {my_trades_future_sorted}")

                    if my_trades_perpetual_with_lower_price_sorted:

                        # log.debug (f"my_trades_perpetual_with_lower_price_sorted {my_trades_perpetual_with_lower_price_sorted}")

                        perpetual_trade = my_trades_perpetual_with_lower_price_sorted[0]

                        log.warning(future_trade)
                        log.debug(perpetual_trade)

                        waiting_time_expired = waiting_time_has_expired(
                            strategy_params,
                            future_trade,
                            perpetual_trade,
                            server_time,
                        )

                        if waiting_time_expired:

                            side_perpetual = perpetual_trade["side"]
                            side_future = future_trade["side"]

                            filter = "trade_id"
                            trade_id = perpetual_trade[filter]
                            new_label = future_trade["label"]

                            # market contango
                            if my_trades_perpetual_with_lower_price_sorted:

                                # market contango
                                if side_future == "sell" and side_perpetual == "buy":

                                    await db_mgt.update_status_data(
                                        archive_db_table,
                                        "label",
                                        filter,
                                        trade_id,
                                        new_label,
                                        "=",
                                    )

                                    log.warning(future_trade)
                                    log.debug(perpetual_trade)
                                    log.debug(new_label)

                                    paired_success = True

                                    break

    return paired_success
