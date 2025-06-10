# src\services\cleaner\managing_closed_transactions.py

"""_summary_"""
# built ins
import asyncio

# installed
from loguru import logger as log

# user defined formula
from core.db import sqlite as db_mgt
from core.db.sqlite import (
    executing_query_based_on_currency_or_instrument_and_strategy as get_query,
)
from src.scripts.deribit.strategies.basic_strategy import get_label_integer
from src.shared.utils import (
    string_modification as str_mod,
    error_handling,
)


def get_label_main(
    result: list,
    strategy_label: str,
) -> list:
    """ """

    return [
        o
        for o in result
        if str_mod.parsing_label(strategy_label)["main"]
        == str_mod.parsing_label(o["label"])["main"]
    ]


async def get_unrecorded_trade_id(instrument_name: str) -> dict:

    currency = str_mod.extract_currency_from_text(instrument_name)

    column_list: str = "data", "trade_id"

    from_sqlite_open = await get_query(
        "my_trades_all_json", instrument_name, "all", "all", column_list
    )

    from_sqlite_closed = await get_query(
        "my_trades_closed_json", instrument_name, "all", "all", column_list
    )

    # column_list: str="order_id", "trade_id","label","amount","id","data",
    from_sqlite_all = await get_query(
        f"my_trades_all_{currency.lower()}_json",
        instrument_name,
        "all",
        "all",
        column_list,
        # 40,
        # "id"
    )

    from_sqlite_closed_trade_id = [o["trade_id"] for o in from_sqlite_closed]

    from_sqlite_open_trade_id = [o["trade_id"] for o in from_sqlite_open]

    from_exchange_trade_id = [o["trade_id"] for o in from_sqlite_all]

    combined_trade_closed_open = from_sqlite_open_trade_id + from_sqlite_closed_trade_id

    unrecorded_trade_id = str_mod.get_unique_elements(
        from_exchange_trade_id, combined_trade_closed_open
    )

    log.debug(f"unrecorded_trade_id {unrecorded_trade_id}")

    return (
        []
        if not from_sqlite_all
        else [
            o["data"] for o in from_sqlite_all if o["trade_id"] in unrecorded_trade_id
        ]
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


def get_unrecorded_trade_transactions(
    direction: str,
    my_trades_instrument_name: list,
    from_transaction_log_instrument: list,
) -> dict:
    """_summary_
    direction: "from_trans_log_to_my_trade" -> transaction log more update
    direction: "from_my_trade_to_trans_log" -> my_trade more update

    Returns:
        _type_: _description_
    """

    if direction == "from_trans_log_to_my_trade":

        from_transaction_log_instrument_trade_id = sorted(
            [o["trade_id"] for o in from_transaction_log_instrument]
        )

        if my_trades_instrument_name:
            my_trades_instrument_name_trade_id = sorted(
                [o["trade_id"] for o in my_trades_instrument_name]
            )

            if my_trades_instrument_name_trade_id:
                unrecorded_trade_id = str_mod.get_unique_elements(
                    from_transaction_log_instrument_trade_id,
                    my_trades_instrument_name_trade_id,
                )
        else:
            unrecorded_trade_id = from_transaction_log_instrument_trade_id

        log.debug(f"unrecorded_trade_from_transaction_log {unrecorded_trade_id}")

        return unrecorded_trade_id

    if direction == "from_my_trade_to_trans_log":

        from_transaction_log_instrument_trade_id = sorted(
            [o["trade_id"] for o in from_transaction_log_instrument]
        )

        if my_trades_instrument_name:
            my_trades_instrument_name_trade_id = [
                o["trade_id"] for o in my_trades_instrument_name
            ]

            if my_trades_instrument_name_trade_id:
                unrecorded_trade_id = str_mod.get_unique_elements(
                    my_trades_instrument_name_trade_id,
                    from_transaction_log_instrument_trade_id,
                )

        else:
            unrecorded_trade_id = []

        log.debug(f"unrecorded_trade_from_transaction_log {unrecorded_trade_id}")

        return unrecorded_trade_id

    if direction == "delivered":

        delivered_from_transaction_log_instrument = [
            o for o in from_transaction_log_instrument if o["type"] == "delivery"
        ]

        from_transaction_log_instrument_trade_id = sorted(
            str_mod.remove_redundant_elements(
                [o["timestamp"] for o in delivered_from_transaction_log_instrument]
            )
        )

        if my_trades_instrument_name:
            my_trades_instrument_name_trade_id = sorted(
                [o["timestamp"] for o in my_trades_instrument_name]
            )

            if my_trades_instrument_name_trade_id:
                unrecorded_time_stamp = str_mod.get_unique_elements(
                    from_transaction_log_instrument_trade_id,
                    my_trades_instrument_name_trade_id,
                )
        else:
            unrecorded_time_stamp = from_transaction_log_instrument_trade_id

        log.debug(
            f"unrecorded_time_stamp {unrecorded_time_stamp} {delivered_from_transaction_log_instrument}"
        )

        return [] if not unrecorded_time_stamp else max(unrecorded_time_stamp)


def get_transactions_with_closed_label(transactions_all: list) -> list:
    """ """
    transactions_with_labels = [o for o in transactions_all if o["label"] is not None]

    # log.error(f"transactions_with_labels {transactions_with_labels}")

    return (
        []
        if not transactions_with_labels
        else [o for o in transactions_with_labels if "closed" in o["label"]]
    )


def transactions_under_label_int(label_integer: int, transactions_all: list) -> str:
    """ """

    transactions = [o for o in transactions_all if str(label_integer) in o["label"]]

    return dict(
        closed_transactions=transactions,
        summing_closed_transaction=sum([o["amount"] for o in transactions]),
    )


def get_closed_open_transactions_under_same_label_int(
    transactions_all: list, label: str
) -> list:
    """ """
    label_integer = get_label_integer(label)

    return [o for o in transactions_all if str(label_integer) in o["label"]]


async def closing_orphan_order(
    label: str,
    label_integer: int,
    trade_table: str,
    where_filter: str,
    transactions_under_label_main: list,
) -> None:
    """ """

    closed_transaction_size = abs(
        [o["amount"] for o in transactions_under_label_main if label in o["label"]][0]
    )

    log.info(f" closed_transaction_size label {closed_transaction_size}")

    open_label_with_same_size_as_closed_label_int = [
        o
        for o in transactions_under_label_main
        if "open" in o["label"] and closed_transaction_size == abs(o["amount"])
    ]

    # log.warning(F"open_label_with_same_size_as_closed_label {open_label_with_same_size_as_closed_label_int}")

    if open_label_with_same_size_as_closed_label_int:

        open_label_with_the_same_label_int = [
            o
            for o in open_label_with_same_size_as_closed_label_int
            if label_integer in o["label"]
        ]

        log.warning(f"label_integer {label_integer}")

        if False and open_label_with_the_same_label_int:
            log.error(
                f"open_label_with_the_same_label_int {open_label_with_the_same_label_int}"
            )

            trade_id = closed_transaction[where_filter]

            # insert closed transaction to db for closed transaction
            await db_mgt.insert_tables("my_trades_closed_json", closed_transaction)

            # delete respective transaction form active db
            await db_mgt.deleting_row(
                trade_table,
                "databases/trading.sqlite3",
                where_filter,
                "=",
                trade_id,
            )


async def updating_status_closed_transactions(
    closed_transaction: dict,
    where_filter: str,
    trade_table: str,
) -> None:
    """ """
    trade_id = closed_transaction[where_filter]
    # trade_tabel = f"my_trades_all_{currency.lower()}.json"

    await db_mgt.update_status_data(
        trade_table,
        "is_open",
        where_filter,
        trade_id,
        0,
        "=",
    )


async def closing_one_to_one(
    transaction_closed_under_the_same_label_int: dict,
    where_filter: str,
    trade_table: str,
) -> None:
    """ """

    for transaction in transaction_closed_under_the_same_label_int:

        await updating_status_closed_transactions(
            transaction,
            where_filter,
            trade_table,
        )


async def closing_one_to_many_single_open_order(
    currency: str,
    open_label: dict,
    transaction_closed_under_the_same_label_int,
    where_filter: str,
    trade_table: str,
) -> None:

    open_label_size = abs(open_label["amount"])
    log.warning(f"open_label_size {open_label_size}")

    closed_label_with_same_size_as_open_label = [
        o
        for o in transaction_closed_under_the_same_label_int
        if "closed" in o["label"] and abs(o["amount"]) == open_label_size
    ]

    log.warning(
        f"closed_label_with_same_size_as_open_label{closed_label_with_same_size_as_open_label}"
    )

    if closed_label_with_same_size_as_open_label:

        closed_label_with_same_size_as_open_label_int = str(
            str_mod.extract_integers_aggregation_from_text(
                "trade_id", min, closed_label_with_same_size_as_open_label
            )
        )
        log.warning(
            f"closed_label_with_same_size_as_open_label_int {closed_label_with_same_size_as_open_label_int}"
        )

        closed_label_ready_to_close = (
            [
                o
                for o in closed_label_with_same_size_as_open_label
                if closed_label_with_same_size_as_open_label_int in o["trade_id"]
            ]
        )[0]

        log.debug(f"closed_label_ready_to_close{closed_label_ready_to_close}")

        # closed one of closing order
        await updating_status_closed_transactions(
            closed_label_ready_to_close, where_filter, trade_table
        )


async def closing_one_to_many(
    currency: str,
    open_label: str,
    transaction_closed_under_the_same_label_int: dict,
    where_filter: str,
    trade_table: str,
) -> None:
    """ """

    len_open_label_size = len(open_label)

    if len_open_label_size == 1:
        log.info(f"len_open_label_size 1 {len_open_label_size}")

        await closing_one_to_many_single_open_order(
            currency,
            open_label[0],
            transaction_closed_under_the_same_label_int,
            where_filter,
            trade_table,
        )

    else:
        for transaction in open_label:
            log.warning(f"len_open_label_size > 1 {len_open_label_size}")

            await closing_one_to_many_single_open_order(
                currency,
                transaction,
                transaction_closed_under_the_same_label_int,
                where_filter,
                trade_table,
            )


async def clean_up_closed_transactions(
    trade_table: str,
    transaction_all: list = None,
) -> None:
    """
    closed transactions: buy and sell in the same label id = 0. When flagged:
    1. remove them from db for open transactions/my_trades_all_json
    2. delete them from active trade db
    """

    # prepare basic parameters for table query

    # log.critical(f" {"clean_up_closed_transactions".upper()} {instrument_name} START")

    where_filter = f"trade_id"

    try:

        # filtered transactions with closing labels
        if transaction_all:

            transaction_with_closed_labels = get_transactions_with_closed_label(
                transaction_all
            )

            if transaction_with_closed_labels:

                labels_only = str_mod.remove_redundant_elements(
                    [o["label"] for o in transaction_with_closed_labels]
                )

                for label in labels_only:

                    transactions_under_label_main = get_label_main(
                        transaction_all, label
                    )

                    label_integer = get_label_integer(label)

                    closed_transactions_all = transactions_under_label_int(
                        label_integer, transactions_under_label_main
                    )

                    size_to_close = closed_transactions_all[
                        "summing_closed_transaction"
                    ]

                    transaction_closed_under_the_same_label_int = (
                        closed_transactions_all["closed_transactions"]
                    )

                    # log.error(f"closed_transactions_all {closed_transactions_all}")

                    if size_to_close == 0:

                        instrument_name_under_the_same_label_int = [
                            o["instrument_name"]
                            for o in transaction_closed_under_the_same_label_int
                        ]

                        for instrument_name in instrument_name_under_the_same_label_int:

                            instrument_transactions = [
                                o
                                for o in transaction_closed_under_the_same_label_int
                                if instrument_name in o["instrument_name"]
                            ]

                            sum_instrument_transactions = sum(
                                [o["amount"] for o in instrument_transactions]
                            )

                            if sum_instrument_transactions == 0:
                                # log.info(F" instrument_transactions {instrument_transactions}")

                                await closing_one_to_one(
                                    instrument_transactions,
                                    where_filter,
                                    trade_table,
                                )

                    if size_to_close != 0:

                        open_label = [
                            o
                            for o in transaction_closed_under_the_same_label_int
                            if "open" in o["label"]
                        ]

                        transactions = []
                        for open_transaction in open_label:

                            open_transaction_size = open_transaction["amount"]
                            open_transaction_instrument_name = open_transaction[
                                "instrument_name"
                            ]

                            closed_transaction_with_same_size_as_open_label = [
                                o
                                for o in transaction_closed_under_the_same_label_int
                                if "closed" in o["label"]
                                and abs(o["amount"]) == abs(open_transaction_size)
                            ]

                            if closed_transaction_with_same_size_as_open_label:

                                for (
                                    closed_transaction
                                ) in closed_transaction_with_same_size_as_open_label:

                                    closed_transaction_size = closed_transaction[
                                        "amount"
                                    ]
                                    closed_transaction_instrument_name = (
                                        closed_transaction["instrument_name"]
                                    )

                                    if (
                                        closed_transaction_instrument_name
                                        == open_transaction_instrument_name
                                        and closed_transaction_size
                                        + open_transaction_size
                                        == 0
                                    ):

                                        transactions.append(open_transaction)
                                        transactions.append(closed_transaction)

                                        log.critical(f"transactions {transactions}")
                                        log.warning(
                                            f"closed_transaction_instrument_name {closed_transaction_instrument_name}"
                                        )
                                        log.error(
                                            f"open_transaction_instrument_name {open_transaction_instrument_name}"
                                        )

                                        await closing_one_to_one(
                                            transactions,
                                            where_filter,
                                            trade_table,
                                        )

                                        if (
                                            closed_transaction_instrument_name
                                            == open_transaction_instrument_name
                                        ):

                                            break

    except Exception as error:

        error_handling.parse_error_message(error)
