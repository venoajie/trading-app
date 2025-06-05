# -*- coding: utf-8 -*-
"""_summary_"""
# built ins
import asyncio

# installed
from loguru import logger as log

# user defined formula
from streaming_helper.db_management import sqlite_management as db_mgt
from streaming_helper.utilities import string_modification as str_mod


def get_settlement_period(strategy_attributes) -> list:

    return str_mod.remove_redundant_elements(
        str_mod.remove_double_brackets_in_list(
            [o["settlement_period"] for o in strategy_attributes]
        )
    )


def is_instrument_name_has_delivered(
    instrument_name: list, instrument_attributes_futures_all
) -> bool:

    active_futures_instrument = [
        o["instrument_name"] for o in instrument_attributes_futures_all
    ]

    return instrument_name not in active_futures_instrument


async def clean_up_closed_futures_because_has_delivered_(
    instrument_name, transaction, delivered_transaction
) -> None:

    log.warning(f"instrument_name {instrument_name}")
    log.warning(f"transaction {transaction}")
    try:
        trade_id_sqlite = int(transaction["trade_id"])

    except:
        trade_id_sqlite = transaction["trade_id"]

    timestamp = transaction["timestamp"]

    closed_label = f"futureSpread-closed-{timestamp}"

    transaction.update({"instrument_name": instrument_name})
    transaction.update({"timestamp": timestamp})
    transaction.update({"price": transaction["price"]})
    transaction.update({"amount": transaction["amount"]})
    transaction.update({"label": transaction["label"]})
    transaction.update({"trade_id": trade_id_sqlite})
    transaction.update({"order_id": transaction["order_id"]})

    # log.warning(f"transaction {transaction}")
    await db_mgt.insert_tables("my_trades_closed_json", transaction)

    await db_mgt.deleting_row(
        "my_trades_all_json",
        "databases/trading.sqlite3",
        "trade_id",
        "=",
        trade_id_sqlite,
    )

    delivered_transaction = delivered_transaction[0]

    timestamp_from_transaction_log = delivered_transaction["timestamp"]

    try:
        price_from_transaction_log = delivered_transaction["price"]

    except:
        price_from_transaction_log = delivered_transaction["index_price"]

    closing_transaction = transaction
    closing_transaction.update({"label": closed_label})
    closing_transaction.update({"amount": (closing_transaction["amount"]) * -1})
    closing_transaction.update({"price": price_from_transaction_log})
    closing_transaction.update({"trade_id": None})
    closing_transaction.update({"order_id": None})
    closing_transaction.update({"timestamp": timestamp_from_transaction_log})

    await db_mgt.insert_tables("my_trades_closed_json", closing_transaction)


async def updating_delivered_instruments(
    archive_db_table: str, instrument_name: str
) -> None:
    """ """

    where_filter = f"instrument_name"

    await db_mgt.update_status_data(
        archive_db_table, "is_open", where_filter, instrument_name, 0, "="
    )
