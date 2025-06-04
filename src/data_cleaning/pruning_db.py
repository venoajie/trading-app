# -*- coding: utf-8 -*-

# built ins
import asyncio

# user defined formula
from streaming_helper.db_management import sqlite_management as db_mgt
from streaming_helper.utilities import error_handling


async def count_and_delete_ohlc_rows(
    database: str,
    table: str,
) -> None:

    try:
        rows_threshold = max_rows(table)

        if "supporting_items_json" in table or "account_summary_json" in table:
            where_filter = f"id"

        else:
            where_filter = f"tick"

        count_rows_query = db_mgt.querying_arithmetic_operator(
            where_filter, "COUNT", table
        )

        rows = await db_mgt.executing_query_with_return(count_rows_query)

        rows = (
            rows[0]["COUNT (tick)"] if where_filter == "tick" else rows[0]["COUNT (id)"]
        )

        if rows > rows_threshold:

            first_tick_query = db_mgt.querying_arithmetic_operator(
                where_filter, "MIN", table
            )

            first_tick_fr_sqlite = await db_mgt.executing_query_with_return(
                first_tick_query
            )

            if where_filter == "tick":
                first_tick = first_tick_fr_sqlite[0]["MIN (tick)"]

            if where_filter == "id":
                first_tick = first_tick_fr_sqlite[0]["MIN (id)"]

            await db_mgt.deleting_row(
                table,
                database,
                where_filter,
                "=",
                first_tick,
            )

    except Exception as error:
        error_handling.parse_error_message(error)


def max_rows(table) -> int:
    """ """
    if "market_analytics_json" in table:
        threshold = 10
    if "ohlc" in table:
        threshold = 10000
    if "supporting_items_json" in table:
        threshold = 200
    if "account_summary_json" in table:
        downloading_times = 2  # every 30 seconds
        currencies = 2
        instruments = 8
        threshold = (
            downloading_times * currencies * instruments
        ) * 120  # roughly = 2 hours

    return threshold


async def clean_up_databases(idle_time) -> None:
    """ """

    while True:

        tables = [
            "market_analytics_json",
            # "account_summary_json",
            "ohlc1_eth_perp_json",
            "ohlc1_btc_perp_json",
            "ohlc15_eth_perp_json",
            "ohlc15_btc_perp_json",
            # "ohlc30_eth_perp_json",
            "ohlc60_eth_perp_json",
            # "ohlc3_eth_perp_json",
            # "ohlc3_btc_perp_json",
            "ohlc5_eth_perp_json",
            "ohlc5_btc_perp_json",
        ]

        database: str = "databases/trading.sqlite3"

        for table in tables:

            await count_and_delete_ohlc_rows(database, table)

        await asyncio.sleep(idle_time)
