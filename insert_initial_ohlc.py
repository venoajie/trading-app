#!/usr/bin/python3
# -*- coding: utf-8 -*-

# installed
import asyncio
import json

# built ins
from datetime import datetime, timedelta

from loguru import logger as log

# user defined formula
from core.db.postgres import insert_ohlc
from src.shared.utils import error_handling
from src.shared.config.settings import DERIBIT_CURRENCIES
from src.scripts.deribit.restful_api import end_point_params_template as end_point


async def insert_ohlc(
    currency,
    instrument_name: str,
    resolution: int = 1,
    qty_candles: int = 6000,
) -> None:

    ohlc_request = await end_point.get_ohlc(
        instrument_name, resolution, qty_candles, None, True
    )

    log.info(ohlc_request)

    result = transform_nested_dict_to_list(ohlc_request)

    table = f"ohlc{resolution}_{currency.lower()}_perp_json"

    for data in result:
        log.debug(f"insert tables {table}")
        await sqlite_management.insert_tables(table, data)


async def main():

    currencies = DERIBIT_CURRENCIES

    for currency in currencies:

        instrument_name = f"{currency.upper()}-PERPETUAL"

        log.critical(f"instrument_name {instrument_name} currency {currency}")

        resolutions = [60, 5, 15, 1]

        qty_candles = 6000

        for res in resolutions:

            await insert_ohlc(currency, instrument_name, res, qty_candles)


if __name__ == "__main__":
    try:
        asyncio.get_event_loop().run_until_complete(main())

    except (KeyboardInterrupt, SystemExit):
        asyncio.get_event_loop().run_until_complete(main().stop_ws())

    except Exception as error:
        await error_handling.parse_error_message(
            error,
        )
