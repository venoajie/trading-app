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
from core.error_handler import error_handler
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

    if not ohlc_request:
        log.error(f"No OHLC data for {instrument_name}")
        return

    table = f"ohlc{resolution}_{currency.lower()}_perp"

    # Transform array-based OHLC to dict-per-candle format
    candles = []
    for i in range(len(ohlc_request["t"])):
        candle = {
            "tick": ohlc_request["t"][i],
            "open": ohlc_request["open"][i],
            "high": ohlc_request["high"][i],
            "low": ohlc_request["low"][i],
            "close": ohlc_request["close"][i],
            "volume": ohlc_request["volume"][i],
        }
        if "cost" in ohlc_request:
            candle["cost"] = ohlc_request["cost"][i]
        candles.append(candle)

    for candle in candles:
        await postgres_client.insert_ohlc(table, candle)


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
