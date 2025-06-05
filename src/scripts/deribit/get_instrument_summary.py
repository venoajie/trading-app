# -*- coding: utf-8 -*-

# built ins
import asyncio

# user defined formula
from src.scripts.deribit.restful_api.deribit import end_point_params_template as end_point
from src.shared.utils.pickling import read_data
from src.shared.utils.string_modification import (
    remove_double_brackets_in_list,
)
from src.shared.utils.system_tools import provide_path_for_file


def get_instruments_kind(
    currency: str,
    settlement_periods: list,
    kind: str = "all",
    result: list = None,
) -> list:
    """_summary_

    Args:
        currency (str): _description_
        kind (str): "future_combo",  "future"
        Instance:  [
                    {'tick_size_steps': [], 'quote_currency': 'USD', 'min_trade_amount': 1,'counter_currency': 'USD',
                    'settlement_period': 'month', 'settlement_currency': 'ETH', 'creation_timestamp': 1719564006000,
                    'instrument_id': 342036, 'base_currency': 'ETH', 'tick_size': 0.05, 'contract_size': 1, 'is_active': True,
                    'expiration_timestamp': 1725004800000, 'instrument_type': 'reversed', 'taker_commission': 0.0,
                    'maker_commission': 0.0, 'instrument_name': 'ETH-FS-27SEP24_30AUG24', 'kind': 'future_combo',
                    'rfq': False, 'price_index': 'eth_usd'}, ]
     Returns:
        list: _description_


    """

    if not result:

        my_path_instruments = provide_path_for_file("instruments", currency)

        instruments_raw = read_data(my_path_instruments)

        result = instruments_raw[0]["result"]

    non_spot_instruments = [o for o in result if o["kind"] != "spot"]

    instruments_kind = (
        non_spot_instruments
        if kind == "all"
        else [o for o in result if o["kind"] == kind]
    )

    return [o for o in instruments_kind if o["settlement_period"] in settlement_periods]


async def get_futures_for_active_currencies(
    active_currencies: list,
    settlement_periods: list,
) -> list:
    """_summary_

    Returns:
        list: _description_
    """

    instruments_holder_place = []
    for currency in active_currencies:

        result = await end_point.get_instruments(currency)

        future_instruments = get_instruments_kind(
            currency, settlement_periods, "future", result
        )

        future_combo_instruments = get_instruments_kind(
            currency, settlement_periods, "future_combo", result
        )

        active_combo_perp = [
            o for o in future_combo_instruments if "_PERP" in o["instrument_name"]
        ]

        combined_instruments = future_instruments + active_combo_perp
        instruments_holder_place.append(combined_instruments)

    # removing inner list
    # typical result: [['BTC-30AUG24', 'BTC-6SEP24', 'BTC-27SEP24', 'BTC-27DEC24',
    # 'BTC-28MAR25', 'BTC-27JUN25', 'BTC-PERPETUAL'], ['ETH-30AUG24', 'ETH-6SEP24',
    # 'ETH-27SEP24', 'ETH-27DEC24', 'ETH-28MAR25', 'ETH-27JUN25', 'ETH-PERPETUAL']]

    instruments_holder_plc = []
    for instr in instruments_holder_place:
        instruments_holder_plc.append(instr)

    return remove_double_brackets_in_list(instruments_holder_plc)


async def get_futures_instruments(
    active_currencies: list,
    settlement_periods: list,
) -> dict:

    active_futures = await get_futures_for_active_currencies(
        active_currencies, settlement_periods
    )

    min_expiration_timestamp = min([o["expiration_timestamp"] for o in active_futures])

    return dict(
        instruments_name=[o["instrument_name"] for o in (active_futures)],
        min_expiration_timestamp=min_expiration_timestamp,
        active_futures=[o for o in active_futures if "future" in o["kind"]],
        active_combo=[o for o in active_futures if "future_combo" in o["kind"]],
        instruments_name_with_min_expiration_timestamp=[
            o["instrument_name"]
            for o in active_futures
            if o["expiration_timestamp"] == min_expiration_timestamp
        ][0],
    )
