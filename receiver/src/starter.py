# -*- coding: utf-8 -*-

# built ins
import asyncio

# installed
from loguru import logger as log

# user defined formulas
from shared.db_management import sqlite_management as db_mgt
from restful_api.deribit import end_point_params_template as end_point
from shared import (
    error_handling,
    pickling,
    string_modification as str_mod,
    system_tools,
    time_modification as time_mod,
)


async def initial_procedures(
    private_data: object,
    client_redis: object,
    config_app: list,
) -> None:

    try:

        # get tradable strategies
        tradable_config_app = config_app["tradable"]

        # get TRADABLE currencies
        currencies: list = [o["spot"] for o in tradable_config_app][0]

        # get ALL traded currencies in deribit
        get_currencies_all = await end_point.get_currencies()

        all_exc_currencies = [o["currency"] for o in get_currencies_all["result"]]

        server_time = time_mod.get_now_unix_time()

        ONE_SECOND = 1000

        one_minute = ONE_SECOND * 60

        five_days_ago = server_time - (one_minute * 60 * 24 * 5)

        my_path_cur = system_tools.provide_path_for_file("currencies")

        pickling.replace_data(
            my_path_cur,
            all_exc_currencies,
        )

        for currency in all_exc_currencies:

            # get ALL traded currencies in deribit
            instruments = await end_point.get_instruments(currency)

            my_path_instruments = system_tools.provide_path_for_file(
                "instruments", currency
            )

            pickling.replace_data(
                my_path_instruments,
                instruments,
            )

    except Exception as error:

        await error_handling.parse_error_message_with_redis(
            client_redis,
            error,
        )


def portfolio_combining(
    portfolio_all: list,
    portfolio_channel: str,
    result_template: dict,
) -> dict:

    portfolio = (
        [o["portfolio"] for o in portfolio_all if o["type"] == "subaccount"][0]
    ).values()

    #! need to update currency to upper

    result_template["params"].update({"data": portfolio})
    result_template["params"].update({"channel": portfolio_channel})

    return result_template


def my_trades_active_combining(
    my_trades_active_all: list,
    my_trades_channel: str,
    result_template: dict,
) -> dict:

    result_template["params"].update({"data": my_trades_active_all})
    result_template["params"].update({"channel": my_trades_channel})

    return result_template


def sub_account_combining(
    sub_accounts: list,
    sub_account_cached_channel: str,
    result_template: dict,
) -> dict:

    orders_cached = []
    positions_cached = []

    try:

        for sub_account in sub_accounts:

            sub_account = sub_account[0]

            sub_account_orders = sub_account["open_orders"]

            if sub_account_orders:

                for order in sub_account_orders:

                    orders_cached.append(order)

            sub_account_positions = sub_account["positions"]

            if sub_account_positions:

                for position in sub_account_positions:

                    positions_cached.append(position)

        sub_account = dict(
            orders_cached=orders_cached,
            positions_cached=positions_cached,
        )

        result_template["params"].update({"data": sub_account})
        result_template["params"].update({"channel": sub_account_cached_channel})

        return result_template

    except:

        sub_account = dict(
            orders_cached=[],
            positions_cached=[],
        )

        result_template["params"].update({"data": sub_account})
        result_template["params"].update({"channel": sub_account_cached_channel})

        return result_template


def is_order_allowed_combining(
    all_instruments_name: list,
    order_allowed_channel: str,
    result_template: dict,
) -> dict:

    combined_order_allowed = []
    for instrument_name in all_instruments_name:

        currency: str = str_mod.extract_currency_from_text(instrument_name)

        if "-FS-" in instrument_name:
            size_is_reconciled = 1

        else:
            size_is_reconciled = 0

        order_allowed = dict(
            instrument_name=instrument_name,
            size_is_reconciled=size_is_reconciled,
            currency=currency,
        )

        combined_order_allowed.append(order_allowed)

    result_template["params"].update({"data": combined_order_allowed})
    result_template["params"].update({"channel": order_allowed_channel})

    return result_template
