# -*- coding: utf-8 -*-

# built ins
import asyncio

# installed
import orjson
from loguru import logger as log

# user defined formula
from shared.db.redis import publishing_result
from shared.db.sqlite import (
    executing_query_with_return,
    insert_tables,
    querying_arithmetic_operator,
    update_status_data,
)
from restful_api.deribit import end_point_params_template as end_point
from restful_api import connector
from shared.utils import error_handling, string_modification as str_mod


async def last_tick_fr_sqlite(last_tick_query_ohlc1: str) -> int:
    """ """
    last_tick = await executing_query_with_return(last_tick_query_ohlc1)

    return last_tick[0]["MAX (tick)"]


async def updating_ohlc(
    client_redis: object,
    redis_channels: list,
) -> None:
    """ """

    try:

        # connecting to redis pubsub
        pubsub: object = client_redis.pubsub()

        chart_channel: str = redis_channels["chart_update"]
        chart_low_high_tick_channel: str = redis_channels["chart_low_high_tick"]

        # prepare channels placeholders
        channels = [
            chart_channel,
        ]

        # subscribe to channels
        [await pubsub.subscribe(o) for o in channels]

        WHERE_FILTER_TICK: str = "tick"

        is_updated = True

        start_timestamp = 0

        basic_https_connection_url = end_point.basic_https()

        endpoint_tradingview = end_point.endpoint_tradingview()

        while is_updated:

            try:

                message_byte = await pubsub.get_message()

                #                log.info(f"message_byte {message_byte}")

                if message_byte and message_byte["type"] == "message":

                    message_byte_data = orjson.loads(message_byte["data"])

                    message_channel = message_byte["channel"]

                    if chart_channel in message_channel:

                        data = message_byte_data["data"]

                        instrument_name = message_byte_data["instrument_name"]

                        currency = message_byte_data["currency"]

                        resolution = message_byte_data["resolution"]

                        end_timestamp = data["tick"]

                        table_ohlc = f"ohlc{resolution}_{currency.lower()}_perp_json"

                        last_tick_query_ohlc_resolution: str = (
                            querying_arithmetic_operator(
                                WHERE_FILTER_TICK,
                                "MAX",
                                table_ohlc,
                            )
                        )

                        # need cached
                        start_timestamp: int = await last_tick_fr_sqlite(
                            last_tick_query_ohlc_resolution
                        )

                        delta_time = end_timestamp - start_timestamp

                        pub_message = dict(
                            instrument_name=instrument_name,
                            currency=currency,
                            resolution=resolution,
                        )

                        if delta_time == 0:

                            # refilling current ohlc table with updated data
                            await update_status_data(
                                table_ohlc,
                                "data",
                                end_timestamp,
                                WHERE_FILTER_TICK,
                                data,
                                "is",
                            )

                            if resolution != 1:

                                table_ohlc = (
                                    f"ohlc{resolution}_{currency.lower()}_perp_json"
                                )

                                ohlc_query = f"SELECT data FROM {table_ohlc} WHERE tick = {end_timestamp}"

                                result_from_sqlite = await executing_query_with_return(
                                    ohlc_query
                                )

                                high_from_ws = data["high"]
                                low_from_ws = data["low"]

                                ohlc_from_sqlite = str_mod.remove_apostrophes_from_json(
                                    o["data"] for o in result_from_sqlite
                                )[0]

                                high_from_db = ohlc_from_sqlite["high"]
                                low_from_db = ohlc_from_sqlite["low"]

                                if (
                                    high_from_ws > high_from_db
                                    or low_from_ws < low_from_db
                                ):

                                    await publishing_result(
                                        client_redis,
                                        chart_low_high_tick_channel,
                                        pub_message,
                                    )

                        else:

                            endpoint_ohlc = end_point.get_ohlc_end_point(
                                endpoint_tradingview,
                                instrument_name,
                                resolution,
                                start_timestamp,
                                end_timestamp,
                                False,
                            )

                            # catch up data through FIX
                            ohlc = await connector.get_connected(
                                basic_https_connection_url,
                                endpoint_ohlc,
                            )

                            result_all = str_mod.transform_nested_dict_to_list_ohlc(
                                ohlc
                            )

                            await publishing_result(
                                client_redis,
                                chart_low_high_tick_channel,
                                pub_message,
                            )

                            for result in result_all:

                                await insert_tables(
                                    table_ohlc,
                                    result,
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


async def inserting_open_interest(
    currency,
    WHERE_FILTER_TICK,
    TABLE_OHLC1,
    data_orders,
) -> None:
    """ """

    if (
        currency_inline_with_database_address(currency, TABLE_OHLC1)
        and "open_interest" in data_orders
    ):

        open_interest = data_orders["open_interest"]

        last_tick_query_ohlc1: str = querying_arithmetic_operator(
            "tick", "MAX", TABLE_OHLC1
        )

        last_tick1_fr_sqlite: int = await last_tick_fr_sqlite(last_tick_query_ohlc1)

        await update_status_data(
            TABLE_OHLC1,
            "open_interest",
            last_tick1_fr_sqlite,
            WHERE_FILTER_TICK,
            open_interest,
            "is",
        )



def currency_inline_with_database_address(
    currency: str,
    database_address: str,
) -> bool:

    return currency.lower() in str(database_address)
