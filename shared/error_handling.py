# -*- coding: utf-8 -*-

import asyncio
import orjson
import traceback

from loguru import logger as log

from streaming_helper.utilities import template


def parse_error_message(
    error: str,
    message: str = None,
) -> str:
    """

    Capture & emit error message

    Args:
        message (str): error message

    Returns:
        trace back error message

    """
    info = f"{traceback.format_exception(error)}"

    if message != None:
        info = f"{message} {traceback.format_exception(error)}"

    log.critical(f"{info}")

    return info


async def parse_error_message_with_redis(
    client_redis: object,
    error: str,
    message: str = None,
) -> None:
    """ """

    channel = "error"

    try:

        result: dict = template.redis_message_template()

        info = parse_error_message(
            error,
            message,
        )

        pub_message = dict(
            data=info,
        )

        result["params"].update({"channel": channel})
        result["params"].update(pub_message)

        # publishing message
        await client_redis.publish(
            channel,
            orjson.dumps(result),
        )

    except Exception as error:

        parse_error_message(
            error,
            message,
        )
