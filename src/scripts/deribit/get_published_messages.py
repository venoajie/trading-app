#src\scripts\deribit\get_published_messages.py

# built ins
import asyncio

# installed
import orjson


async def get_redis_message(message_byte: bytes) -> dict:
    """ """

    if message_byte and message_byte["type"] == "message":

        message_byte_data = orjson.loads(message_byte["data"])

        params = message_byte_data["params"]

        return dict(
            data=params["data"],
            channel=params["channel"],
            message_all=message_byte_data,
        )

    else:

        return dict(
            data=[],
            channel=[],
            message_all=[],
        )
