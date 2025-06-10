# src\scripts\telegram\connector.py

"""
why aiohttp over httpx?
    - Our module is fully using asynchronous which is aiohttp spesialization
    - has more mature asyncio support than httpx
    - has BasicAuth support for authentication, which is not available in httpx
    - aiohttp is more suitable for applications that require high concurrency and low latency, such as web scraping or real-time data processing.

references:
    - https://github.com/encode/httpx/issues/3215#issuecomment-2157885121
    - https://github.com/encode/httpx/discussions/3100
    - https://brightdata.com/blog/web-data/requests-vs-httpx-vs-aiohttp


"""

# built ins
import asyncio

# installed
import aiohttp
from aiohttp.helpers import BasicAuth

# user defined formula
from src.scripts.telegram import (
    end_point_params_template as telegram_end_point,
)


async def get_connected(
    connection_url: str,
    endpoint: str = None,
    client_id: str = None,
    client_secret: str = None,
    params: str = None,
) -> None:

    async with aiohttp.ClientSession() as session:

        if client_id:

            if "telegram" in connection_url:

                response = await telegram_response(
                    session,
                    connection_url,
                    endpoint,
                    client_id,
                    client_secret,
                    params,
                )

            if "deribit" in connection_url:

                response: dict = await deribit_response(
                    session,
                    connection_url,
                    endpoint,
                    client_id,
                    client_secret,
                    params,
                )

        else:

            connect_end_point = (
                connection_url if endpoint is None else (connection_url + endpoint)
            )

            async with session.get(connect_end_point) as response:

                # RESToverHTTP Response Content
                response: dict = await response.json()

        return response


async def telegram_response(
    session: object,
    connection_url: str,
    endpoint: str,
    client_id: str,
    client_secret: str,
    params: str,
) -> None:
    """ """
    endpoint = telegram_end_point.message_end_point(client_id, client_secret, params)

    connection_endpoint = connection_url + endpoint

    async with session.get(connection_endpoint) as response:

        # RESToverHTTP Response Content
        return await response.json()

async def send_message(message: Dict[str, Any]):
    """Send formatted error message to Telegram"""
    text = (
        f"🚨 *{message['severity']} in {message['service']}*\n"
        f"_{message['timestamp']}_\n\n"
        f"*Error*: `{message['type']}`\n"
        f"*Message*: {message['message']}\n"
        f"*Context*: {message['context']}\n\n"
        f"```{message['stack_trace'][:1000]}```"
    )
    await get_connected(
        get_basic_https(),
        message_end_point(
            settings.TELEGRAM_BOT_TOKEN,
            settings.TELEGRAM_CHAT_ID,
            text
        )
    )