
from trading_app.restful_api import connector


def get_basic_https() -> str:
    return f"https://api.telegram.org/bot"


def message_end_point(
    bot_token: str,
    bot_chatID: str,
    bot_message: str,
) -> str:

    return (
        bot_token
        + ("/sendMessage?chat_id=")
        + bot_chatID
        + ("&parse_mode=HTML&text=")
        + str(bot_message)
    )


async def send_message(
    bot_token: str,
    bot_chatID: str,
    bot_message: str,
) -> None:
    return await connector.get_connected(
        get_basic_https(),
        message_end_point(bot_token, bot_chatID, bot_message),
    )
