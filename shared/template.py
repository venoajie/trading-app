# -*- coding: utf-8 -*-


def redis_message_template() -> str:
    """ """

    result = {}
    result.update({"params": {}})
    result.update({"method": "subscription"})
    result["params"].update({"data": None})
    result["params"].update({"channel": None})
    result["params"].update({"stream": None})

    return result


def trade_template() -> str:
    """
    combining result from websocket (user changes/trade)
        and rest API (get_transaction_log)

        instrument_name: str=None,
        amount: int=None,
        price: float=None,
        side: str=None,
        direction: str=None,
        position: int=None,
        currency: str=None,
        timestamp: int=None,
        trade_id: str=None,
        order_id: str=None,
        user_seq: int=None

    label is not registered since it needed as identification for no label
    """
    return dict(
        instrument_name=None,
        amount=None,
        price=None,
        side=None,
        direction=None,
        position=None,
        currency=None,
        timestamp=None,
        trade_id=None,
        order_id=None,
        user_seq=None,
        label=None,
        sl=None,
        tp=None,
    )


def get_custom_label(transaction: dict) -> str:

    try:

        side = transaction["direction"]

    except:

        if "sell" in transaction["side"]:
            side = "sell"

        if "buy" in transaction["side"]:
            side = "buy"

    side_label = "Short" if side == "sell" else "Long"

    try:
        last_update = transaction["timestamp"]
    except:
        try:
            last_update = transaction["last_update_timestamp"]
        except:
            last_update = transaction["creation_timestamp"]

    return f"custom{side_label.title()}-open-{last_update}"


def get_custom_label_oto(transaction: dict) -> dict:

    side = transaction["direction"]
    side_label = "Short" if side == "sell" else "Long"

    try:
        last_update = transaction["timestamp"]
    except:
        try:
            last_update = transaction["last_update_timestamp"]
        except:
            last_update = transaction["creation_timestamp"]

    return dict(
        open=(f"custom{side_label.title()}-open-{last_update}"),
        closed=(f"custom{side_label.title()}-closed-{last_update}"),
    )
