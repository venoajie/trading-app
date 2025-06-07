# src\shared\utils\template.py


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
    
    result_example = {
            'trades': [
                {
                    'label': 'customLong-open-1746435478513',
                    'timestamp': 1746435505432,
                    'state': 'filled',
                    'price': 1827.75,
                    'amount': 1.0,
                    'direction': 'buy',
                    'index_price': 1827.94,
                    'profit_loss': 0.0,
                    'instrument_name': 'ETH-PERPETUAL',
                    'trade_seq': 179290420,
                    'api': True,
                    'mark_price': 1827.67,
                    'order_id': 'ETH-67544635984',
                    'matching_id': None,
                    'tick_direction': 3,
                    'fee': 0.0,
                    'mmp': False,
                    'self_trade': False,
                    'post_only': True,
                    'reduce_only': False,
                    'contracts': 1.0,
                    'trade_id': 'ETH-247401821',
                    'fee_currency': 'ETH',
                    'order_type': 'limit',
                    'risk_reducing': False,
                    'liquidity': 'M'
                    }
                ]
            }
    
    sl = stop loss
    tp = take profit
    both variables determined by strategy and provided before order was sent to exchange. 
        they could be adjusted later on
    
    the price is derived from the price of the trade. 
        however, they could be adjusted later on (esepcially when averaging down/up)
    """
    return dict(
        instrument_name=None,
        amount=None,
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
