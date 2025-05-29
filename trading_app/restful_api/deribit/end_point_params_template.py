# -*- coding: utf-8 -*-

import asyncio

from dataclassy import dataclass

# user defined formula
from trading_app.shared import (
    time_modification as time_mod,
    string_modification as str_mod,
)
from trading_app..restful_api import connector


def get_basic_https() -> str:
    return f"https://www.deribit.com/api/v2/"


def get_currencies_end_point() -> str:
    return f"public/get_currencies?"


def get_server_time_end_point() -> str:
    return f"public/get_time?"


def get_instruments_end_point(currency) -> str:
    return f"public/get_instruments?currency={currency.upper()}"


async def get_instruments(currency) -> str:

    result = await connector.get_connected(
        get_basic_https(),
        get_instruments_end_point(currency),
    )

    return result["result"]


def get_tickers_end_point(instrument_name: str) -> str:

    return f"public/ticker?instrument_name={instrument_name}"


async def get_ticker(instrument_name) -> str:

    result = await connector.get_connected(
        get_basic_https(),
        get_tickers_end_point(instrument_name),
    )

    return result["result"]


def get_tradingview_chart_data_end_point() -> str:
    return f"get_tradingview_chart_data?"


def get_ohlc_end_point(
    endpoint_tradingview: str,
    instrument_name: str,
    resolution: int,
    qty_or_start_time_stamp: int,
    provided_end_timestamp: int = None,
    qty_as_start_time_stamp: bool = False,
) -> str:

    now_unix = time_mod.get_now_unix_time()

    # start timestamp is provided
    start_timestamp = qty_or_start_time_stamp

    # recalculate start timestamp using qty as basis point
    if qty_as_start_time_stamp:
        start_timestamp = now_unix - (60000 * resolution) * qty_as_start_time_stamp

    if provided_end_timestamp:
        end_timestamp = provided_end_timestamp
    else:
        end_timestamp = now_unix

    return f"{endpoint_tradingview}end_timestamp={end_timestamp}&instrument_name={instrument_name}&resolution={resolution}&start_timestamp={start_timestamp}"


def get_json_payload(
    endpoint: str,
    params: dict,
) -> dict:

    id = id_numbering(
        endpoint,
        endpoint,
    )

    return {
        "jsonrpc": "2.0",
        "id": id,
        "method": f"{endpoint}",
        "params": params,
    }


def get_open_orders_end_point() -> str:
    return f"private/get_open_orders"


def get_open_orders_params(
    kind: str,
    type: str,
) -> dict:
    return {
        "kind": kind,
        "type": type,
    }


def get_subaccounts_end_point() -> str:
    return f"private/get_subaccounts"


def get_subaccounts_params(
    with_portfolio: bool = True,
) -> dict:
    return {
        "with_portfolio": with_portfolio,
    }


def get_subaccounts_details_end_point() -> str:
    return f"private/get_subaccounts_details"


def get_subaccounts_details_params(
    currency: str,
    with_open_orders: bool = True,
) -> dict:
    return {
        "currency": currency,
        "with_open_orders": with_open_orders,
    }


def get_transaction_log_end_point() -> str:
    return f"private/get_transaction_log"


def get_transaction_log_params(
    currency: str,
    start_timestamp: int,
    count: int = 1000,
    query: str = "trade",
) -> dict:

    now_unix = time_mod.get_now_unix_time()

    return {
        "count": count,
        "currency": currency,
        "end_timestamp": now_unix,
        "query": query,
        "start_timestamp": start_timestamp,
    }


def send_orders_end_point(side: dict) -> str:

    return f"private/{side}"


def send_orders_params(
    side: str,
    instrument,
    amount,
    label: str = None,
    price: float = None,
    type: str = "limit",
    otoco_config: list = None,
    linked_order_type: str = None,
    trigger_price: float = None,
    trigger: str = "last_price",
    time_in_force: str = "fill_or_kill",
    reduce_only: bool = False,
    post_only: bool = True,
    reject_post_only: bool = False,
) -> str:

    params = {}

    params.update({"instrument_name": instrument})
    params.update({"amount": amount})
    params.update({"label": label})
    params.update({"instrument_name": instrument})
    params.update({"type": type})

    if trigger_price is not None:

        params.update({"trigger": trigger})
        params.update({"trigger_price": trigger_price})
        params.update({"reduce_only": reduce_only})

    if "market" not in type:
        params.update({"price": price})
        params.update({"post_only": post_only})
        params.update({"reject_post_only": reject_post_only})

    if otoco_config:
        params.update({"otoco_config": otoco_config})

        if linked_order_type is not None:
            params.update({"linked_order_type": linked_order_type})
        else:
            params.update({"linked_order_type": "one_triggers_other"})

        params.update({"trigger_fill_condition": "incremental"})

    return params


def send_limit_order_params(
    params: dict,
) -> str:
    """ """

    # basic params
    side = params["side"]
    instrument = params["instrument_name"]
    label_numbered = params["label"]
    size = params["size"]
    type = params["type"]
    limit_prc = params["entry_price"]

    try:
        otoco_config = params["otoco_config"]
    except:
        otoco_config = None

    try:
        linked_order_type = params["linked_order_type"]

    except:
        linked_order_type = None

    order_result = None

    if side != None:

        if type == "limit":  # limit has various state

            order_result = send_orders_params(
                side,
                instrument,
                size,
                label_numbered,
                limit_prc,
                type,
                otoco_config,
            )

        else:

            trigger_price = params["trigger_price"]
            trigger = params["trigger"]

            order_result = send_orders_params(
                side,
                instrument,
                size,
                label_numbered,
                limit_prc,
                type,
                otoco_config,
                linked_order_type,
                trigger_price,
                trigger,
            )

    return order_result


def cancel_all_orders_end_point() -> str:
    return f"private/cancel_all"


def cancel_order_end_point() -> str:
    return f"private/cancel"


def get_cancel_order_params(
    order_id: str,
) -> dict:

    return {"order_id": order_id}


def get_user_trades_by_currency_end_point() -> str:
    return f"private/get_user_trades_by_currency"


def get_user_trades_by_instrument_and_time_end_point() -> str:
    return f"private/get_user_trades_by_instrument_and_time"


def get_user_trades_by_instrument_and_time_params(
    instrument_name: str,
    start_timestamp: int,
    historical: bool = True,
    count: int = 1000,
) -> dict:

    now_unix = time_mod.get_now_unix_time()

    return {
        "count": count,
        "end_timestamp": now_unix,
        "instrument_name": instrument_name,
        "historical": historical,
        "start_timestamp": start_timestamp,
    }


def simulate_portfolio_end_point() -> str:
    return f"private/simulate_portfolio"


def simulate_portfolio_params(
    instrument_name: str,
    position: int,
    add_positions: bool = True,
) -> dict:

    currency = str_mod.extract_currency_from_text(instrument_name)

    instrument_name_text = f"{instrument_name}"

    return {
        "currency": currency,
        "add_positions": add_positions,
        "simulated_positions": {instrument_name_text: position},
    }


@dataclass(unsafe_hash=True, slots=True)
class SendApiRequest:
    """ """

    client_id: str
    client_secret: str

    async def simulate_portfolio(
        self,
        instrument_name: str,
        position: int,
        add_positions: bool = True,
    ) -> None:
        """
        result_example = {
            "jsonrpc": "2.0",
            "id": 5,
            "result": {
                "total_delta_total_usd": 5.30812019,
                "options_theta_map": {},
                "projected_maintenance_margin": 0.000727,
                "maintenance_margin": 0.000727,
                "initial_margin": 0.000979,
                "futures_session_rpl": 0,
                "spot_reserve": 0,
                "balance": 0.000567,
                "available_subaccount_transfer_funds": 0,
                "options_theta": 0,
                "margin_balance": 0.002644,
                "cross_collateral_enabled": True,
                "total_maintenance_margin_usd": 1.334470538932,
                "options_session_rpl": 0,
                "projected_delta_total": 0.002742,
                "locked_balance": 0,
                "options_vega": 0,
                "portfolio_margining_enabled": True,
                "options_value": 0,
                "equity": 0.000574,
                "futures_session_upl": 0.000006,
                "futures_pl": -0.000068,
                "session_upl": 0.000006,
                "total_margin_balance_usd": 4.852886723,
                "delta_total": 0.002742,
                "options_vega_map": {},
                "total_pl": -0.000068,
                "margin_model": "cross_pm",
                "options_session_upl": 0,
                "options_delta": 0,
                "fee_balance": 0,
                "session_rpl": 0,
                "currency": "ETH",
                "available_funds": 0.001664,
                "options_pl": 0,
                "options_gamma": 0,
                "projected_initial_margin": 0.000979,
                "available_withdrawal_funds": 0.000567,
                "total_initial_margin_usd": 1.797687367,
                "total_equity_usd": 4.852886723,
                "delta_total_map": {
                "eth_usd": 0.002742375
                },
                "additional_reserve": 0,
                "options_gamma_map": {}
                },
            "usIn": 1746618638373417,
            "usOut": 1746618638377054,
            "usDiff": 3637,
            "testnet": False
            }

        """

        portfolio_simulation = await connector.get_connected(
            get_basic_https(),
            simulate_portfolio_end_point(),
            self.client_id,
            self.client_secret,
            simulate_portfolio_params(
                instrument_name,
                position,
                add_positions,
            ),
        )
        
        return portfolio_simulation["result"]

    async def get_cancel_order_byOrderId(
        self,
        order_id: str,
    ) -> None:
        """ """

        cancel_order = await connector.get_connected(
            get_basic_https(),
            cancel_order_end_point(),
            self.client_id,
            self.client_secret,
            get_cancel_order_params(
                order_id,
            ),
        )

        return cancel_order["result"]

    async def send_limit_order(
        self,
        params: dict,
    ) -> None:
        """ """

        # basic params
        side = params["side"]
        instrument = params["instrument_name"]
        label_numbered = params["label"]
        size = params["size"]
        type = params["type"]
        limit_prc = params["entry_price"]

        try:
            otoco_config = params["otoco_config"]
        except:
            otoco_config = None

        try:
            linked_order_type = params["linked_order_type"]

        except:
            linked_order_type = None

        order_result = None

        if side != None:

            if type == "limit":  # limit has various state

                order_result = await self.send_order(
                    side,
                    instrument,
                    size,
                    label_numbered,
                    limit_prc,
                    type,
                    otoco_config,
                )

            else:

                trigger_price = params["trigger_price"]
                trigger = params["trigger"]

                order_result = await self.send_order(
                    side,
                    instrument,
                    size,
                    label_numbered,
                    limit_prc,
                    type,
                    otoco_config,
                    linked_order_type,
                    trigger_price,
                    trigger,
                )

        # log.warning(f"order_result {order_result}")

        if order_result != None and (
            "error" in order_result or "message" in order_result
        ):

            error = order_result["error"]
            message = error["message"]

            try:
                data = error["data"]
            except:
                data = message

        return order_result

    async def send_order(
        self,
        side: str,
        instrument,
        amount,
        label: str = None,
        price: float = None,
        type: str = "limit",
        otoco_config: list = None,
        linked_order_type: str = None,
        trigger_price: float = None,
        trigger: str = "last_price",
        time_in_force: str = "fill_or_kill",
        reduce_only: bool = False,
        post_only: bool = True,
        reject_post_only: bool = False,
    ) -> None:

        params = {}

        params.update({"instrument_name": instrument})
        params.update({"amount": amount})
        params.update({"label": label})
        params.update({"instrument_name": instrument})
        params.update({"type": type})

        if trigger_price is not None:

            params.update({"trigger": trigger})
            params.update({"trigger_price": trigger_price})
            params.update({"reduce_only": reduce_only})

        if "market" not in type:
            params.update({"price": price})
            params.update({"post_only": post_only})
            params.update({"reject_post_only": reject_post_only})

        if otoco_config:
            params.update({"otoco_config": otoco_config})

            if linked_order_type is not None:
                params.update({"linked_order_type": linked_order_type})
            else:
                params.update({"linked_order_type": "one_triggers_other"})

            params.update({"trigger_fill_condition": "incremental"})

        result = None

        if side is not None:

            result = await connector.get_connected(
                get_basic_https(),
                send_orders_end_point(side),
                self.client_id,
                self.client_secret,
                send_orders_params(
                    side,
                    instrument,
                    amount,
                    label,
                    price,
                    type,
                    otoco_config,
                    linked_order_type,
                    trigger_price,
                    trigger,
                    time_in_force,
                    reduce_only,
                    post_only,
                    reject_post_only,
                ),
            )

        return result

    async def get_subaccounts(
        self,
        with_portfolio: bool = True,
    ) -> dict:

        sub_account = await connector.get_connected(
            get_basic_https(),
            get_subaccounts_end_point(),
            self.client_id,
            self.client_secret,
            get_subaccounts_params(with_portfolio),
        )

        return sub_account["result"]

    async def get_subaccounts_details(
        self,
        currency: str,
        with_open_orders: bool = True,
    ) -> dict:

        sub_account = await connector.get_connected(
            get_basic_https(),
            get_subaccounts_details_end_point(),
            self.client_id,
            self.client_secret,
            get_subaccounts_details_params(
                currency,
                with_open_orders,
            ),
        )

        return sub_account["result"]

    async def get_transaction_log(
        self,
        currency: str,
        start_timestamp: int,
        count: int = 1000,
        query: str = "trade",
    ) -> list:

        result_transaction_log = await connector.get_connected(
            get_basic_https(),
            get_transaction_log_end_point(),
            self.client_id,
            self.client_secret,
            get_transaction_log_params(
                currency,
                start_timestamp,
                count,
                query,
            ),
        )

        result = result_transaction_log["result"]

        return [] if not result else result["logs"]

    async def get_open_orders(
        self,
        kind: str,
        type: str,
    ) -> list:

        result_open_orders = await connector.get_connected(
            get_basic_https(),
            get_open_orders_end_point(),
            self.client_id,
            self.client_secret,
            get_open_orders_params(
                kind,
                type,
            ),
        )

        return result_open_orders["result"]

    async def get_user_trades_by_instrument_and_time(
        self,
        instrument_name: str,
        start_timestamp: int,
        historical: True,
        count: 1000,
    ) -> list:
        """
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
                ],
            'has_more': False
            }

        """

        result_trades = await connector.get_connected(
            get_basic_https(),
            get_user_trades_by_instrument_and_time_end_point(),
            self.client_id,
            self.client_secret,
            get_user_trades_by_instrument_and_time_params(
                instrument_name,
                start_timestamp,
                historical,
                count,
            ),
        )

        return result_trades["result"]["trades"]


def get_user_trades_by_currency_params(
    currency: str,
    kind: str = "any",
    count: int = 1000,
) -> dict:
    return {
        "currency": currency,
        "kind": kind,
        "count": count,
    }


def get_api_end_point(
    endpoint: str,
    parameters: dict = None,
) -> dict:
    """
    used in data receiver deribit

    """

    private_endpoint = f"private/{endpoint}"

    params = {}
    params.update({"jsonrpc": "2.0"})
    params.update({"method": private_endpoint})
    if endpoint == "get_subaccounts":
        params.update({"params": {"with_portfolio": True}})

    if endpoint == "get_open_orders":
        end_point_params = dict(kind=parameters["kind"], type=parameters["type"])

        params.update({"params": end_point_params})

    return params


def id_numbering(
    operation: str,
    ws_channel: str,
) -> str:
    """

    id convention

    method
    subscription    3
    get             4

    auth
    public	        1
    private	        2

    instruments
    all             0
    btc             1
    eth             2

    subscription
    --------------  method      auth    seq    inst
    portfolio	        3	    1	    01
    user_order	        3	    1	    02
    my_trade	        3	    1	    03
    order_book	        3	    2	    04
    trade	            3	    1	    05
    index	            3	    1	    06
    announcement	    3	    1	    07

    get
    --------------
    currencies	        4	    2	    01
    instruments	        4	    2	    02
    positions	        4	    1	    03

    """
    id_auth = 1
    if "user" in ws_channel:
        id_auth = 9

    id_method = 0
    if "subscribe" in operation:
        id_method = 3
    if "get" in operation:
        id_method = 4
    id_item = 0
    if "book" in ws_channel:
        id_item = 1
    if "user" in ws_channel:
        id_item = 2
    if "chart" in ws_channel:
        id_item = 3
    if "index" in ws_channel:
        id_item = 4
    if "order" in ws_channel:
        id_item = 5
    if "position" in ws_channel:
        id_item = 6
    if "simulation" in ws_channel:
        id_item = 7
    id_instrument = 0
    if "BTC" or "btc" in ws_channel:
        id_instrument = 1
    if "ETH" or "eth" in ws_channel:
        id_instrument = 2
    return int(f"{id_auth}{id_method}{id_item}{id_instrument}")
