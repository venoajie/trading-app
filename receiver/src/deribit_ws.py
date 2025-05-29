# built ins
import asyncio
import json
from datetime import datetime, timedelta, timezone

# installed
import orjson
import uvloop
import websockets
from dataclassy import dataclass, fields

# user defined formula
from restful_api.deribit import end_point_params_template
from shared import error_handling, string_modification as str_mod


@dataclass(unsafe_hash=True, slots=True)
class StreamingAccountData:
    """ """

    sub_account_id: str
    client_id: str
    client_secret: str
    # Async Event Loop
    loop = asyncio.get_event_loop()
    ws_connection_url: str = "wss://www.deribit.com/ws/api/v2"
    # Instance Variables
    websocket_client: websockets.WebSocketClientProtocol = None
    refresh_token: str = None
    refresh_token_expiry_time: int = None

    async def ws_manager(
        self,
        client_redis,
        exchange,
        queue_general: object,
        futures_instruments,
        resolutions: list,
    ) -> None:

        async with websockets.connect(
            self.ws_connection_url,
            ping_interval=None,
            compression=None,
            close_timeout=60,
        ) as self.websocket_client:

            try:

                instruments_name = futures_instruments["instruments_name"]

                while True:

                    # Authenticate WebSocket Connection
                    await self.ws_auth(client_redis)

                    # Establish Heartbeat
                    await self.establish_heartbeat(client_redis)

                    # Start Authentication Refresh Task
                    self.loop.create_task(self.ws_refresh_auth())

                    ws_instruments = []

                    instrument_kinds = ["future, future_combo"]

                    for kind in instrument_kinds:

                        user_changes = f"user.changes.{kind}.any.raw"
                        ws_instruments.append(user_changes)

                    orders = f"user.orders.any.any.raw"
                    ws_instruments.append(orders)
                    trades = f"user.trades.any.any.raw"
                    ws_instruments.append(trades)

                    for instrument in instruments_name:

                        if "PERPETUAL" in instrument:

                            currency = str_mod.extract_currency_from_text(instrument)
                            portfolio = f"user.portfolio.{currency}"

                            ws_instruments.append(portfolio)

                            for resolution in resolutions:

                                ws_chart = f"chart.trades.{instrument}.{resolution}"
                                ws_instruments.append(ws_chart)

                        incremental_ticker = f"incremental_ticker.{instrument}"

                        ws_instruments.append(incremental_ticker)

                    await self.ws_operation(
                        operation="subscribe",
                        ws_channel=ws_instruments,
                        source="ws-combination",
                    )

                    while True:

                        # Receive WebSocket messages
                        message: bytes = await self.websocket_client.recv()
                        message: dict = orjson.loads(message)

                        if "id" in list(message):
                            if message["id"] == 9929:

                                if self.refresh_token is None:
                                    print(
                                        "Successfully authenticated WebSocket Connection"
                                    )

                                else:
                                    print(
                                        "Successfully refreshed the authentication of the WebSocket Connection"
                                    )

                                self.refresh_token = message["result"]["refresh_token"]

                                # Refresh Authentication well before the required datetime
                                if message["testnet"]:
                                    expires_in: int = 300
                                else:
                                    expires_in: int = (
                                        message["result"]["expires_in"] - 240
                                    )

                                now_utc: int = datetime.now(timezone.utc)

                                self.refresh_token_expiry_time = now_utc + timedelta(
                                    seconds=expires_in
                                )

                            elif message["id"] == 8212:
                                # Avoid logging Heartbeat messages
                                continue

                        elif "method" in list(message):
                            # Respond to Heartbeat Message
                            if message["method"] == "heartbeat":
                                await self.heartbeat_response(client_redis)

                        if "params" in list(message):

                            if message["method"] != "heartbeat":

                                message_params: dict = message["params"]

                                if message_params:

                                    message_params.update({"exchange": exchange})
                                    message_params.update(
                                        {"account_id": self.sub_account_id}
                                    )

                                    # queing message to dispatcher
                                    await queue_general.put(message_params)

                                """
                                    message examples:
                                    
                                    incremental_ticker = {
                                        'channel': 'incremental_ticker.BTC-7FEB25', 
                                        'data': {
                                            'timestamp': 1738407481107, 
                                            'type': 'snapshot', 
                                            'state': 'open',
                                            'stats': {
                                                'high': 106245.0, 
                                                'low': 101550.0, 
                                                'price_change': -2.6516, 
                                                'volume': 107.12364526, 
                                                'volume_usd': 11081110.0, 
                                                'volume_notional': 11081110.0
                                                }, 
                                                'index_price': 101645.32,
                                                'instrument_name': 'BTC-7FEB25', 
                                                'last_price': 101787.5, 
                                                'settlement_price': 102285.5, 
                                                'min_price': 100252.5,
                                                'max_price': 103310.0,
                                                'open_interest': 18836380, 
                                                'mark_price': 101781.52,
                                                'best_ask_price': 101780.0, 
                                                'best_bid_price': 101775.0,
                                                'estimated_delivery_price': 101645.32,
                                                'best_ask_amount': 15500.0, 
                                                'best_bid_amount': 11310.0
                                                }
                                                }
                                            
                                    chart.trades = {
                                        'channel': 'chart.trades.BTC-PERPETUAL.1', 
                                        'data': {
                                            'close': 101650.5, 
                                            'high': 101650.5, 
                                            'low': 101650.5, 
                                            'open': 101650.5, 
                                            'tick': 1738407480000, 
                                            'cost': 0.0, 
                                            'volume': 0.0}
                                            }
                                            
                                    portfolio = {
                                        'channel': 'user.portfolio.btc', 
                                        'data': {
                                            'options_pl': 0.0, 
                                            'balance': 0.00214241, 
                                            'session_rpl': 0.0,
                                            'initial_margin': 0.00075353, 
                                            'additional_reserve': 0.0, 
                                            'spot_reserve': 0.0, 
                                            'futures_pl': -1.442e-05,
                                            'total_delta_total_usd': 193.408186282, 
                                            'total_maintenance_margin_usd': 53.465166986729, 
                                            'options_theta_map': {}, 
                                            'projected_delta_total': 0.002373, 
                                            'options_gamma': 0.0, 
                                            'total_pl': -1.442e-05, 
                                            'options_gamma_map': {},
                                            'projected_initial_margin': 0.00075353, 
                                            'options_session_rpl': 0.0,
                                            'options_session_upl': 0.0, 
                                            'total_margin_balance_usd': 273.683871785, 
                                            'equity': 0.00213253, 
                                            'cross_collateral_enabled': True, 
                                            'delta_total': 0.002373, 
                                            'delta_total_map': {
                                                'btc_usd': 0.002373282}, 
                                                'options_value': 0.0, 
                                                'total_equity_usd': 273.683871785,
                                                'locked_balance': 0.0, 
                                                'margin_balance': 0.00269254, 
                                                'available_withdrawal_funds': 0.00203902,
                                                'options_delta': 0.0,
                                                'currency': 'BTC', 
                                                'fee_balance': 0.0,
                                                'options_vega_map': {}, 
                                                'available_funds': 0.00193902, 
                                                'maintenance_margin': 0.000526, 
                                                'futures_session_rpl': 0.0, 
                                                'total_initial_margin_usd': 76.592212527, 
                                                'session_upl': -9.88e-06, 
                                                'options_vega': 0.0, 
                                                'options_theta': 0.0,
                                                'futures_session_upl': -9.88e-06,
                                                'portfolio_margining_enabled': True, 
                                                'projected_maintenance_margin': 0.000526, 
                                                'margin_model': 'cross_pm'
                                                }
                                                }
                                    """

            except Exception as error:

                await error_handling.parse_error_message_with_redis(
                    client_redis,
                    error,
                )

    async def establish_heartbeat(
        self,
        client_redis,
    ) -> None:
        """
        reference: https://github.com/ElliotP123/crypto-exchange-code-samples/blob/master/deribit/websockets/dbt-ws-authenticated-example.py

        Requests DBT's `public/set_heartbeat` to
        establish a heartbeat connection.
        """
        msg: dict = {
            "jsonrpc": "2.0",
            "id": 9098,
            "method": "public/set_heartbeat",
            "params": {"interval": 10},
        }

        try:
            await self.websocket_client.send(json.dumps(msg))

        except Exception as error:

            await error_handling.parse_error_message_with_redis(
                client_redis,
                error,
            )

    async def heartbeat_response(
        self,
        client_redis,
    ) -> None:
        """
        Sends the required WebSocket response to
        the Deribit API Heartbeat message.
        """
        msg: dict = {
            "jsonrpc": "2.0",
            "id": 8212,
            "method": "public/test",
            "params": {},
        }

        try:

            await self.websocket_client.send(json.dumps(msg))

        except Exception as error:

            await error_handling.parse_error_message_with_redis(
                client_redis,
                error,
            )

    async def ws_auth(
        self,
        client_redis,
    ) -> None:
        """
        Requests DBT's `public/auth` to
        authenticate the WebSocket Connection.
        """
        msg: dict = {
            "jsonrpc": "2.0",
            "id": 9929,
            "method": "public/auth",
            "params": {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
        }

        try:
            await self.websocket_client.send(json.dumps(msg))

        except Exception as error:

            await error_handling.parse_error_message_with_redis(
                client_redis,
                error,
            )

    async def ws_refresh_auth(self) -> None:
        """
        Requests DBT's `public/auth` to refresh
        the WebSocket Connection's authentication.
        """
        while True:

            now_utc = datetime.now(timezone.utc)

            if self.refresh_token_expiry_time is not None:

                if now_utc > self.refresh_token_expiry_time:

                    msg: dict = {
                        "jsonrpc": "2.0",
                        "id": 9929,
                        "method": "public/auth",
                        "params": {
                            "grant_type": "refresh_token",
                            "refresh_token": self.refresh_token,
                        },
                    }

                    await self.websocket_client.send(json.dumps(msg))

            await asyncio.sleep(150)

    async def ws_operation(
        self,
        operation: str,
        ws_channel: str,
        source: str = "ws-single",
    ) -> None:
        """
        Requests `public/subscribe` or `public/unsubscribe`
        to DBT's API for the specific WebSocket Channel.

        source:
        ws-single
        ws-combination
        rest
        """
        sleep_time: int = 0.05

        await asyncio.sleep(sleep_time)

        id = end_point_params_template.id_numbering(
            operation,
            ws_channel,
        )

        msg: dict = {
            "jsonrpc": "2.0",
        }

        if "ws" in source:

            if "single" in source:
                ws_channel = [ws_channel]

            extra_params: dict = dict(
                id=id,
                method=f"private/{operation}",
                params={"channels": ws_channel},
            )

            msg.update(extra_params)

            if msg["params"]["channels"]:
                await self.websocket_client.send(json.dumps(msg))

        if "rest_api" in source:

            extra_params: dict = await end_point_params_template.get_api_end_point(
                operation,
                ws_channel,
            )

            msg.update(extra_params)

            await self.websocket_client.send(json.dumps(msg))
