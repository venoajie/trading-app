# # -*- coding: utf-8 -*-

# built ins
import asyncio

# installed
from dataclassy import dataclass, fields
from loguru import logger as log

from streaming_helper.strategies.deribit.basic_strategy import BasicStrategy

# user defined formula


def get_delta(my_trades_currency_strategy) -> int:
    """ """

    return (
        0
        if my_trades_currency_strategy == []
        else sum([o["amount"] for o in my_trades_currency_strategy])
    )


@dataclass(unsafe_hash=True, slots=True)
class FutureSpread(BasicStrategy):
    """ """

    orders_currency_strategy: list
    server_time: int
    my_trades_currency_strategy: list = None
    ticker_perpetual: dict = None
    delta: float = fields
    basic_params: object = fields

    def __post_init__(self):

        self.delta: float = get_delta(self.my_trades_currency_strategy)
        self.basic_params: str = BasicStrategy(
            self.strategy_label, self.strategy_parameters
        )

        log.critical(f"""delta  {self.delta} """)
