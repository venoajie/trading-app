# src\scripts\deribit\strategies\hedging\hedging_spot.py

# built ins
import asyncio
from secrets import randbelow

# installed
from dataclassy import dataclass, fields
from loguru import logger as log

from core.db import sqlite as db_mgt import update_status_data
from src.scripts.deribit.strategies.basic_strategy import (
    BasicStrategy,
    are_size_and_order_appropriate,
    ensure_sign_consistency,
    is_label_and_side_consistent,
    is_minimum_waiting_time_has_passed,
    size_rounding,
)

from src.scripts.deribit.restful_api import end_point_params_template,connector
from src.shared.utils import string_modification import parsing_label

# user defined formula


def get_transactions_len(result_strategy_label) -> int:
    """ """
    return 0 if result_strategy_label == [] else len([o for o in result_strategy_label])


def get_transactions_sum(result_strategy_label) -> int:
    """ """
    return (
        0
        if result_strategy_label == []
        else sum([o["amount"] for o in result_strategy_label])
    )


def get_label_integer(label: dict) -> bool:
    """ """

    return parsing_label(label)["int"]


def reading_from_pkl_data(end_point, currency, status: str = None) -> dict:
    """ """

    from utilities.pickling import read_data
    from utilities.system_tools import provide_path_for_file

    path: str = provide_path_for_file(end_point, currency, status)
    data = read_data(path)

    return data


def hedged_value_to_notional(notional: float, hedged_value: float) -> float:
    """ """
    return abs(hedged_value / notional)


def determine_opening_size(
    instrument_name: str,
    futures_instruments,
    side: str,
    max_position: float,
    factor: float,
) -> int:
    """ """
    sign = ensure_sign_consistency(side)

    proposed_size = max(1, int(abs(max_position) * factor))

    return size_rounding(instrument_name, futures_instruments, proposed_size) * sign


def get_waiting_time_factor(
    weighted_factor,
    strong_fluctuation: bool,
    some_fluctuation: bool,
) -> float:
    """
    Provide factor for size determination.
    """

    ONE_PCT = 1 / 100

    BEARISH_FACTOR = (
        weighted_factor["extreme"] * ONE_PCT
        if strong_fluctuation
        else weighted_factor["medium"] * ONE_PCT
    )

    return BEARISH_FACTOR if (strong_fluctuation or some_fluctuation) else ONE_PCT


def is_hedged_value_to_notional_exceed_threshold(
    notional: float, hedged_value: float, threshold: float
) -> float:
    """ """
    return hedged_value_to_notional(notional, hedged_value) > threshold


def max_order_stack_has_not_exceeded(len_orders: float, strong_market: float) -> bool:
    """ """
    if strong_market:
        max_order = True

    else:
        max_order = True if len_orders == 0 else False

    return max_order


def get_timing_factor(strong_bearish: bool, bearish: bool, threshold: float) -> float:
    """
    Determine order outstanding timing for size determination.
    strong bearish : 30% of normal interval
    bearish        : 6% of normal interval
    """

    ONE_PCT = 1 / 100

    bearish_interval_threshold = (
        (threshold * ONE_PCT * 30) if strong_bearish else (threshold * ONE_PCT * 60)
    )

    return (
        threshold * bearish_interval_threshold
        if (strong_bearish or bearish)
        else threshold
    )


def check_if_minimum_waiting_time_has_passed(
    strong_bullish: bool,
    bullish: bool,
    threshold: float,
    timestamp: int,
    server_time: int,
) -> bool:
    """ """

    cancel_allowed: bool = False

    time_interval = get_timing_factor(strong_bullish, bullish, threshold)

    minimum_waiting_time_has_passed: bool = is_minimum_waiting_time_has_passed(
        server_time, timestamp, threshold
    )

    # log.warning (f"minimum_waiting_time_has_passed {minimum_waiting_time_has_passed} time_interval {time_interval}")
    # log.warning (f"server_time {server_time} max_tstamp_orders {timestamp} threshold {threshold} { {server_time-timestamp} }")

    if minimum_waiting_time_has_passed:
        cancel_allowed: bool = True

    return cancel_allowed


def current_hedge_position_exceed_max_position(
    sum_my_trades_currency_str: int, max_position: float
) -> bool:
    """ """

    return abs(sum_my_trades_currency_str) > abs(max_position)


def net_size_of_label(my_trades_currency_strategy: list, transaction: dict) -> bool:
    """ """

    label_integer = get_label_integer(transaction["label"])

    return sum(
        [
            o["amount"]
            for o in my_trades_currency_strategy
            if str(label_integer) in o["label"]
        ]
    )


def net_size_not_over_bought(
    my_trades_currency_strategy: list, transaction: list
) -> bool:
    """ """
    return net_size_of_label(my_trades_currency_strategy, transaction) < 0


def size_multiply_factor(market_condition: dict) -> float:
    """ """

    strong_bearish = market_condition["strong_bearish"]

    bearish = market_condition["bearish"]
    weak_bullish = market_condition["weak_bullish"]
    weak_bearish = market_condition["weak_bearish"]
    bullish = market_condition["bullish"]
    strong_bullish = market_condition["strong_bullish"]

    multiply_factor = 1

    if bullish or strong_bullish:
        multiply_factor = 0
    else:
        if weak_bullish:
            multiply_factor = 0.8
        elif strong_bearish:
            multiply_factor = 1.5
        elif bearish:
            multiply_factor = 1.3
        elif weak_bearish:
            multiply_factor = 1.1

    return multiply_factor


def size_to_be_hedged(
    notional,
    sum_my_trades_currency_strategy,
    over_hedged_closing,
    market_condition: dict,
) -> float:
    """ """

    multiply = size_multiply_factor(market_condition)

    max_position = (
        (abs(notional) + sum_my_trades_currency_strategy) * -1
        if over_hedged_closing
        else notional
    )

    return multiply * max_position


async def modify_hedging_instrument(
    strong_bearish: bool,
    bearish: bool,
    instrument_attributes_futures_for_hedging: list,
    ticker_all: list,
    ticker_perpetual_instrument_name: dict,
    currency_upper: str,
) -> dict:

    if bearish or not strong_bearish:

        instrument_attributes_future = [
            o
            for o in instrument_attributes_futures_for_hedging
            if "PERPETUAL" not in o["instrument_name"]
            and currency_upper in o["instrument_name"]
        ]

        if len(instrument_attributes_future) > 1:
            index_attributes = randbelow(2)
            instrument_attributes = instrument_attributes_future[index_attributes]

        else:
            instrument_attributes = instrument_attributes_future[0]

        instrument_name: str = instrument_attributes["instrument_name"]

        instrument_ticker = [
            o for o in ticker_all if instrument_name in o["instrument_name"]
        ]

        # log.error (f"instrument_ticker {instrument_ticker}")
        if instrument_ticker:
            instrument_ticker = instrument_ticker[0]

        else:

            connection_url = end_point_params_template.basic_https()

            tickers_end_point = end_point_params_template.get_tickers_end_point(
                instrument_name
            )

            instrument_ticker = await connector.get_connected(
                connection_url, tickers_end_point
            )

        return instrument_ticker

    else:
        return ticker_perpetual_instrument_name


@dataclass(unsafe_hash=True, slots=True)
class HedgingSpot(BasicStrategy):
    """ """

    notional: float
    my_trades_currency_strategy: int
    market_condition: list
    index_price: float
    my_trades_currency_all: float
    sum_my_trades_currency_strategy: int = fields
    over_hedged_opening: bool = fields
    over_hedged_closing: bool = fields
    max_position: float = fields
    max_position: float = fields

    def __post_init__(self):

        self.sum_my_trades_currency_strategy = get_transactions_sum(
            self.my_trades_currency_strategy
        )
        self.over_hedged_opening = current_hedge_position_exceed_max_position(
            self.sum_my_trades_currency_strategy, self.notional
        )
        self.over_hedged_closing = self.sum_my_trades_currency_strategy > 0
        self.max_position = size_to_be_hedged(
            self.notional,
            self.sum_my_trades_currency_strategy,
            self.over_hedged_closing,
            self.market_condition,
        )

    #        log.info(
    #            f" max_position {self.max_position} over_hedged_opening {self.over_hedged_opening}  over_hedged_closing {self.over_hedged_closing} sum_my_trades_currency_strategy {self.sum_my_trades_currency_strategy}"
    #        )

    def get_basic_params(self) -> dict:
        """ """
        return BasicStrategy(self.strategy_label, self.strategy_parameters)

    async def understanding_the_market(self, threshold_market_condition) -> None:
        """ """

    async def risk_managament(self) -> None:
        """ """
        pass

    def opening_position(
        self,
        non_checked_strategies,
        instrument_name,
        futures_instruments,
        open_orders_label,
        params,
        SIZE_FACTOR,
        len_orders,
    ) -> bool:
        """ """

        order_allowed: bool = False

        max_position = self.max_position

        my_trades_currency_all_sum = get_transactions_sum(self.my_trades_currency_all)

        over_hendge_opening_all = my_trades_currency_all_sum < max_position

        log.debug(
            f"my_trades_currency_all_sum {my_trades_currency_all_sum} max_position {max_position} over_hendge_opening_all {over_hendge_opening_all}"
        )

        if len_orders == 0 and not over_hendge_opening_all:

            over_hedged_cls = self.over_hedged_closing

            size = determine_opening_size(
                instrument_name,
                futures_instruments,
                params["side"],
                self.max_position,
                SIZE_FACTOR,
            )

            sum_orders: int = get_transactions_sum(open_orders_label)

            size_and_order_appropriate_for_ordering: bool = (
                are_size_and_order_appropriate(
                    "add_position",
                    self.sum_my_trades_currency_strategy,
                    sum_orders,
                    size,
                    max_position,
                )
            )

            order_allowed: bool = (
                size_and_order_appropriate_for_ordering or over_hedged_cls
            )

            log.info(f"order_allowed {order_allowed} ")
            log.info(
                f" size_and_order_appropriate_for_ordering {size_and_order_appropriate_for_ordering} { (size_and_order_appropriate_for_ordering or over_hedged_cls)}"
            )

            if order_allowed:

                label_and_side_consistent = is_label_and_side_consistent(
                    non_checked_strategies, params
                )
                log.info(f"label_and_side_consistent {label_and_side_consistent} ")

                if label_and_side_consistent:  # and not order_has_sent_before:

                    params.update({"instrument_name": instrument_name})
                    params.update({"size": abs(size)})
                    params.update(
                        {"is_label_and_side_consistent": label_and_side_consistent}
                    )

                else:

                    order_allowed = False

        return order_allowed

    def closing_position(
        self,
        transaction: dict,
        exit_params: dict,
        bullish: bool,
        strong_bullish: bool,
        weak_bullish: bool,
        bid_price: float,
    ) -> bool:
        """ """

        order_allowed: bool = False

        # bid_price_is_lower = bid_price < transaction ["price"]
        over_hedged_closing = self.over_hedged_closing
        over_hedged_opening = self.over_hedged_opening

        if over_hedged_closing:

            order_allowed: bool = False

        else:

            size = exit_params["size"]

            log.debug(f"transaction {transaction}")
            log.warning(
                f"size != 0 {size} (bullish or strong_bullish) {(weak_bullish or bullish or strong_bullish)}"
            )

            if size != 0 and over_hedged_opening:

                if weak_bullish or bullish or strong_bullish:

                    exit_params.update({"entry_price": bid_price})

                    # convert size to positive sign
                    exit_params.update({"size": abs(size)})

                    order_allowed: bool = True

        log.warning(f"order_allowed {order_allowed} ")

        return order_allowed

    async def cancelling_orders(
        self,
        transaction: dict,
        orders_currency_strategy: list,
        server_time: int,
        strategy_params: list = None,
    ) -> bool:
        """ """

        cancel_allowed: bool = False

        ONE_SECOND = 1000
        ONE_MINUTE = ONE_SECOND * 60

        if strategy_params is None:
            hedging_attributes: dict = self.strategy_parameters
        else:
            hedging_attributes: dict = strategy_params

        waiting_minute_before_cancel = (
            hedging_attributes["waiting_minute_before_cancel"] * ONE_MINUTE
        )

        market_condition: dict = self.market_condition

        bullish, strong_bullish = (
            market_condition["bullish"],
            market_condition["strong_bullish"],
        )

        bearish, strong_bearish = (
            market_condition["bearish"],
            market_condition["strong_bearish"],
        )
        # neutral = market_condition["neutral_price"]

        try:
            timestamp: int = transaction["timestamp"]

        except:
            timestamp: int = transaction["last_update_timestamp"]

        if timestamp:

            if "open" in transaction["label"]:

                open_size_not_over_bought = self.over_hedged_opening

                open_orders_label: list = [
                    o for o in orders_currency_strategy if "open" in o["label"]
                ]

                len_open_orders: int = get_transactions_len(open_orders_label)

                log.info(
                    f"open_size_not_over_bought {open_size_not_over_bought} len_open_orders {len_open_orders}"
                )

                if not open_size_not_over_bought:

                    # Only one open order a time
                    if len_open_orders > 1:

                        cancel_allowed = True

                    else:

                        cancel_allowed: bool = check_if_minimum_waiting_time_has_passed(
                            strong_bearish,
                            bearish,
                            waiting_minute_before_cancel,
                            timestamp,
                            server_time,
                        )
                        log.info(f"cancel_allowed {cancel_allowed}")

                # cancel any orders when overhedged
                else:

                    if len_open_orders > 0:

                        cancel_allowed = True

            if "closed" in transaction["label"]:

                exit_size_not_over_bought = net_size_not_over_bought(
                    self.my_trades_currency_strategy, transaction
                )

                log.info(f"exit_size_not_over_bought {exit_size_not_over_bought}")
                if exit_size_not_over_bought:

                    closed_orders_label: list = [
                        o for o in orders_currency_strategy if "closed" in (o["label"])
                    ]

                    len_closed_orders: int = get_transactions_len(closed_orders_label)

                    log.info(f" len_closed_orders {len_closed_orders}")

                    if len_closed_orders > 1:

                        cancel_allowed = True

                    else:

                        cancel_allowed: bool = check_if_minimum_waiting_time_has_passed(
                            strong_bullish,
                            bullish,
                            waiting_minute_before_cancel,
                            timestamp,
                            server_time,
                        )
                        log.info(f" cancel_allowed {cancel_allowed}")

                # cancel any orders when overhedged
                else:

                    cancel_allowed = True

        return cancel_allowed

    async def modifying_order(self) -> None:
        """ """
        pass

    async def is_cancelling_orders_allowed(
        self,
        selected_transaction: list,
        orders_currency_strategy: list,
        server_time: int,
    ) -> dict:
        """ """

        cancel_allowed, cancel_id = False, None

        cancel_allowed = await self.cancelling_orders(
            selected_transaction, orders_currency_strategy, server_time
        )

        if cancel_allowed:
            cancel_id = selected_transaction["order_id"]

        return dict(cancel_allowed=cancel_allowed, cancel_id=cancel_id)

    async def is_send_open_order_allowed(
        self,
        non_checked_strategies,
        instrument_name: str,
        futures_instruments,
        orders_currency_strategy: list,
        ask_price: float,
        archive_db_table: str,
        trade_db_table: str,
    ) -> dict:
        """ """

        order_allowed = False

        open_orders_label: list = [
            o for o in orders_currency_strategy if "open" in o["label"]
        ]

        len_open_orders: int = get_transactions_len(open_orders_label)

        hedging_attributes = self.strategy_parameters

        market_condition = self.market_condition

        # bullish = market_condition["rising_price"]
        bearish = market_condition["bearish"]

        # strong_bullish = market_condition["strong_rising_price"]
        strong_bearish = market_condition["strong_bearish"]
        #        neutral = market_condition["neutral"]
        params: dict = self.get_basic_params().get_basic_opening_parameters(ask_price)

        weighted_factor = hedging_attributes["weighted_factor"]

        SIZE_FACTOR = get_waiting_time_factor(weighted_factor, strong_bearish, bearish)

        order_allowed: bool = self.opening_position(
            non_checked_strategies,
            instrument_name,
            futures_instruments,
            open_orders_label,
            params,
            SIZE_FACTOR,
            len_open_orders,
        )

        # additional test for possibilities of orphaned closed orders was existed
        if not order_allowed:

            if len_open_orders == 0:

                # orphaned closed orders: have closed only
                my_trades_orphan = [
                    o
                    for o in self.my_trades_currency_strategy
                    if "closed" in o["label"] and "open" not in (o["label"])
                ]

                if my_trades_orphan:

                    max_timestamp = max([o["timestamp"] for o in my_trades_orphan])

                    transaction = [
                        o for o in my_trades_orphan if max_timestamp == o["timestamp"]
                    ][0]

                    label_integer = get_label_integer(transaction["label"])

                    new_label = f"futureSpread-open-{label_integer}"

                    filter = "label"

                    await update_status_data(
                        archive_db_table,
                        "label",
                        filter,
                        transaction["label"],
                        new_label,
                        "=",
                    )

                    await update_status_data(
                        trade_db_table,
                        "label",
                        filter,
                        transaction["label"],
                        new_label,
                        "=",
                    )

        return dict(
            order_allowed=order_allowed and len_open_orders == 0,
            order_parameters=[] if not order_allowed else params,
        )

    async def is_send_exit_order_allowed(
        self,
        orders_currency_strategy_label_closed,
        bid_price: float,
        selected_transaction: list,
        # orders_currency_strategy: list
    ) -> dict:
        """ """
        order_allowed: bool = False

        transaction: dict = selected_transaction[0]

        exit_size_not_over_bought = net_size_not_over_bought(
            self.my_trades_currency_strategy, transaction
        )

        label_integer: int = get_label_integer(transaction["label"])

        closed_orders_int = [
            o
            for o in orders_currency_strategy_label_closed
            if str(label_integer) in o["label"]
        ]

        len_closed_orders_int = get_transactions_len(closed_orders_int)

        log.warning(f" len_closed_orders_int == 0 {len_closed_orders_int == 0}")
        log.error(f" closed_orders_int {closed_orders_int}")
        log.debug(f" label_integer {label_integer}")
        log.info(
            f" orders_currency_strategy_label_closed {orders_currency_strategy_label_closed}"
        )

        if len_closed_orders_int == 0 and exit_size_not_over_bought:

            market_condition = self.market_condition

            bullish, strong_bullish, weak_bullish = (
                market_condition["bullish"],
                market_condition["strong_bullish"],
                market_condition["weak_bullish"],
            )

            exit_params: dict = self.get_basic_params().get_basic_closing_paramaters(
                selected_transaction,
                orders_currency_strategy_label_closed,
            )

            log.warning(
                f"sum_my_trades_currency_strategy {self.sum_my_trades_currency_strategy} "
            )

            order_allowed = self.closing_position(
                transaction,
                exit_params,
                bullish,
                strong_bullish,
                weak_bullish,
                bid_price,
            )

            # default type: limit
            exit_params.update({"type": "limit"})

        return dict(
            order_allowed=order_allowed,
            order_parameters=([] if not order_allowed else exit_params),
        )

    async def send_contra_order_for_orphaned_closed_transctions(
        self,
        orders_currency_strategy_label_open,
        ask_price: float,
        selected_transaction: list,
    ) -> dict:
        """ """
        order_allowed: bool = False

        transaction: dict = selected_transaction[0]

        exit_size_not_over_bought = net_size_not_over_bought(
            self.my_trades_currency_strategy, transaction
        )

        label_integer: int = get_label_integer(transaction["label"])

        open_orders_int = [
            o
            for o in orders_currency_strategy_label_open
            if str(label_integer) in o["label"]
        ]

        len_open_orders_int = get_transactions_len(open_orders_int)

        log.warning(f" len_closed_orders_int == 0 {len_open_orders_int == 0}")

        if len_open_orders_int == 0 and exit_size_not_over_bought:

            exit_params: dict = self.get_basic_params().get_basic_closing_paramaters(
                selected_transaction,
                orders_currency_strategy_label_open,
                "contra",
            )

            log.warning(
                f"sum_my_trades_currency_strategy {self.sum_my_trades_currency_strategy} "
            )

            size = exit_params["size"]

            ask_price_is_higher = ask_price > transaction["price"]

            if size != 0 and ask_price_is_higher:

                exit_params.update({"entry_price": ask_price})

                # convert size to positive sign
                exit_params.update({"size": abs(size)})

                order_allowed: bool = True

            # default type: limit
            exit_params.update({"type": "limit"})

        return dict(
            order_allowed=order_allowed,
            order_parameters=([] if not order_allowed else exit_params),
        )
