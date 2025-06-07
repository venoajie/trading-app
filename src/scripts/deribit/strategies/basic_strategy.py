# # -*- coding: utf-8 -*-

# built ins
import asyncio

# installed
from dataclassy import dataclass
from loguru import logger as log

# user defined formula
from src.shared.utils import error_handling, string_modification as str_mod

def positions_and_orders(
    current_size: int,
    current_orders_size: int,
) -> int:
    """ """

    return current_size + current_orders_size


def ensure_sign_consistency(side) -> float:
    """ """
    return -1 if side == "sell" else 1


def proforma_size(
    current_size: int,
    current_orders_size: int,
    next_orders_size: int,
) -> int:
    """ """

    return (
        positions_and_orders(current_size, current_orders_size)
        + next_orders_size  # the sign is +
    )


def are_size_and_order_appropriate(
    purpose: str,
    current_size_or_open_position: float,
    current_orders_size: int,
    next_orders_size: int,
    max_position: float = None,
) -> bool:
    """
    purpose: add_position/reduce_position
    for reduce: open position/individual trade
    for add: current position
    """

    proforma = proforma_size(
        current_size_or_open_position, current_orders_size, next_orders_size
    )

    ordering_is_ok = False

    if purpose == "add_position":

        if max_position < 0:
            ordering_is_ok = (proforma) > (max_position)

        if max_position > 0:
            ordering_is_ok = (proforma) < (max_position)

    if purpose == "reduce_position":

        if current_size_or_open_position < 0:
            ordering_is_ok = proforma > current_size_or_open_position

        if current_size_or_open_position > 0:
            ordering_is_ok = proforma < current_size_or_open_position

    log.debug(
        f"ordering_is_ok  {ordering_is_ok} current_size_or_open_position \
        {current_size_or_open_position} proforma  {proforma} max_position  \
            {max_position} current_orders_size  {current_orders_size} next_orders_size  {next_orders_size} "
    )

    return ordering_is_ok


def check_if_next_closing_size_will_not_exceed_the_original(
    basic_size: int,
    net_size: int,
    next_size: int,
) -> bool:
    """ """

    basic_size_higher_than_next_closing_size = abs(basic_size) >= abs(next_size)
    basic_size_higher_than_net_size = abs(basic_size) >= abs(net_size)
    basic_size_plus_next_size = (next_size) + (basic_size)

    if abs(net_size) != abs(basic_size):
        pass

    if basic_size > 0:
        basic_size_sign_diff_than_next_size = basic_size_plus_next_size < basic_size

    if basic_size < 0:
        basic_size_sign_diff_than_next_size = basic_size_plus_next_size > basic_size

    # log.debug (f"basic_size_higher_than_next_closing_size {basic_size_higher_than_next_closing_size} basic_size_higher_than_net_size {basic_size_higher_than_net_size} basic_size_plus_next_size {basic_size_plus_next_size} basic_size_sign_diff_than_next_size {basic_size_sign_diff_than_next_size}")

    return (
        basic_size_higher_than_next_closing_size
        and basic_size_higher_than_net_size
        and basic_size_sign_diff_than_next_size
    )  # and net_size_exceeding_the_basic_size


def provide_size_to_close_transaction(
    basic_size: int,
    net_size: int,
) -> int:
    """ """

    next_size = min(basic_size, net_size)

    return abs(next_size)


def size_rounding(
    instrument_name: str,
    futures_instruments,
    proposed_size: float,
) -> int:
    """ """

    min_trade_amount = [
        o["min_trade_amount"]
        for o in futures_instruments
        if o["instrument_name"] == instrument_name
    ][0]

    rounded_size = round(proposed_size / min_trade_amount) * min_trade_amount

    return max(min_trade_amount, rounded_size)  # size is never 0


def delta(
    last_price: float,
    prev_price: float,
) -> float:
    """ """
    return last_price - prev_price


def delta_pct(
    last_price: float,
    prev_price: float,
) -> float:
    """ """
    return abs(delta(last_price, prev_price) / prev_price)


def get_label(
    status: str,
    label_main_or_label_transactions: str,
) -> str:
    """
    provide transaction label
    """

    if status == "open":

        # get open label
        label = str_mod.labelling("open", label_main_or_label_transactions)

    else:

        # parsing label id
        label_id: int = str_mod.parsing_label(label_main_or_label_transactions)["int"]

        # parsing label strategy
        label_main: str = str_mod.parsing_label(label_main_or_label_transactions)[
            "main"
        ]

        if status == "contra":

            if "closed" in label_main_or_label_transactions:
                label = f"""{label_main}-open-{label_id}"""

            if "open" in label_main_or_label_transactions:
                label = f"""{label_main}-closed-{label_id}"""

        if status == "closed":

            # combine id + label strategy
            label: str = f"""{label_main}-closed-{label_id}"""

    return label


def compute_profit_usd(
    transaction_price: float,
    currrent_price: float,
    size: int,
    side: str,
) -> float:
    """
    get difference between now and transaction time
    """
    if side == "buy":
        delta = currrent_price - transaction_price

    if side == "sell":
        delta = transaction_price - currrent_price

    profit = delta / transaction_price * abs(size)

    return profit


def pct_price_in_usd(
    price: float,
    pct_threshold: float,
) -> float:

    return price * pct_threshold


def price_plus_pct(
    price: float,
    pct_threshold: float,
) -> float:

    return price + pct_price_in_usd(
        price,
        pct_threshold,
    )


def price_minus_pct(
    price: float,
    pct_threshold: float,
) -> float:

    return price - pct_price_in_usd(
        price,
        pct_threshold,
    )


def is_transaction_price_minus_below_threshold(
    last_transaction_price: float,
    current_price: float,
    pct_threshold: float,
) -> bool:

    return price_minus_pct(last_transaction_price, pct_threshold) >= current_price


def is_transaction_price_plus_above_threshold(
    last_transaction_price: float,
    current_price: float,
    pct_threshold: float,
) -> bool:

    return (
        price_plus_pct(
            last_transaction_price,
            pct_threshold,
        )
        < current_price
    )


def profit_usd_has_exceed_target(
    target_profit: float,
    transaction_price: float,
    currrent_price: float,
    size: int,
    side: str,
) -> bool:
    """
    get difference between now and transaction time
    """
    current_profit = compute_profit_usd(
        transaction_price,
        currrent_price,
        size,
        side,
    )

    target = abs(pct_price_in_usd(target_profit, size))

    return current_profit > 0 and current_profit > target


def delta_time(
    server_time: int,
    time_stamp: int,
) -> int:
    """
    get difference between now and transaction time
    """

    return server_time - time_stamp


def is_minimum_waiting_time_has_passed(
    server_time: int,
    time_stamp: int,
    time_threshold: float,
) -> bool:
    """
    check whether delta time has exceed time threhold
    """

    return (
        True
        if time_stamp == []
        else delta_time(server_time, time_stamp) > time_threshold
    )


def get_max_time_stamp(result_strategy_label: list) -> int:
    """ """
    return (
        []
        if result_strategy_label == []
        else max([o["timestamp"] for o in result_strategy_label])
    )


def get_order_id_max_time_stamp(result_strategy_label: list) -> int:
    """ """
    return (
        0
        if get_max_time_stamp(result_strategy_label) == []
        else [
            o["order_id"]
            for o in result_strategy_label
            if o["timestamp"] == get_max_time_stamp(result_strategy_label)
        ][0]
    )


def get_transactions_len(result_strategy_label: list) -> int:
    """ """
    return 0 if result_strategy_label == [] else len([o for o in result_strategy_label])


def get_transactions_sum(result_strategy_label: list) -> int:
    """
    summing transaction under SAME strategy label
    """
    return (
        0
        if result_strategy_label == []
        else sum([o["amount"] for o in result_strategy_label])
    )


def get_transaction_side(transaction: dict) -> str:
    """ """

    try:
        return transaction["direction"]

    except:
        return transaction["side"]


def get_transaction_size(transaction: dict) -> int:
    """ """
    return transaction["amount"]


def get_transaction_instrument(transaction: dict) -> int:
    """ """
    return transaction["instrument_name"]


def get_transaction_label(transaction: dict) -> str:
    """ """
    return transaction["label"]


def get_transaction_price(transaction: dict) -> float:
    """ """
    return transaction["price"]


def get_label_integer(label: dict) -> bool:
    """ """

    return str_mod.parsing_label(label)["int"]


def get_order_label(data_from_db: list) -> list:
    """ """

    return [o["label"] for o in data_from_db]


def get_label_super_main(result: list, strategy_label: str) -> list:
    """ """

    return [
        o
        for o in result
        if str_mod.parsing_label(strategy_label)["super_main"]
        == str_mod.parsing_label(o["label"])["super_main"]
    ]


def combine_vars_to_get_future_spread_label(timestamp: int) -> str:
    """ """

    return f"futureSpread-open-{timestamp}"


def check_if_id_has_used_before(
    combined_result: str, id_checked: str, transaction_id: str
) -> bool:
    """
    id_checked: order_id, trade_id, label

    verifier: order_id or label?
    - order_id only one per order
    - one label could be processed couple of time (especially when closing the transactions)
    """

    id = f"{id_checked}"
    # log.error (f"id {id}")
    if combined_result != []:
        result_order_id = [o[id] for o in combined_result]

    label_is_exist: list = (
        False
        if (combined_result == [] or result_order_id == [])
        else False if transaction_id not in result_order_id else True
    )

    return label_is_exist


def provide_side_to_close_transaction(transaction: dict) -> str:
    """ """

    # determine side
    transaction_side = get_transaction_side(transaction)

    if transaction_side == "sell":
        side = "buy"

    if transaction_side == "buy":
        side = "sell"

    return side


def sum_order_under_closed_label_int(
    closed_orders_label_strategy: list, label_integer_open: int
) -> int:
    """ """

    if closed_orders_label_strategy:
        order_under_closed_label_int = [
            o for o in closed_orders_label_strategy if label_integer_open in o["label"]
        ]

    return (
        0
        if (closed_orders_label_strategy == [] or order_under_closed_label_int == [])
        else sum([o["amount"] for o in order_under_closed_label_int])
    )


def convert_list_to_dict(transaction: list) -> dict:

    # convert list to dict
    try:
        transaction = transaction[0]
    except:
        return transaction

    return transaction


def is_label_and_side_consistent(
    non_checked_strategies,
    params,
    ) -> bool:
    """ """

    # log.error (f"params {params}")
    label = get_transaction_label(params)

    is_consistent = True if "closed" in label else False
    # log.warning(f"params {params}")

    if bool([o for o in non_checked_strategies if (o in label)]):
        is_consistent = True

    else:

        if "open" in label:

            side = get_transaction_side(params)

            if side == "sell":

                is_consistent = (
                    True
                    if ("Short" in label or "hedging" in label or "custom" in label)
                    else False
                )

                log.warning(f"is_consistent {is_consistent}")

            if side == "buy":
                is_consistent = True if "Long" in label else False

    return is_consistent


def get_take_profit_pct(transaction: dict, strategy_config: dict) -> float:
    """ """

    try:
        tp_pct: float = transaction["profit_target_pct_transaction"]
    except:
        tp_pct: float = strategy_config["take_profit_pct"]

    return tp_pct


def reading_from_db(end_point, instrument: str = None, status: str = None) -> list:
    """ """
    from utilities import pickling, system_tools

    return pickling.read_data(
        system_tools.provide_path_for_file(end_point, instrument, status)
    )


def get_non_label_from_transaction(transactions) -> list:
    """ """

    return [] if transactions == [] else [o for o in transactions if o["label"] == ""]


def check_db_consistencies(
    instrument_name: str,
    trades_from_sqlite: list,
    positions_from_sub_account: list,
    order_from_sqlite_open: list,
    open_orders_from_sub_accounts: list,
) -> bool:
    """ """

    no_non_label_from_from_sqlite_open = (
        False if get_non_label_from_transaction(order_from_sqlite_open) != [] else True
    )

    len_from_sqlite_open = len(order_from_sqlite_open)

    len_open_orders_from_sub_accounts = len(open_orders_from_sub_accounts)
    #
    sum_my_trades_sqlite = (
        0
        if trades_from_sqlite == []
        else sum([o["amount"] for o in trades_from_sqlite])
    )

    size_from_position: int = (
        0
        if positions_from_sub_account == []
        else sum(
            [
                o["size"]
                for o in positions_from_sub_account
                if o["instrument_name"] == instrument_name
            ]
        )
    )

    log.error(
        f"size_is_consistent {sum_my_trades_sqlite == size_from_position} sum_my_trades_sqlite {sum_my_trades_sqlite} size_from_positions {size_from_position} "
    )
    return dict(
        trade_size_is_consistent=sum_my_trades_sqlite == size_from_position,
        order_is_consistent=(
            len_open_orders_from_sub_accounts == len_from_sqlite_open
            and no_non_label_from_from_sqlite_open
        ),
        no_non_label_from_from_sqlite_open=(
            False
            if get_non_label_from_transaction(order_from_sqlite_open) != []
            else True
        ),
    )


def get_basic_closing_paramaters(
    selected_transaction: list,
    closed_orders_label_strategy: list,
    closed_label_status: str = None,
) -> dict:
    """ """

    transaction: dict = convert_list_to_dict(selected_transaction)

    # provide dict placeholder for params
    params = {}

    # determine side
    side = provide_side_to_close_transaction(transaction)
    params.update({"side": side})

    basic_size = get_transaction_size(transaction)

    label_transaction = transaction["label"]
    label_integer_open = get_label_integer(label_transaction)

    sum_order_under_closed_label = sum_order_under_closed_label_int(
        closed_orders_label_strategy, label_integer_open
    )

    net_size = basic_size + sum_order_under_closed_label
    size_abs = provide_size_to_close_transaction(basic_size, net_size)

    size = size_abs * ensure_sign_consistency(side)

    closing_size_ok = check_if_next_closing_size_will_not_exceed_the_original(
        basic_size, net_size, size
    )

    log.debug(
        f"sum_order_under_closed_label {sum_order_under_closed_label} label_integer_open {label_integer_open}"
    )

    # size=exactly amount of transaction size
    params.update({"size": size if closing_size_ok else 0})

    if closed_label_status is None:

        label_closed: str = get_label("closed", label_transaction)

    else:

        label_closed: str = get_label(closed_label_status, label_transaction)

    params.update({"label": label_closed})

    params.update({"instrument_name": transaction["instrument_name"]})

    return params


def get_basic_closing_paramaters_combo_pair(
    selected_transactions: list,
) -> dict:
    """_summary_

    Args:
        selected_transactions (list): pair of instruments in the same futureSpread label

    Returns:
        dict: _description_
    """

    # provide dict placeholder for params
    params = {}

    # default type: limit
    params.update({"type": "limit"})

    # determine side
    side = "buy"
    params.update({"side": side})
    basic_size = abs(max([o["amount"] for o in selected_transactions]))

    label_integer_open = [o["label"] for o in selected_transactions][0]
    log.warning(f"label_integer_open {label_integer_open}")

    instrument_name = [o["combo_id"] for o in selected_transactions][0]

    params.update({"size": basic_size})

    label_closed: str = get_label("closed", label_integer_open)
    params.update({"label": label_closed})

    params.update({"instrument_name": instrument_name})

    return params


@dataclass(unsafe_hash=True, slots=True)
class ManageStrategy:
    """
    https://stackoverflow.com/questions/13646245/is-it-possible-to-make-abstract-classes-in-python/13646263#13646263

    """

    # @abstractmethod
    def understanding_the_market(self) -> None:
        """ """
        pass

    # @abstractmethod
    def risk_managament(self) -> None:
        """ """
        pass

    # @abstractmethod
    def opening_position(self) -> None:
        """ """
        pass

    # @abstractmethod
    def closing_position(self) -> None:
        """ """
        pass

    # @abstractmethod
    def cancelling_order(self) -> None:
        """ """
        pass

    # @abstractmethod
    def modifying_order(self) -> None:
        """ """
        pass


@dataclass(unsafe_hash=True, slots=True)
class BasicStrategy(ManageStrategy):
    """ """

    strategy_label: str
    strategy_parameters: dict

    def get_basic_opening_parameters(
        self,
        ask_price: float = None,
        bid_price: float = None,
        notional: float = None,
    ) -> dict:
        """ """

        # provide placeholder for params
        params = {}

        # default type: limit
        params.update({"type": "limit"})

        strategy_config: dict = self.strategy_parameters

        side: str = strategy_config["side"]

        params.update({"side": side})

        if side == "sell":
            params.update({"entry_price": ask_price})

        if side == "buy":
            params.update({"entry_price": bid_price})

        label_open: str = get_label("open", self.strategy_label)
        params.update({"label": label_open})

        return params

    def get_basic_closing_paramaters(
        self, selected_transaction: list, closed_orders_label_strategy: list
    ) -> dict:
        """ """

        return get_basic_closing_paramaters(
            selected_transaction, closed_orders_label_strategy
        )

    def get_basic_closing_paramaters_combo_pair(
        self, selected_transactions: list
    ) -> dict:
        """ """

        return get_basic_closing_paramaters_combo_pair(selected_transactions)
