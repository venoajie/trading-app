# -*- coding: utf-8 -*-


def remove_double_brackets_in_list(data: list) -> list:
    """_summary_

    Args:
        data (list): instance: [
                                ['BTC-30AUG24', 'BTC-6SEP24', 'BTC-27SEP24', 'BTC-27DEC24',
                                'BTC-28MAR25', 'BTC-27JUN25', 'BTC-PERPETUAL'
                                ],
                                ['ETH-30AUG24', 'ETH-6SEP24', 'ETH-27SEP24', 'ETH-27DEC24',
                                'ETH-28MAR25', 'ETH-27JUN25', 'ETH-PERPETUAL'
                                ]
                                ]

    Returns:
        list: _description_

    Reference:
        https://stackoverflow.com/questions/952914/how-do-i-make-a-flat-list-out-of-a-list-of-lists
    """
    return [o for os in data for o in os]


def get_duplicated_elements(data: list) -> list:
    """

    Args:
        data (list)

    Returns:
        list:

    Example:
        data_original = ['A', 'A', 'B', 'B', 'B', 'C']
        data_cleaned = ['A','B']

    Reference:
        https://www.geeksforgeeks.org/python-program-print-duplicates-list-integers/
        https://medium.com/@ryan_forrester_/finding-duplicates-in-python-lists-a-complete-guide-6092b34ac375

    """

    return list(set([x for x in data if data.count(x) > 1]))


def remove_redundant_elements(data: list) -> list:
    """
    Remove redundant items in a list

    Args:
        data (list)

    Returns:
        list:

    Example:
        data_original = ['A', 'A', 'B', 'B', 'B', 'C']
        data_cleaned = ['A','B','C']

    Reference:
        1. https://stackoverflow.com/questions/9427163/remove-duplicate-dict-in-list-in-python
        2. https://python.plainenglish.io/how-to-remove-duplicate-elements-from-lists-without-using-sets-in-python-5796e93e6d43
    """

    # Create an empty list
    result = []

    # Check if the data is a list and not empty
    if isinstance(data, list) and data != []:
        try:
            # Ref 1
            result = list({frozenset(item.items()): item for item in data}.values())

        except:
            # Ref 2
            result = list(dict.fromkeys(data))

    return result


def find_non_repeatable_elements(
    data1: list,
    data2: list,
) -> list:
    """

    Comparing two lists and picking non-repeatable items between them

    Args:
        data (list)  and its subset (list) for comparation

    Returns:
        list

    Example:
        data_original = [1, 2, 3, 4, 5] # all data
        data_redundant = [2, 4] # subset of all data
        data_cleaned =  [1, 3, 5]

    Reference:
        https://stackoverflow.com/questions/45098206/unique-values-between-2-lists

    """
    return [i for i in data1 if i not in data2]


def get_unique_elements(
    data1: list,
    data2: list,
) -> list:
    """

    Comparing two lists and picking only unique items between them

    Args:
        data (list)  and its subset (list) for comparation

    Returns:
        list

    Example:
        data1 = [4, 5, 6] # all data
        data_redundant = [1, 2, 3, 4, 5] # subset of all data
        data_cleaned =  [6]

    Reference:
        https://stackoverflow.com/questions/45098206/unique-values-between-2-lists

    """
    return list(set(data1).difference(data2))


def remove_dict_elements(
    original: dict,
    item_to_remove: str,
) -> str:
    """ """

    return {i: original[i] for i in original if i != item_to_remove}


def remove_list_elements(
    original: dict,
    item_to_remove: str,
) -> str:
    """
    https://stackoverflow.com/questions/13254241/removing-key-values-pairs-from-a-list-of-dictionaries
    """

    return [
        {key: value for key, value in dict.items() if key != item_to_remove}
        for dict in original
    ]


def extract_currency_from_text(words: str) -> str:
    """

    some variables:
    chart.trades.BTC-PERPETUAL.1
    incremental_ticker.BTC-4OCT24
    """

    if "." in words:
        filter1 = (words.partition(".")[2]).lower()

        if "." in filter1:
            filter1 = (filter1.partition(".")[2]).lower()

            if "chart.trades" in words:
                filter1 = (words.partition(".")[2]).lower()

            if "." in filter1:
                filter1 = (filter1.partition(".")[2]).lower()

                if "." in filter1:
                    filter1 = (filter1.partition(".")[0]).lower()

    else:
        filter1 = (words.partition(".")[0]).lower()

    return (filter1.partition("-")[0]).lower()


def remove_apostrophes_from_json(json_load: list) -> int:
    """ """
    import ast

    return [ast.literal_eval(str(i)) for i in json_load]


def parsing_sqlite_json_output(json_load: list) -> int:
    """
    parsing_sqlite_json_output

    References:
        https://stackoverflow.com/questions/46991650/remove-quotes-from-list-of-dictionaries-in-python
        https://stackoverflow.com/questions/14611352/malformed-string-valueerror-ast-literal-eval-with-string-representation-of-tup
    """

    try:

        result_json = [
            i.replace(":false", ":False")
            .replace(":true", ":True")
            .replace(":null", ":None")
            for i in json_load
        ]
        # print (f'result_json {[ast.literal_eval(str(i)) for i in result_json]}')
        return remove_apostrophes_from_json(result_json)

    except:
        return []


def parsing_redis_market_json_output(json_load: list) -> int:
    """
    parsing_sqlite_json_output

    References:
        https://stackoverflow.com/questions/46991650/remove-quotes-from-list-of-dictionaries-in-python
        https://stackoverflow.com/questions/14611352/malformed-string-valueerror-ast-literal-eval-with-string-representation-of-tup
    """

    try:

        result_json = [
            i.replace(":false", ":False").replace(":true", ":True") for i in json_load
        ]
        # print (f'result_json {[ast.literal_eval(str(i)) for i in result_json]}')
        return remove_apostrophes_from_json(result_json)

    except:
        return []


def get_strings_before_character(
    label: str,
    character: str = "-",
    character_place: int = [0, 2],
) -> str:
    """

    Get strings before a character

    Args:
        label (str)
        character (str)
        character_place (list (default)/int)

    Returns:
        str

    Example:
        data_original = 'hedgingSpot-open-1671189554374' become 'hedgingSpot'

    Reference:
        https://stackoverflow.com/questions/32682199/how-to-get-string-before-hyphen
    """

    if isinstance(character_place, list):
        splitted1 = label.split(character)[character_place[0]]
        splitted2 = label.split(character)[character_place[1]]
        splitted = f"{splitted1}-{splitted2}"
    else:
        splitted = label.split(character)[character_place]

    return splitted


def extract_integers_from_text(words: list) -> int:
    """
    Extracting integers from label text. More general than get integer in parsing label function
    """

    words_to_str = str(
        words
    )  # ensuring if integer used as argument, will be returned as itself

    return int("".join([o for o in words_to_str if o.isdigit()]))


def extract_integers_aggregation_from_text(
    identifier,
    aggregator,
    words: list,
) -> int:
    """
    identifier: id: trade/order/etc
    aggregator: min, max, len
    """

    return aggregator([extract_integers_from_text(o[f"{identifier}"]) for o in words])


def parsing_label(
    label: str,
    integer: int = None,
) -> dict:
    """

    Args:
        label (str)

    Returns:
        dict

    Example:
        'hedgingSpot-open-1671189554374'
        main: 'hedgingSpot'
        super_main: 'hedgingSpot'
        int = 1671189554374
        transaction_status:'hedgingSpot-open'
        transaction_net:'hedgingSpot-1671189554374'

        'every5mtestLong-open-1681617021717'
        main: 'every5mtestLong'
        super_main: 'every5mtest'
        int = 1681617021717
        transaction_status:'every5mtestLong-open''
        transaction_net:'every5mtestLong-1681617021717''

    """
    try:
        try:
            get_integer = get_strings_before_character(label, "-", 2)
        except:
            get_integer = get_strings_before_character(label, "-", 1)
    except:
        get_integer = None

    try:
        status = get_strings_before_character(label, "-", [0, 1])
    except:
        status = None

    try:
        net = get_strings_before_character(label)
    except:
        net = None

    try:
        main = get_strings_before_character(label, "-", 0)
    except:
        main = None

    try:
        side = ["Short", "Long"]
        super_main = [main.replace(o, "") for o in side if o in main]
    except:
        super_main = None

    try:
        closed_to_open = f"{main}-open-{get_integer}"

    except:
        closed_to_open = None

    try:
        if "Short" in main:
            flip = main.replace("Short", "Long")

        if "Long" in main:
            flip = main.replace("Long", "Short")
        flipping_closed = f"{flip}-open-{integer}"
    except:
        flipping_closed = None

    return {
        # "super_main":  bool([o not in main for o in side]),
        "super_main": (
            None
            if super_main == None
            else (main if all([o not in main for o in side]) else super_main[0])
        ),
        "main": main,
        "int": get_integer,
        "transaction_status": status,
        "transaction_net": net,
        "flipping_closed": flipping_closed,
        "closed_to_open": closed_to_open,
    }


def transform_nested_dict_to_list(list_example) -> dict:
    """ """
    len_tick = len(list_example["volume"])

    my_list = []

    for k in range(len_tick):

        dict_result = dict(
            volume=list_example["volume"][k],
            tick=list_example["ticks"][k],
            open=list_example["open"][k],
            low=list_example["low"][k],
            high=list_example["high"][k],
            cost=list_example["cost"][k],
            close=list_example["close"][k],
        )

        my_list.append(dict_result)

    return my_list


def transform_nested_dict_to_list_ohlc(list_example) -> dict:
    """ """
    len_tick = len(list_example["open"])

    my_list = []

    for k in range(len_tick):

        dict_result = dict(
            volume=list_example["volume"][k],
            tick=list_example["ticks"][k],
            open=list_example["open"][k],
            high=list_example["high"][k],
            low=list_example["low"][k],
            cost=list_example["cost"][k],
            close=list_example["close"][k],
        )

        my_list.append(dict_result)

    return my_list


def filtering_list_with_missing_key(
    list_examples: list,
    missing_key: str = "label",
) -> dict:
    """
    https://stackoverflow.com/questions/34710571/can-i-use-a-list-comprehension-on-a-list-of-dictionaries-if-a-key-is-missing

    """
    return [o for o in list_examples if missing_key not in o]


def sorting_list(
    listing: list,
    item_reference: str = "price",
    is_reversed: bool = True,
) -> list:
    """
    https://sparkbyexamples.com/python/sort-list-of-dictionaries-by-value-in-python/

    Args:
        listing (list): _description_
        item_reference (str, optional): _description_. Defaults to "price".
        is_reversed (bool, optional): _description_. Defaults to True.
                                    True = from_highest_to_lowest
                                    False = from_lowest_to_highest

    Returns:
        list: _description_
    """
    import operator

    return sorted(
        listing,
        key=operator.itemgetter(item_reference),
        reverse=is_reversed,
    )


def hashing(
    timestamp: int,
    client_id: str,
    apiSecret: str,
) -> str:

    import hashlib
    import hmac
    from urllib.parse import urlencode

    payload = {"apiKey": client_id, "timestamp": timestamp}

    return hmac.new(
        apiSecret.encode("utf-8"),
        urlencode(payload).encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def convert_to_bytes(
    input_string: str,
    encoding: str = "utf-8",
) -> bytes:
    """
    https://www.w3resource.com/python-exercises/extended-data-types/python-extended-data-types-bytes-bytearrays-exercise-1.php

    encodings = [
        "utf-8",
        "utf-16",
        "ascii"
        ]

    """
    try:
        encoded_bytes = input_string.encode(encoding)
        return encoded_bytes

    except Exception as e:
        ("Error:", e)
        return None


def labelling(
    order: str,
    strategy: str,
    id_strategy: int = None,
) -> str:
    """
    labelling based on  strategy and unix time at order is made
    """

    id_unix_time = get_now_unix_time() if id_strategy is None else id_strategy

    return (
        (f"{strategy}-{order}-{id_unix_time}")
        if id_strategy is None
        else (f"{id_strategy}")
    )
