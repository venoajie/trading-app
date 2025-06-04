""" 
core/db/sqlite.py 
""" 

# built ins
import asyncio
import json
import sqlite3
from contextlib import contextmanager
from typing import Any, Optional, Union, List, Dict
import os
import aiosqlite
from loguru import logger as log

# user defined formulas
from core.db.redis import publishing_specific_purposes

from src.shared.utils import (
    error_handling,
    string_modification as str_mod,
)

# Initialize module-level Redis client (to be set at runtime)
_redis_client = None

def set_redis_client(redis_client):
    """Set Redis client for error reporting"""
    global _redis_client
    _redis_client = redis_client
    
def get_db_path():
    """Get SQLite database path with Docker compatibility"""
    base_path = os.environ.get('DB_BASE_PATH', '/app/data')
    db_path = os.path.join(base_path, 'trading.sqlite3')
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    # Return path without changing permissions
    return db_path

def create_connection():
    db_path = get_db_path()
    conn = sqlite3.connect(db_path, check_same_thread=False)
    
    # Enable write-ahead logging for better concurrency
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    
    # Set timeout to handle concurrent access
    conn.execute("PRAGMA busy_timeout=5000")
    return conn

async def telegram_bot_sendtext(bot_message, purpose: str = "general_error") -> None:

    return await telegram_bot(bot_message, purpose)


async def create_dataBase_sqlite(db_name: str = None) -> None:
    """
    https://stackoverflow.com/questions/71729956/aiosqlite-result-object-has-no-attribue-execute
    """

    if db_name is None:
        db_name = get_db_path() 
        
    try:
        conn = await aiosqlite.connect(db_name)
        cur = await conn.cursor()
        await conn.commit()
        await conn.close()

    except Exception as error:

        await error_handling.parse_error_message_with_redis(
            client_redis,
            error,
        )


@contextmanager
async def db_ops(db_name: str == None):
    """
    # prepare sqlite initial connection + close
            Return and rtype: None
            #https://stackoverflow.com/questions/67436362/decorator-for-sqlite3/67436763#67436763
            # https://charlesleifer.com/blog/going-fast-with-sqlite-and-python/
            https://code-kamran.medium.com/python-convert-json-to-sqlite-d6fa8952a319
    """

    if db_name is None:
        db_name = get_db_path() 

    conn = await aiosqlite.connect(db_name, isolation_level=None)
        
    try:
        cur = await conn.cursor()
        yield cur

    except Exception as e:

        await error_handling.parse_error_message_with_redis(
            client_redis,
            e,
        )

        await conn.rollback()
        raise e

    else:
        await conn.commit()
        await conn.close()


async def insert_tables(
    table_name: str,
    params,#: list | dict | str,
    db_name: str = None,
    )-> None:
    """
    
    Insert data into specified table
    
    alternative insert format (safer):
    https://stackoverflow.com/questions/56910918/saving-json-data-to-sqlite-python

    """

    if db_name is None:
        db_name = get_db_path() 
        
    try:

        async with aiosqlite.connect(
            db_name, 
            isolation_level=None,
            ) as db:

            await db.execute("pragma journal_mode=wal;")

            if "json" in table_name:

                # input was in list format. Insert them to db one by one
                if isinstance(params, list):
                    for param in params:
                        insert_table_json = f"""INSERT  OR IGNORE INTO {table_name} (data) VALUES (json ('{json.dumps(param)}'));"""
                        await db.execute(insert_table_json)

                # input is in dict format. Insert them to db directly
                if isinstance(params, dict):
                    insert_table_json = f"""INSERT  OR IGNORE INTO {table_name} (data) VALUES (json ('{json.dumps(params)}'));"""
                    await db.execute(insert_table_json)

                if isinstance(params, str):
                    insert_table_json = f"""INSERT OR IGNORE INTO {table_name} (data) VALUES (json ('{(params)}'));"""
                    await db.execute(insert_table_json)

    # except sqlite3.IntegrityError as error:
    #    pass

    # except sqlite3.OperationalError as error:
    #    pass

    except Exception as error:

        await error_handling.parse_error_message_with_redis(
            client_redis,
            error,
        )

    finally:

        if "my_trades" in table_name or "order" in table_name:

            query_trades = f"SELECT * FROM  v_trading_all_active"

            my_trades_currency_all_transactions: list = (
                await executing_query_with_return(query_trades)
            )

            result = {}
            result.update({"params": {}})
            result.update({"method": "subscription"})
            result["params"].update({"data": my_trades_currency_all_transactions})

            await publishing_specific_purposes(
                "sqlite_record_updating",
                result,
            )


async def querying_table(
    table: str = "mytrades",
    database: str = None,
    filter: str = None,
    operator=None,
    filter_value=None,
) -> list:
    """
    Reference
    # https://stackoverflow.com/questions/65934371/return-data-from-sqlite-with-headers-python3
    """

    from utilities import string_modification as str_mod

    if database is None:
        database = get_db_path() 
        
    NONE_DATA: None = [0, None, []]

    query_table = f"SELECT  * FROM {table} WHERE  {filter} {operator}?"

    filter_val = (f"{filter_value}",)

    if filter == None:
        query_table = f"SELECT  * FROM {table}"

    if "market_analytics" in table and "last" in table:
        query_table = f"SELECT  * FROM market_analytics_json ORDER BY id DESC LIMIT 1"

    combine_result = []

    try:
        async with aiosqlite.connect(database, isolation_level=None) as db:

            await db.execute("pragma journal_mode=wal;")

            db = (
                db.execute(query_table)
                if filter == None
                else db.execute(query_table, filter_val)
            )

            async with db as cur:
                fetchall = await cur.fetchall()

                head = map(lambda attr: attr[0], cur.description)
                headers = list(head)

        for i in fetchall:
            combine_result.append(dict(zip(headers, i)))

    except Exception as error:
        log.critical(f"querying_table  {table} {error}")

        await error_handling.parse_error_message_with_redis(
            client_redis,
            error,
        )

    return dict(
        all=[] if combine_result in NONE_DATA else (combine_result),
        list_data_only=(
            []
            if combine_result in NONE_DATA
            else str_mod.parsing_sqlite_json_output([o["data"] for o in combine_result])
        ),
    )


async def deleting_row(
    table: str = "mytrades",
    database: str = None,
    filter: str = None,
    operator=None,
    filter_value=None,
) -> list:
    """ """

    if database is None:
        database = get_db_path() 
        
    query_table = f"DELETE  FROM {table} WHERE  {filter} {operator}?"
    query_table_filter_none = f"DELETE FROM {table}"

    filter_val = (f"{filter_value}",)

    if "LIKE" in operator:
        filter_val = (f"""' %{filter_value}%' """,)

    try:
        async with aiosqlite.connect(database, isolation_level=None) as db:

            await db.execute("pragma journal_mode=wal;")

            if filter == None:
                await db.execute(query_table_filter_none)
            else:
                await db.execute(query_table, filter_val)

    # except sqlite3.IntegrityError as error:
    #    pass

    # except sqlite3.OperationalError as error:
    #    pass

    except Exception as error:
        log.critical(f"deleting_row {query_table} {error}")

        await error_handling.parse_error_message_with_redis(
            client_redis,
            error,
        )

    finally:

        if "my_trades" in table or "order" in table:

            query_trades = f"SELECT * FROM  v_trading_all_active"

            my_trades_currency_all_transactions: list = (
                await executing_query_with_return(query_trades)
            )

            result = {}
            result.update({"params": {}})
            result.update({"method": "subscription"})
            result["params"].update({"data": my_trades_currency_all_transactions})

            await publishing_specific_purposes(
                "sqlite_record_updating",
                result,
            )


async def querying_duplicated_transactions(
    label: str,
    group_by: str = "trade_id",
    database: str = None,
) -> list:
    """ """

    if database is None:
        database = get_db_path() 
        
    # query_table = f"""SELECT CAST(SUBSTR((label),-13)as integer) AS label_int, count (*)  FROM {label} GROUP BY label_int HAVING COUNT (*) >1"""
    query_table = f"""SELECT id, data, {group_by}  FROM {label} GROUP BY {group_by} HAVING count(*) >1"""
    combine_result = []

    try:
        async with aiosqlite.connect(database, isolation_level=None) as db:

            db = db.execute(query_table)

            async with db as cur:

                fetchall = await cur.fetchall()

                head = map(lambda attr: attr[0], cur.description)
                headers = list(head)

        for i in fetchall:
            combine_result.append(dict(zip(headers, i)))

    except Exception as error:
        log.critical(f"querying_table {query_table} {error}")

        await error_handling.parse_error_message_with_redis(
            client_redis,
            error,
        )

    return 0 if (combine_result == [] or combine_result == None) else (combine_result)


async def add_additional_column(
    column_name,
    dataType,
    table: str = "ohlc1_eth_perp_json",
    database: str = None,
) -> list:
    """ """

    if database is None:
        database = get_db_path() 
        
    try:
        query_table = f"ALTER TABLE {table} ADD {column_name} {dataType}"

        async with aiosqlite.connect(database, isolation_level=None) as db:

            await db.execute("pragma journal_mode=wal;")

            db = await db.execute(query_table)

            async with db as cur:
                result = await cur.fetchone()

    except Exception as error:
        print(f"querying_table {query_table} {error}")

        await error_handling.parse_error_message_with_redis(
            client_redis,
            error,
        )

    try:
        return 0 if result == None else int(result[0])
    except:
        return None


def querying_last_open_interest_tick(
    last_tick: int, table: str = "ohlc1_eth_perp_json"
) -> str:

    return f"SELECT open_interest FROM {table} WHERE tick is {last_tick}"


async def update_status_data(
    table: str,
    data_column: str,
    filter: str,
    filter_value: any,
    new_value: any,
    operator,#=None | str,
    database: str = None,
) -> None:
    """
    https://www.beekeeperstudio.io/blog/sqlite-json-with-text
    https://www.sqlitetutorial.net/sqlite-json-functions/sqlite-json_replace-function/
    https://stackoverflow.com/questions/75320010/update-json-data-in-sqlite3
    """

    where_clause = f"WHERE {filter}  LIKE '%{filter_value}%'"

    query = f"""UPDATE {table} SET data = JSON_REPLACE (data, '$.{data_column}', '{new_value}') {where_clause};"""

    if "is_open" in data_column:
        query = f"""UPDATE {table} SET {data_column} = ({new_value}) {where_clause};"""

    if "ohlc" in table:

        query = f"""UPDATE {table} SET {data_column} = JSON_REPLACE ('{json.dumps(new_value)}')   {where_clause};"""

        if data_column == "open_interest":

            query = (
                f"""UPDATE {table} SET {data_column} = ({new_value})  {where_clause};"""
            )

    # log.warning (f"query {query}")
    try:

        if database is None:
            database = get_db_path() 
            
        async with aiosqlite.connect(database, 
                                     isolation_level=None,
                                     ) as db:

            await db.execute("pragma journal_mode=wal;")

            await db.execute(query)

    # except sqlite3.IntegrityError as error:
    #    pass

    # except sqlite3.OperationalError as error:
    #    pass

    except Exception as error:
        log.critical(f" ERROR {error}")
        log.info(f"query update status data {query}")

        await error_handling.parse_error_message_with_redis(
            client_redis,
            error,
        )

    finally:

        if "my_trades" in table or "order" in table:

            query_trades = f"SELECT * FROM  v_trading_all_active"

            my_trades_currency_all_transactions: list = (
                await executing_query_with_return(query_trades)
            )

            result = {}
            result.update({"params": {}})
            result.update({"method": "subscription"})
            result["params"].update({"data": my_trades_currency_all_transactions})

            await publishing_specific_purposes(
                "sqlite_record_updating",
                result,
            )


def querying_open_interest(
    price: float = "close",
    table: str = "ohlc1_eth_perp_json",
    limit: int = None,
) -> str:

    all_data = f"""SELECT tick, JSON_EXTRACT (data, '$.volume') AS volume, JSON_EXTRACT (data, '$.{price}')  AS close, open_interest, \
        (open_interest - LAG (open_interest, 1, 0) OVER (ORDER BY tick)) as delta_oi FROM {table}"""
    return all_data if limit == None else f"""{all_data} limit {limit}"""


def querying_ohlc_price_vol(
    price: float = "close",
    table: str = "ohlc1_eth_perp_json",
    limit: int = None,
) -> str:

    all_data = f"""SELECT  tick, JSON_EXTRACT (data, '$.volume') AS volume, JSON_EXTRACT (data, '$.{price}')  AS {price} FROM {table} ORDER BY tick DESC"""

    return all_data if limit == None else f"""{all_data} limit {limit}"""


def querying_ohlc_closed(
    price: float = "close",
    table: str = "ohlc1_eth_perp_json",
    limit: int = None,
) -> str:

    all_data = f"""SELECT  JSON_EXTRACT (data, '$.{price}')  AS close FROM {table} ORDER BY tick DESC"""

    return all_data if limit == None else f"""{all_data} limit {limit}"""


def querying_arithmetic_operator(
    item: str,
    operator: str = "MAX",
    table: str = "ohlc1_eth_perp_json",
) -> float:

    return f"SELECT {operator} ({item}) FROM {table}"


# Generate SQL insert commands from data
def generate_insert_sql(table_name, data, columns):
    # Construct the column and placeholder strings

    columns_str = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))  # (%s ,%s)

    # Create the SQL INSERT statement
    sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

    # Extract values from data
    values = [tuple(row[col] for col in columns) for row in data]

    balance = "sum(amount_dir) OVER (ORDER BY timestamp) as balance"
    columns = (
        "instrument_name",
        "label",
        "amount_dir",
        "timestamp",
        "order_id",
        balance,
    )

    table_name = "test"
    columns_str = ", ".join(columns)
    print(columns_str)
    placeholders = ", ".join(["%s"] * len(columns))  # (%s ,%s)
    print(placeholders)

    # Create the SQL INSERT statement
    sql = f"SELECT {columns_str}) FROM {table_name}"
    print(sql)

    return sql, values


def querying_based_on_currency_or_instrument_and_strategy(
    table: str,
    currency_or_instrument: str,
    strategy: str = "all",
    status: str = "all",
    columns: list = "standard",
    limit: int = 0,
    order: str = None,
    ordering: str = "DESC",
) -> str:
    """_summary_

    status: all, open, closed

    https://medium.com/@ccpythonprogramming/letting-software-define-the-structure-of-a-database-dynamic-schema-d3bb7e17026c

    Returns:
        _type_: _description_
    """
    standard_columns = (
        f"instrument_name, label, amount_dir as amount, timestamp, order_id"
    )

    balance = f"sum(amount_dir) OVER (ORDER BY timestamp) as balance"

    if "balance" in columns:
        standard_columns = f"instrument_name, label, amount_dir as amount, {balance}, timestamp, order_id"

    if "trade" in table or "order" in table:
        standard_columns = f"{standard_columns}, price"

        if "trade" in table:

            standard_columns = f"{standard_columns}, trade_id"

    if "transaction_log" in table:

        standard_columns = f"{standard_columns}, trade_id, price, type"

        table = f"transaction_log_{str_mod.extract_currency_from_text(currency_or_instrument).lower()}_json"

        # log.error (f"table transaction_log {table}")

    if columns != "standard":

        if "data" in columns:
            standard_columns = ",".join(
                str(f"""{i}{("_dir as amount") if i=="amount" else ""}""")
                for i in columns
            )

        else:
            standard_columns = ",".join(
                str(f"""{i}{("_dir as amount") if i=="amount" else ""}""")
                for i in columns
            )

    where_clause = f"WHERE (instrument_name LIKE '%{currency_or_instrument}%')"

    if strategy != "all":
        where_clause = f"WHERE (instrument_name LIKE '%{currency_or_instrument}%' AND label LIKE '%{strategy}%')"

    if status != "all":
        where_clause = f"WHERE (instrument_name LIKE '%{currency_or_instrument}%' AND label LIKE '%{strategy}%' AND label LIKE '%{status}%')"

    tab = f"SELECT {standard_columns},{balance} FROM {table} {where_clause}"

    if order is not None:

        # tab = f"SELECT instrument_name, label_main as label, amount_dir as amount, order_id, trade_seq FROM {table} {where_clause} ORDER BY {order}"
        tab = f"SELECT {standard_columns},{balance} FROM {table} {where_clause} ORDER BY {order} {ordering} "

    if limit > 0:

        tab = f"{tab} LIMIT {limit}"

    #    log.error (f"table {tab}")
    return tab


async def executing_query_based_on_currency_or_instrument_and_strategy(
    table: str,
    currency_or_instrument,
    strategy: str = "all",
    status: str = "all",
    columns: list = "standard",
    limit: int = 0,
    order: str = "id",
) -> dict:
    """
    Provide execution template for querying summary of trading results from sqlite.
    Consist of transaction label, size, and price only.
    """

    # get query
    query = querying_based_on_currency_or_instrument_and_strategy(
        table,
        currency_or_instrument,
        strategy,
        status,
        columns,
        limit,
        order,
    )

    # execute query
    result = await executing_query_with_return(query)

    # log.critical (f"table {table} filter {filter}")
    # log.info (f"result {result}")

    # define none from queries result. If the result=None, return []
    NONE_DATA: None = [0, None, []]

    # log.error (f"table {query}")
    # log.warning (f"result {result}")

    return [] if not result else (result)


async def executing_query_with_return(
    query_table,
    filter: str = None,
    filter_value=None,
    database: str = None,
) -> list:
    """
    Reference
    # https://stackoverflow.com/questions/65934371/return-data-from-sqlite-with-headers-python3

    Return type: 'list'/'dataframe'

    """
    if database is None:
        database = get_db_path() 
            

    filter_val = (f"{filter_value}",)

    combine_result = []

    try:
        async with aiosqlite.connect(
            database,
            isolation_level=None,
        ) as db:

            await db.execute("pragma journal_mode=wal;")

            db = (
                db.execute(query_table)
                if filter == None
                else db.execute(
                    query_table,
                    filter_val,
                )
            )

            async with db as cur:
                fetchall = await cur.fetchall()

                head = map(lambda attr: attr[0], cur.description)
                headers = list(head)

        for i in fetchall:
            combine_result.append(dict(zip(headers, i)))

    except Exception as error:
        # import traceback
        log.critical(f"querying_table {query_table} {error}")
        # traceback.format_exc()

        await error_handling.parse_error_message_with_redis(
            client_redis,
            error,
        )

    return [] if not combine_result else (combine_result)


async def back_up_db_sqlite(database: str = None) -> None:

    from datetime import datetime

    TIMESTAMP = datetime.now().strftime("%Y%m%d-%H-%M-%S")

    if database is None:
        database = get_db_path() 
            

    src = sqlite3.connect(database)
    dst = sqlite3.connect(f"databases/trdg-{TIMESTAMP}.bak")

    with dst:
        src.backup(dst)
    dst.close()
    src.close()
