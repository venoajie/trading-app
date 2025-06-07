# core/db/postgres.py
import json
import asyncpg
from loguru import logger as log

# user defined formulas
from core.db.redis import publishing_specific_purposes
from src.shared.config.settings import POSTGRES_DSN


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

    if "order" in table:
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


async def messaging_all_services(
    data: dict,
    table: str,
) -> None:

    if "order" in table:

        result = {}
        result.update({"params": {}})
        result.update({"method": "subscription"})
        result["params"].update({"data": data})

        await publishing_specific_purposes(
            "sqlite_record_updating",
            data,
        )


class PostgresClient:
    def __init__(self):
        self._pool = None
        
    async def start_pool(self):
        if not self._pool:
            for _ in range(3):
                try:
                    self._pool = await asyncpg.create_pool(
                        dsn=POSTGRES_DSN,
                        min_size=5,
                        max_size=20,
                        command_timeout=60,
                        server_settings={
                            'application_name': 'trading-app',
                            'jit': 'off'
                        }
                    )
                    log.info("PostgreSQL pool created")
                    return
                except Exception as e:
                    log.error(f"Connection failed: {e}")
                    await asyncio.sleep(2)
            raise ConnectionError("Failed to create PostgreSQL pool")
    
    async def insert_trade_or_order(self, data: dict):
        currency = data.get('fee_currency') or data['instrument_name'].split('-')[0].upper()
        is_trade = 'trade_id' in data
        
        query = """
            INSERT INTO orders (
                currency, instrument_name, label, amount_dir, price, 
                side, timestamp, trade_id, order_id, is_open, data
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (currency, instrument_name, COALESCE(trade_id, order_id)) 
            DO UPDATE SET 
                label = EXCLUDED.label,
                amount_dir = EXCLUDED.amount_dir,
                price = EXCLUDED.price,
                side = EXCLUDED.side,
                timestamp = EXCLUDED.timestamp,
                is_open = EXCLUDED.is_open,
                data = EXCLUDED.data
        """
        params = (
            currency,
            data['instrument_name'],
            data.get('label'),
            data.get('amount'),
            data.get('price'),
            data.get('side') or data.get('direction'),
            data.get('timestamp'),
            data.get('trade_id'),
            data.get('order_id'),
            is_trade,  # Mark as open if it's a trade
            json.dumps(data)
        )
        
        await self.start_pool()
        try:
            async with self._pool.acquire() as conn:
                return await conn.execute(query, *params)
        finally:
            await messaging_all_services( data, table,)

    async def fetch_active_trades(self, query):
        await self.start_pool()
        async with self._pool.acquire() as conn:
            return await conn.fetch(query)

# Singleton instance
postgres_client = PostgresClient()

# Module-level aliases
insert_json = postgres_client.insert_trade_or_order
fetch = postgres_client.fetch_active_trades