# core/db/postgres.py
import orjson
from typing import Any, Optional, Union, List, Dict
import asyncpg
from loguru import logger as log

# user defined formulas
from core.db.redis import publishing_specific_purposes
from src.shared.config.settings import POSTGRES_DSN


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
            orjson.dumps(data)
        )

        await self.start_pool()
        async with self._pool.acquire() as conn:
            await conn.execute(query, *params)
        
        # Publish update to Redis
        await self.publish_table_update(table)


    async def fetch_active_trades(self, query):
        await self.start_pool()
        async with self._pool.acquire() as conn:
            return await conn.fetch(query)

    async def delete_row(
        self,
        table: str,
        filter_col: str,
        operator: str,
        filter_value: Any
    ) -> None:
        """
        Delete rows from PostgreSQL table
        """
        if operator.upper() == "LIKE":
            query = f"DELETE FROM {table} WHERE {filter_col} LIKE $1"
            filter_value = f"%{filter_value}%"
        else:
            query = f"DELETE FROM {table} WHERE {filter_col} {operator} $1"
        
        await self.start_pool()
        async with self._pool.acquire() as conn:
            await conn.execute(query, filter_value)
        
        # Publish update to Redis
        await self.publish_table_update(table)

    async def update_status_data(
        self,
        table: str,
        data_column: str,
        filter_col: str,
        filter_value: Any,
        new_value: Any
    ) -> None:
        """
        Update data in PostgreSQL table
        """
        # Update JSONB field
        if data_column.startswith("data->"):
            json_path = data_column.replace("data->", "")
            query = f"""
                UPDATE {table}
                SET data = jsonb_set(data, '{{{json_path}}}', $1::jsonb)
                WHERE {filter_col} = $2
            """
            params = (orjson.dumps(new_value), filter_value)
        # Update top-level column
        else:
            query = f"""
                UPDATE {table}
                SET {data_column} = $1
                WHERE {filter_col} = $2
            """
            params = (new_value, filter_value)
        
        await self.start_pool()
        async with self._pool.acquire() as conn:
            await conn.execute(query, *params)
        
        # Publish update to Redis
        await self.publish_table_update(table)

    async def publish_table_update(self, table: str) -> None:
        """
        Publish table updates to Redis
        """
        if "orders" in table:
            active_trades = await self.fetch_active_trades()
            result = {
                "params": {"data": active_trades},
                "method": "subscription"
            }
            await publishing_specific_purposes("sqlite_record_updating", result)


    async def get_table_schema(self,table_name: str) -> list:
        """
        Get column details for a table
        """
        query = """
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = $1
        """
        return await fetch(query)

    async def query_table_data(self,
        table_name: str, 
        limit: int = 10
    ) -> list:
        """
        Query data from a table
        """
        query = f"SELECT * FROM {table_name} LIMIT $1"
        return await fetch(query)


# Singleton instance
postgres_client = PostgresClient()


# Add to module-level aliases
delete_row = postgres_client.delete_row
update_status_data = postgres_client.update_status_data
insert_trade_or_order = postgres_client.insert_trade_or_order
fetch = postgres_client.fetch_active_trades



# View orders table schema
schema = postgres_client. get_table_schema("orders")
print(schema)
for col in schema:
    print(f"{col['column_name']}: {col['data_type']}")

# View first 10 rows
rows = postgres_client. query_table_data("orders")
print(rows)
for row in rows:
    print(row)