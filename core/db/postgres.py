# core/db/postgres.py

import orjson
from typing import Any, Optional, Union, List, Dict
import asyncpg, asyncio
from loguru import logger as log

# user defined formulas
from core.db.redis import publishing_specific_purposes
from core.error_handler import error_handler
from src.shared.config.config import config


def query_insert_trade_or_order(data: dict):
    currency = data.get("fee_currency") or data["instrument_name"].split("-")[0].upper()
    is_trade = "trade_id" in data

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
        data["instrument_name"],
        data.get("label"),
        data.get("amount"),
        data.get("price"),
        data.get("side") or data.get("direction"),
        data.get("timestamp"),
        data.get("trade_id"),
        data.get("order_id"),
        is_trade,  # Mark as open if it's a trade
        orjson.dumps(data),
    )


class PostgresClient:
    def __init__(self):
        self.postgres_config = config.get("postgres")
        self.dsn = self.postgres_config["dsn"] if self.postgres_config else None
        self.pool_config = (
            self.postgres_config["pool"] if self.postgres_config else None
        )
        self._pool = None  #  explicit initialization

    async def start_pool(self):
        if not self._pool or self._pool._closed:  # Check if closed
            for _ in range(3):
                try:
                    self._pool = await asyncpg.create_pool(
                        dsn=self.dsn,
                        min_size=5,
                        max_size=50,
                        command_timeout=60,
                        server_settings={
                            "application_name": "trading-app",
                            "jit": "off",
                        },
                    )
                    log.info("PostgreSQL pool created")
                    return
                except Exception as e:
                    log.error(f"Connection failed: {e}")
                    await asyncio.sleep(2)
            raise ConnectionError("Failed to create PostgreSQL pool")
        
    async def close_pool(self):
        if self._pool:
            await self._pool.close()
            self._pool = None
            
    async def insert_ohlc(self, table_name: str, candle: dict):
        query = f"""
            INSERT INTO {table_name} (data)
            VALUES ($1)
        """
        await self.start_pool()
        async with self._pool.acquire() as conn:
            await conn.execute(query, orjson.dumps(candle))

    async def insert_trade_or_order(self, data: dict):
        currency = (
            data.get("fee_currency") or data["instrument_name"].split("-")[0].upper()
        )
        is_trade = "trade_id" in data

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
            data["instrument_name"],
            data.get("label"),
            data.get("amount"),
            data.get("price"),
            data.get("side") or data.get("direction"),
            data.get("timestamp"),
            data.get("trade_id"),
            data.get("order_id"),
            is_trade,  # Mark as open if it's a trade
            orjson.dumps(data),
        )

        await self.start_pool()
        async with self._pool.acquire() as conn:
            await conn.execute(query, *params)

    async def fetch_active_trades(self, query):
        await self.start_pool()
        async with self._pool.acquire() as conn:
            return await conn.fetch(query)

    async def delete_row(
        self, table: str, filter_col: str, operator: str, filter_value: Any
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

    async def querying_arithmetic_operator(
        self,
        item: str,
        operator: str = "MAX",
        table: str = "ohlc1_btc_perp_json",
    ) -> float:
        """Safe PostgreSQL version using function"""
        query = "SELECT get_arithmetic_value($1, $2, $3)"

        await self.start_pool()
        async with self._pool.acquire() as conn:
            return await conn.fetchval(query, item, operator, table)

    async def update_status_data(
        self,
        table: str,
        data_column: str,
        filter_col: str,
        filter_value: Any,
        new_value: Any,
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

    async def get_table_schema(self, table_name: str) -> list:
        """
        Get column details for a table
        """
        query = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1"

        return await self.fetch_active_trades(query)

    async def query_table_data(self, table_name: str, limit: int = 10) -> list:
        """
        Query data from a table
        """
        query = f"SELECT * FROM {table_name} LIMIT $1"
        return await self.fetch_active_trades(query)

async def shutdown():
    if postgres_client._pool:
        await postgres_client._pool.close()

# Singleton instance
postgres_client = PostgresClient()
schema = postgres_client.get_table_schema

# Add to module-level aliases
querying_by_arithmetic = postgres_client.querying_arithmetic_operator
delete_row = postgres_client.delete_row
update_status_data = postgres_client.update_status_data
insert_trade_or_order = postgres_client.insert_trade_or_order
fetch = postgres_client.fetch_active_trades
insert_ohlc = postgres_client.insert_ohlc
update_status = postgres_client.update_status_data