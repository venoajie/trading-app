# core/db/postgres.py
import json
import asyncpg
from loguru import logger as log
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
            json.dumps(data)
        )
        
        await self.start_pool()
        async with self._pool.acquire() as conn:
            return await conn.execute(query, *params)

    async def fetch_active_trades(self, query):
        await self.start_pool()
        async with self._pool.acquire() as conn:
            return await conn.fetch(query)

# Singleton instance
postgres_client = PostgresClient()

# Module-level aliases
insert_json = postgres_client.insert_trade_or_order
fetch = postgres_client.fetch_active_trades