import asyncio  # Added missing import
import asyncpg
from loguru import logger as log
from src.shared.config.settings import POSTGRES_DSN

class PostgresClient:
    def __init__(self):  # Initialize _pool attribute
        self._pool = None
        
    async def start_pool(self):
        if not self._pool:
            for _ in range(3):  # Retry mechanism
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
    
    async def insert_json(self, table: str, data: dict):
    data['currency'] = data['fee_currency']
    query = """INSERT INTO my_trades_all_json (currency, data) 
               VALUES ($1, $2) 
               ON CONFLICT (trade_id) DO NOTHING"""
    await self.start_pool()
    async with self._pool.acquire() as conn:
        return await conn.execute(query, data['currency'], data)
    
    async def fetch(self, query: str, *args, timeout=30):
        await self.start_pool()
        async with self._pool.acquire(timeout=timeout) as conn:
            return await conn.fetch(query, *args)
    
    async def fetchrow(self, query: str, *args, timeout=30):
        await self.start_pool()
        async with self._pool.acquire(timeout=timeout) as conn:
            return await conn.fetchrow(query, *args)
            
    async def update_json_field(self, table: str, id: int, field: str, value):
        query = f"""UPDATE {table} SET data = jsonb_set(data, '{{{field}}}', $1) WHERE id = $2"""
        await self.start_pool()
        async with self._pool.acquire() as conn:
            return await conn.execute(query, value, id)
    
    async def json_table_query(self, table: str, columns: list, condition: str = ""):
        await self.start_pool()  # Ensure pool exists
        cols = ", ".join(columns)
        query = f"""
            SELECT j.* 
            FROM {table}, 
            JSON_TABLE(data, '$' COLUMNS({cols})) AS j
            {condition}
        """
        async with self._pool.acquire() as conn:
            return await conn.fetch(query)


# Singleton instance
postgres_client = PostgresClient()

async def init_db():
    await postgres_client.start_pool()
    log.info("PostgreSQL connection pool initialized")
    return postgres_client

    
# Add module-level aliases for singleton methods
insert_json = postgres_client.insert_json
fetch = postgres_client.fetch
fetchrow = postgres_client.fetchrow
update_json_field = postgres_client.update_json_field
json_table_query = postgres_client.json_table_query