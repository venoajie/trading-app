import asyncpg
from loguru import logger as log
from src.shared.config.settings import POSTGRES_DSN

class PostgresClient:
    _pool = None
    
    async def execute(self, query: str, *args):
        async with self._pool.acquire() as conn:
            return await conn.execute(query, *args)
    
    async def fetch(self, query: str, *args):
        async with self._pool.acquire() as conn:
            return await conn.fetch(query, *args)
    
    async def fetchrow(self, query: str, *args):
        async with self._pool.acquire() as conn:
            return await conn.fetchrow(query, *args)
            
    async def start_pool(self):
        if not self._pool:
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
    
    async def insert_json(self, table: str, data: dict):
        async with self._pool.acquire() as conn:
            return await conn.execute(
                f"INSERT INTO {table} (data) VALUES ($1)",
                data
            )
    
    async def update_json_field(self, table: str, id: int, field: str, value):
        async with self._pool.acquire() as conn:
            return await conn.execute(
                f"UPDATE {table} SET data = jsonb_set(data, '{{{field}}}', $1) WHERE id = $2",
                value, id
            )
    
    async def json_table_query(self, table: str, columns: list, condition: str = ""):
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