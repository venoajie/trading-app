# src\services\executor\deribit\processing_orders.py

# built ins
import asyncio

# user defined formula
from core.db.postgres import fetch, insert_trade_or_order, delete_row, update_status_data


async def get_table_schema(table_name: str) -> list:
    """
    Get column details for a table
    """
    query = """
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = $1
    """
    return await fetch(query, table_name)

async def query_table_data(
    table_name: str, 
    limit: int = 10
) -> list:
    """
    Query data from a table
    """
    query = f"SELECT * FROM {table_name} LIMIT $1"
    return await fetch(query, limit)

# View orders table schema
schema = await get_table_schema("orders")
for col in schema:
    print(f"{col['column_name']}: {col['data_type']}")

# View first 10 rows
rows = await query_table_data("orders")
for row in rows:
    print(row)