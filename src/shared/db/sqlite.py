"""
shared/db/sqlite.py
SQLite database management utilities
"""

import sqlite3
import logging
import os
from typing import List, Dict, Any

# Configure logger
log = logging.getLogger(__name__)

def get_db_path() -> str:
    """Get SQLite database path from environment"""
    base_path = os.getenv("DB_BASE_PATH", "/app/data")
    return os.path.join(base_path, "trading.sqlite3")

async def execute_query(query: str, params: tuple = None) -> None:
    """Execute SQL query without returning results"""
    db_path = get_db_path()
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            conn.commit()
    except sqlite3.Error as e:
        log.error(f"SQLite execute error: {e}")

async def fetch_query(query: str, params: tuple = None) -> List[Dict[str, Any]]:
    """Execute SQL query and return results"""
    db_path = get_db_path()
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e:
        log.error(f"SQLite fetch error: {e}")
        return []