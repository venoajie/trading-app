"""
shared/config/settings.py
Centralized application settings
"""

from .loader import get_setting

# Redis settings
REDIS_URL = get_setting("redis.url", "redis://localhost:6379")
REDIS_DB = int(get_setting("redis.db", 0))

# Deribit settings
DERIBIT_SUBACCOUNT = get_setting("deribit.subaccount", "deribit-148510")
DERIBIT_CURRENCIES = get_setting("deribit.currencies", ["BTC", "ETH"])

# Security settings
BLOCKED_SCANNERS = get_setting("security.blocked_scanners", ["censysinspect"])