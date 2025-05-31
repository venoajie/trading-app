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
SECURITY_BLOCKED_SCANNERS = get_setting("security.blocked_scanners", ["censysinspect"])
SECURITY_RATE_LIMIT = int(get_setting("security.rate_limit", 100))
SECURITY_HEADERS = {
    "X-Frame-Options": get_setting("headers.X-Frame-Options", "DENY"),
    "X-Content-Type-Options": get_setting("headers.X-Content-Type-Options", "nosniff"),
    "Content-Security-Policy": get_setting("headers.Content-Security-Policy", "default-src 'self'"),
    "Referrer-Policy": get_setting("headers.Referrer-Policy", "no-referrer"),
    "Strict-Transport-Security": get_setting("headers.Strict-Transport-Security", "max-age=63072000; includeSubDomains")
}