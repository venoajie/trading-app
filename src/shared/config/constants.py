# src/shared/config/constants.py
class ServiceConstants:
    # Redis keys
    REDIS_STREAM_MARKET = "stream:market_data"
    REDIS_GROUP_DISPATCHER = "dispatcher_group"
    
    # Database
    DB_BASE_PATH = "/app/data"
    
    # Tables
    ORDERS_TABLE = "orders"
    OHLC_TABLE_PREFIX = "ohlc"
    
    # Error handling
    ERROR_TELEGRAM_ENABLED = True
    ERROR_REDIS_ENABLED = True

class ExchangeConstants:
    DERIBIT = "deribit"
    BINANCE = "binance"
    SUPPORTED_EXCHANGES = [DERIBIT, BINANCE]

# Usage example: 
# from src.shared.config.constants import ServiceConstants
# ServiceConstants.REDIS_STREAM_MARKET