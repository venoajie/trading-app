# apps/receiver/main.py
import importlib

EXCHANGES = {
    "deribit": {
        "config_file": "deribit.toml",
        "handler_module": "deribit"
    },
    "binance": {
        "config_file": "binance.toml", 
        "handler_module": "binance"
    }
}

async def initialize_exchanges():
    for exchange_name, config in EXCHANGES.items():
        # Load config
        exchange_config = load_exchange_config(config["config_file"])
        
        # Load handler dynamically
        module = importlib.import_module(
            f"streaming_helper.exchanges.{config['handler_module']}"
        )
        
        # Initialize exchange adapter
        handler = module.ExchangeHandler(
            config=exchange_config,
            redis=redis_client
        )
        asyncio.create_task(handler.run())