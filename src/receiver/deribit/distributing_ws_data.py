# Optimized distributing_ws_data.py
"""Enhanced data distribution service with robust maintenance handling"""

import asyncio
import logging
from typing import Dict, List, Any, Optional

# Third-party imports
import redis.asyncio as aioredis
from redis.asyncio.client import PubSub 

# Application imports
from shared.db import redis as redis_publish
from shared.db import sqlite as db_mgt
from restful_api.deribit import end_point_params_template as end_point
from receiver.deribit import get_instrument_summary, allocating_ohlc
from shared.utils import (
    caching, 
    error_handling, 
    pickling,
    string_modification as str_mod, 
    system_tools, 
    template
)

# Configure logger
log = logging.getLogger(__name__)

class DataDistributor:
    """Main data distribution handler with maintenance awareness"""
    
    def __init__(self):
        self._maintenance_mode = False
        self._last_maintenance_notification = 0

    async def caching_distributing_data(
        self,
        client_redis: aioredis.Redis,
        currencies: List[str],
        initial_data_subaccount: Dict,
        redis_channels: Dict,
        redis_keys: Dict,
        strategy_attributes: List,
        queue_general: asyncio.Queue,
    ) -> None:
        """Distributes WebSocket data with enhanced maintenance handling"""
        try:
            # Initialize state
            pubsub = client_redis.pubsub()
            await self._setup_subscriptions(pubsub, redis_channels)
            
            # Start maintenance handler
            asyncio.create_task(self._maintenance_monitor(pubsub, queue_general))
            
            # Prepare initial data
            server_time = 0
            portfolio = []
            settlement_periods = self._get_settlement_period(strategy_attributes)
            
            futures_instruments = await get_instrument_summary.get_futures_instruments(
                currencies, settlement_periods
            )
            instruments_name = futures_instruments["instruments_name"]
            ticker_all_cached = await self._combining_ticker_data(instruments_name)
            
            # Initialize account data
            sub_account_data = initial_data_subaccount["params"]["data"]
            orders_cached = sub_account_data["orders_cached"]
            positions_cached = sub_account_data["positions_cached"]
            query_trades = "SELECT * FROM v_trading_all_active"
            result_template = template.redis_message_template()

            # Main processing loop
            while True:
                if self._maintenance_mode:
                    await asyncio.sleep(1)
                    continue
                    
                message_params = await queue_general.get()
                await self._process_message(
                    client_redis,
                    message_params,
                    redis_channels,
                    orders_cached,
                    positions_cached,
                    query_trades,
                    result_template,
                    ticker_all_cached,
                    server_time,
                    portfolio
                )

        except Exception as error:
            await error_handling.parse_error_message_with_redis(client_redis, error)
            raise

    async def _setup_subscriptions(self, pubsub: PubSub, redis_channels: Dict) -> None:
        """Subscribe to necessary Redis channels"""
        await pubsub.subscribe("system_status")
        for channel in [redis_channels["order_cache_updating"], 
                       redis_channels["sqlite_record_updating"]]:
            await pubsub.subscribe(channel)

    async def _maintenance_monitor(self, pubsub: aioredis.PubSub, queue: asyncio.Queue) -> None:
        """Handle maintenance mode events"""
        async for message in pubsub.listen():
            if message["type"] == "message":
                status = message["data"].decode()
                current_time = time.time()
                
                if status == "maintenance" and not self._maintenance_mode:
                    self._maintenance_mode = True
                    log.warning("Entering maintenance handling mode")
                    await self._clear_queue(queue)
                elif status == "operational" and self._maintenance_mode:
                    self._maintenance_mode = False
                    log.info("Exiting maintenance mode")

    async def _clear_queue(self, queue: asyncio.Queue) -> None:
        """Safely clear the processing queue"""
        try:
            while not queue.empty():
                queue.get_nowait()
                queue.task_done()
        except Exception as e:
            log.error(f"Error clearing queue: {e}")

    async def _process_message(
        self,
        client_redis: aioredis.Redis,
        message_params: Dict,
        redis_channels: Dict,
        orders_cached: List,
        positions_cached: List,
        query_trades: str,
        result_template: Dict,
        ticker_all_cached: List,
        server_time: int,
        portfolio: List
    ) -> None:
        """Process individual message with error handling"""
        async with client_redis.pipeline() as pipe:
            try:
                data = message_params["data"]
                message_channel = message_params["channel"]
                currency = str_mod.extract_currency_from_text(message_channel)
                currency_upper = currency.upper()

                pub_message = {
                    "data": data,
                    "server_time": server_time,
                    "currency_upper": currency_upper,
                    "currency": currency,
                }

                if "user." in message_channel:
                    await self._process_user_message(
                        pipe,
                        message_channel,
                        data,
                        redis_channels,
                        orders_cached,
                        positions_cached,
                        query_trades,
                        result_template,
                        portfolio,
                        pub_message
                    )
                elif message_channel.startswith("incremental_ticker."):
                    await self._process_ticker_message(
                        pipe,
                        currency,
                        data,
                        message_channel,
                        result_template,
                        pub_message,
                        server_time,
                        ticker_all_cached,
                        redis_channels["ticker_cache_updating"]
                    )
                elif "chart.trades" in message_channel:
                    await self._process_chart_message(
                        pipe,
                        redis_channels["chart_low_high_tick"],
                        message_channel,
                        pub_message,
                        result_template
                    )

                await pipe.execute()
            except Exception as error:
                log.error(f"Error processing message: {error}")
                await error_handling.parse_error_message_with_redis(client_redis, error)

    # ... (other helper methods remain largely the same but with error handling)

    @staticmethod
    def _get_settlement_period(strategy_attributes: List) -> List:
        """Extract settlement periods from strategy config"""
        return str_mod.remove_redundant_elements(
            str_mod.remove_double_brackets_in_list(
                [o["settlement_period"] for o in strategy_attributes]
            )
        )

    @staticmethod
    async def _combining_ticker_data(instruments_name: List) -> List:
        """Combine ticker data from cache or API with fallback"""
        result = []
        for instrument_name in instruments_name:
            try:
                cached_data = pickling.read_data(
                    system_tools.provide_path_for_file("ticker", instrument_name)
                )
                result.append(cached_data[0] if cached_data else await end_point.get_ticker(instrument_name))
            except Exception as e:
                log.error(f"Error loading ticker data for {instrument_name}: {e}")
                result.append({})
        return result

# Maintain existing utility functions (compute_notional_value, get_index, etc.)
# with added error handling and logging