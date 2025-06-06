# src\services\receiver\deribit\main.py

"""Optimized core application entry point with enhanced maintenance handling"""
import os
import asyncio
from asyncio import Queue

# Third-party imports
import uvloop
import orjson
import logging
from loguru import logger as log
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

# Application imports
from core.db.redis import redis_client as global_redis_client
from core.security import get_secret
from src.scripts.deribit import get_instrument_summary, starter
from src.scripts.deribit.restful_api import end_point_params_template
from src.services.receiver.deribit import deribit_ws, distributing_ws_data
from src.shared.config.settings import (
    REDIS_URL, REDIS_DB,
    DERIBIT_SUBACCOUNT, DERIBIT_CURRENCIES,
    DERIBIT_MAINTENANCE_THRESHOLD, DERIBIT_HEARTBEAT_INTERVAL
)
from src.shared.utils import error_handling, system_tools, template



async def run_services() -> None:
    log.info("Starting service orchestration")
    
    try:
        while True:
            if app_state.maintenance_mode:
                log.warning("Application in maintenance mode - waiting")
                await asyncio.sleep(60)
                continue
                
            await trading_main()
            await asyncio.sleep(5)  # Brief pause before restarting
            
    except (KeyboardInterrupt, SystemExit):
        log.info("Service shutdown requested")
    except Exception as error:
        log.exception("Unhandled error in service orchestration")
        await enter_maintenance_mode("Unhandled error in service orchestration")

            
class ApplicationState:
    """Centralized state management with Redis synchronization"""
    def __init__(self):
        self._maintenance_mode = False
        self.connection_active = False
        self.last_successful_connection = 0

    @property
    def maintenance_mode(self):
        return self._maintenance_mode

    @maintenance_mode.setter
    def maintenance_mode(self, value):
        if self._maintenance_mode != value:
            self._maintenance_mode = value
            asyncio.create_task(self.publish_state())

    async def publish_state(self):
        try:
            redis = await global_redis_client.get_pool()
            status = "maintenance" if self._maintenance_mode else "operational"
            await redis.publish("system_status", status)
            log.info(f"Published system status: {status}")
        except Exception as e:
            log.error(f"Failed to publish state: {str(e)}")

app_state = ApplicationState()

async def enter_maintenance_mode(reason: str):
    if not app_state.maintenance_mode:
        log.critical(f"Entering maintenance mode: {reason}")
        app_state.maintenance_mode = True

async def exit_maintenance_mode():
    if app_state.maintenance_mode:
        log.info("Exiting maintenance mode")
        app_state.maintenance_mode = False

async def monitor_system_alerts():
    try:
        redis = await global_redis_client.get_pool()
        pubsub = redis.pubsub()
        await pubsub.subscribe("system_alerts")
        
        async for message in pubsub.listen():
            if message["type"] == "message":
                try:
                    alert = orjson.loads(message["data"])
                    if alert["event"] == "heartbeat_timeout":
                        await enter_maintenance_mode(alert["reason"])
                    elif alert["event"] == "heartbeat_resumed":
                        await exit_maintenance_mode()
                except orjson.JSONDecodeError:
                    log.error("Invalid alert format")
    except Exception as e:
        log.error(f"Alert monitoring failed: {str(e)}")
        await enter_maintenance_mode("Alert system failure")

async def setup_redis():
    max_retries = 3
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            pool = await global_redis_client.get_pool()
            if await pool.ping():
                log.info("Redis connection validated")
                return pool
        except Exception as e:
            log.warning(f"Redis connection attempt {attempt + 1} failed: {str(e)}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
    
    log.critical("All Redis connection attempts failed")
    raise ConnectionError("Redis connection failed after retries")


async def verify_permissions():
    """Verify critical paths have correct permissions"""
    from src.shared.config.settings import DB_BASE_PATH
    from pathlib import Path
    import sqlite3
    
    required_paths = [
        Path(DB_BASE_PATH),
    ]
    
    for path in required_paths:
        path.mkdir(parents=True, exist_ok=True)
        
        if not os.access(path, os.W_OK):
            log.critical(f"Permission denied: {path}")
            await enter_maintenance_mode(f"Permission issue: {path}")
            return False
    
    # Test database write
    try:
        db_path = Path(DB_BASE_PATH) / 'trading.sqlite3'
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE IF NOT EXISTS permission_test (id INTEGER);")
        conn.execute("INSERT INTO permission_test VALUES (1);")
        conn.commit()
        conn.execute("DROP TABLE permission_test;")
        conn.close()
        return True
    except Exception as e:
        log.critical(f"Database write test failed: {str(e)}")
        await enter_maintenance_mode("Database write failure")
        return False

async def trading_main() -> None:
    log.info("Initializing trading system")
    
    if not await verify_permissions():
        return
    
    try:
        client_redis = await setup_redis()
        alert_monitor_task = asyncio.create_task(monitor_system_alerts())
    except ConnectionError:
        await enter_maintenance_mode("Redis connection failed")
        return
    
    exchange = "deribit"
    config_path = "/app/config/strategies.toml"
    
    try:
            
        # Load secrets securely
        client_id = get_secret("deribit_client_id")
        client_secret = get_secret("deribit_client_secret")
        
        if not client_id or not client_secret:
            await enter_maintenance_mode("Deribit credentials not configured")
            return
        
        api_request = end_point_params_template.SendApiRequest(client_id, client_secret)
        currencies = DERIBIT_CURRENCIES
        resolutions = [1, 5, 15, 60]
        
        futures_instruments = await get_instrument_summary.get_futures_instruments(
            currencies, ["perpetual"]
        )
        
        data_queue = Queue(maxsize=1000)
        
        stream = deribit_ws.StreamingAccountData(
            sub_account_id=DERIBIT_SUBACCOUNT,
            client_id=client_id,
            client_secret=client_secret,
            reconnect_base_delay=5,
            max_reconnect_delay=300,
            maintenance_threshold=DERIBIT_MAINTENANCE_THRESHOLD,
            websocket_timeout=900,
            heartbeat_interval=DERIBIT_HEARTBEAT_INTERVAL
        )
        
        config_app = system_tools.get_config_tomli(config_path)
        redis_channels = config_app.get("redis_channels", [{}])[0]
        redis_keys = config_app.get("redis_keys", [{}])[0]
        strategy_config = config_app.get("strategies", [])

        sub_account_cached_channel = redis_channels.get("sub_account_cache_updating", "default_channel")
        
        sub_accounts = []
        for currency in currencies:
            try:
                account_data = await api_request.get_subaccounts_details(currency)
                sub_accounts.append(account_data)
            except Exception as e:
                log.error(f"Failed to get subaccount for {currency}: {str(e)}")
                continue
        
        result_template = template.redis_message_template()
        initial_data_subaccount = starter.sub_account_combining(
            sub_accounts,
            sub_account_cached_channel,
            result_template,
        )
        
        producer_task = asyncio.create_task(
            stream.manage_connection(
                client_redis,
                exchange,
                data_queue,
                futures_instruments,
                resolutions
            )
        )
        
        distributor_task = asyncio.create_task(
            distributing_ws_data.caching_distributing_data(
                client_redis,
                currencies,
                initial_data_subaccount,
                redis_channels,
                redis_keys,
                strategy_config,
                data_queue
            )
        )
        
        # Main monitoring loop
        while True:
            if app_state.maintenance_mode:
                # Pause all processing during maintenance
                await asyncio.sleep(60)
            else:
                await asyncio.sleep(5)
                
    except Exception as error:
        log.exception("Critical error in trading system")
        await enter_maintenance_mode(f"Critical error: {str(error)}")
        
    finally:
        # Clean up tasks
        if 'alert_monitor_task' in locals():
            alert_monitor_task.cancel()
            try:
                await alert_monitor_task
            except asyncio.CancelledError:
                log.info("Alert monitoring task cancelled")

        # Cancel producer and distributor tasks
        for task in [producer_task, distributor_task]:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    log.info("Background task cancelled")


async def main():
    register_services()
    await service_manager.start_all()
    
    # Keep main running until shutdown
    try:
        await asyncio.Future()
    except (KeyboardInterrupt, SystemExit):
        await service_manager.stop_all()
        
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    try:
        log.info("Starting application")
        uvloop.run(run_services())
    except Exception as error:
        log.exception("Fatal error during application startup")
        raise SystemExit(1)