#!/usr/bin/python3

# built ins
import asyncio
from asyncio import Queue

# installed
import uvloop
import redis.asyncio as aioredis

from configuration import config, config_oci
from restful_api.deribit import end_point_params_template
from receiver import deribit_ws as receiver_deribit
from shared import  error_handling,string_modification as str_mod,system_tools,template

async def main():
    """
    """

    exchange = "deribit"
    
    sub_account_id = "deribit-148510"

    # registering strategy config file    
    file_toml = "config_strategies.toml"
        
    try:
        
        config_path = system_tools.provide_path_for_file(".env")
        
        parsed= config.main_dotenv(
            sub_account_id,
            config_path,
        )
                
        client_id: str = parsed["client_id"]
        client_secret: str = config_oci.get_oci_key(parsed["key_ocid"])

        #instantiate private connection
        api_request: object = end_point_params_template.SendApiRequest(client_id,client_secret)

        pool = aioredis.ConnectionPool.from_url(
            "redis://localhost", 
            port=6379, 
            db=0, 
            protocol=3, 
            encoding="utf-8",
            decode_responses=True
            )
        
        client_redis: object = aioredis.Redis.from_pool(pool)
        
        # parsing config file
        config_app = system_tools.get_config_tomli(file_toml)

        # get tradable strategies
        tradable_config_app = config_app["tradable"]

        # get TRADABLE currencies
        currencies: list = [o["spot"] for o in tradable_config_app][0]

        strategy_attributes = config_app["strategies"]

        # get redis channels
        redis_channels: dict = config_app["redis_channels"][0]

        settlement_periods = str_mod.remove_redundant_elements(
        str_mod.remove_double_brackets_in_list(
            [o["settlement_period"] for o in strategy_attributes]
        )
    )
        
        futures_instruments = await get_instrument_summary.get_futures_instruments(
            currencies,
            settlement_periods,
        )
                
        redis_keys: dict = config_app["redis_keys"][0]
        
        resolutions: list = [o["resolutions"] for o in tradable_config_app][0]

        strategy_attributes = config_app["strategies"]

        queue = Queue(maxsize=1)
        
        stream = receiver_deribit.StreamingAccountData(sub_account_id,
                                                       client_id,
                                                       client_secret)

        result_template = template.redis_message_template()
        
        sub_account_cached_channel: str = redis_channels["sub_account_cache_updating"]
        
        # sub_account_combining        
        sub_accounts = [await api_request.get_subaccounts_details(o) for o in currencies]
        
        initial_data_subaccount = starter.sub_account_combining(
            sub_accounts,
            sub_account_cached_channel,
            result_template,
        )
        
        producer_task = asyncio.create_task(
            stream.ws_manager(
                client_redis,
                exchange,
                queue,
                futures_instruments,
                resolutions,
)
            ) 
                
                 
        saving_task_deribit = asyncio.create_task(
            distr_deribit.caching_distributing_data(
                client_redis,
                currencies,
                initial_data_subaccount,
                redis_channels,
                redis_keys,
                strategy_attributes,
                queue,
                )
            ) 
                
                        
        await asyncio.sleep(0.0005)
        
        await asyncio.gather(
            
            producer_task, 
            
            saving_task_deribit,

                        )  

        await queue.join()

    except Exception as error:
        await error_handling.parse_error_message_with_redis(
            client_redis,
            error,
            )

if __name__ == "__main__":
    
    try:
        
        uvloop.run(main())
        
    except(
        KeyboardInterrupt, 
        SystemExit
        ):
        
        asyncio.get_event_loop().run_until_complete(main())
        
    except Exception as error:
        
        error_handling.parse_error_message(error)
        