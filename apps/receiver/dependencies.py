from shared.config.exchange_loader import load_exchange_config
from streaming_helper.exchanges.deribit import StreamingAccountData

class ExchangeDependencies:
    def __init__(self, exchange_name):
        self.exchange_name = exchange_name
        self.config = load_exchange_config(exchange_name)
        self.handler = None
        
    async def connect(self):
        if self.exchange_name == "deribit":
            self.handler = StreamingAccountData(
                client_id=self.config['auth']['client_id'],
                client_secret=self.config['auth']['client_secret']
            )
            await self.handler.initialize()