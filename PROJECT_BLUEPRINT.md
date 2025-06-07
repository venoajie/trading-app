# MY TRADIND APP PROJECT

## CORE ARCHITECTURE
- **Database**: active: sqlite/pickle. on going migrate to PostgreSQL as expected to retain many api secrets
- **Message broker**: Redis
- **Back up**: database backed up at backblaze using inotify
- **Key Services**:
  - `receivers` () - receive data from exchanges through websockets 
  - `distributors` (v0) - distributor received the data, manipulate them, and further dispatched to processor using redis
  both services above combined into one service 'receivers'
  - `processor` (v) - Processing events (order/cancel/transactions) and number (ohlc/balance)
  - `shared` (v) - Shared library: text/numbers/time/db manipulation
  - `general` (v) - Db maintenance/back up, communicating events by telegram

## KNOWN CONCERNS
- idle state: exchanges will stop sending data ata maintenance situation. this will cause websockets in hanging state.
- decoupling: events/data come in the same time and have to be processed separately or caused backlog
- failed message: in heavy backlog/hang situation, there is possibility events failed to reach the services
- ram size is gradually improving although the data volume is fixed
- data consistency: to reduce request to sqlite, all services retain their own data in cache. this cache should be reconciled time after time
  

# SHORT TERM
- ensure docker migration succesfull
  - receiver (done)

## CURRENT PHASE (2025-06-02)
- receiver basic functionalities has completed. doing some optimizations before next steps
- processorbasic functionalities in deployment stage. still facing some errors due to inconsistent caching 
CURRENT PHASE CONCERN:

main.py is overly complex with mixed concerns. Need to refactor
# services.py (new abstraction)-->seems not needed
class ServiceManager:
    def __init__(self):
        self.services = {}
        
    def register(self, name, coro):
        self.services[name] = coro
        
    async def start_all(self):
        for name, coro in self.services.items():
            asyncio.create_task(coro())

# main.py (simplified)
from services import ServiceManager
manager = ServiceManager()
manager.register("redis_monitor", monitor_system_alerts)
manager.register("trading_main", trading_main)

asyncio.run(manager.start_all())


8. Dead Code Removal
Remove these unused components:

starter.py (replaced by service manager)

sqlite.py (replaced by db.py)

caching.py (functionality exists in utils)

Circular dependencies between redis.py and error_handling.py

we have 2 error handlers, at core and shared

Fix:

python
# In db.py
import os

class Database:
    @staticmethod
    async def get_connection():
        db_path = Config().db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        if not os.path.exists(db_path):
            open(db_path, 'w').close()  # Create empty file
        os.chmod(db_path, 0o660)  # Set permissions
        return await aiosqlite.connect(db_path)

        10. Health Check Enhancement
Problem: Basic Redis health check doesn't verify application state.

Fix:

python
# health.py
from fastapi import APIRouter

router = APIRouter()

@router.get("/health")
async def health_check():
    return {
        "redis": await check_redis(),
        "services": ServiceManager.status()
    }


## NEXT PHASE ()
receiver. need add additional things:
- unit testing
- how communicate failures
- listing possible alternatives (speed vs easy maintenance vs technology)
- improve documentation and familiarize my self with the code


# MEDIUM TERM
- migrate to postgresql
- implement threading (to process data from exchanges)
- code optimization
- Add queue overflow handling
- Implement proper task cancellation
- Create health check endpoint
- Split monolith into microservices
- Implement proper backpressure mechanisms
Redis Backpressure: No backpressure handling in distributing_ws_data.py

Add max queue size and overflow handling



# LONG TERM
- Implement circuit breakers for Redis
- add another exchanges (currently binance and deribit)
- invites others to use the service

