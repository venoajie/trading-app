# MY TRADIND APP PROJECT

## CORE ARCHITECTURE
- **Database**: active: sqlite/pickle. on going migrate to PostgreSQL as expected to retain many api secrets
- **Message broker**: Redis
- **Back up**: database backed up at backblaze using inotify
- **Key Services**:
  - `receiver` () - receive data from exchanges through websockets 
  cores involved: redis stream (redis)
  - `distributor` (v0) - distributor received the data from receiver, manipulate them, and further dispatched to processor using redis
    cores involved: redis & postgresql
  - `processor` (v) - Processing events (order/cancel/transactions) and number (ohlc/balance)
  - `shared` (v) - Shared library: text/numbers/time/db manipulation
  - `general` (v) - Db maintenance/back up, communicating events by telegram

## KNOWN CONCERNS
- failed message: in heavy backlog/hang situation, there is possibility events failed to reach the services
- ram size is gradually improving although the data volume is fixed
  

## CURRENT PHASE (2025-06-10)
- testing and optimized/collaborated receiver and distributor services
- migrate sqlite syntax to postgresql

CURRENT PHASE CONCERN:

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
need add additional things:
- unit testing, partially done
- how communicate failures: ignored now
- improve documentation and familiarize my self with the code


# MEDIUM TERM
- migrate to postgresql
- implement threading (to process data from exchanges)
- code optimization
- Implement proper task cancellation
- Create health check endpoint



# LONG TERM
- Implement circuit breakers for Redis
- add another exchanges (currently binance and deribit)
- invites others to use the service

