# MY TRADIND APP PROJECT

## CORE ARCHITECTURE
- **Database**: active: sqlite/pickle. on going migrate to PostgreSQL as expected to retain many api secrets
- **Message broker**: Redis
- **Back up**: database backed up at backblaze using inotify
- **Key Services**:
  - `receiver` () - receive data from exchanges through websockets 
  core involved: redis stream (redis)
  - `distributor` (v0) - distributor received the data from receiver, manipulate them, and further dispatched to processor using redis
    cores involved: redis & postgresql
  - `processor` (v) - Processing events (order/cancel/transactions) and number (ohlc/balance)
      cores involved: redis & postgresql
  - `shared` (v) - Shared library: text/numbers/time/db manipulation
  - `general` (v) - Db maintenance/back up, communicating events by telegram

## KNOWN CONCERNS
- failed message: in heavy backlog/hang situation, there is possibility events failed to reach the services
- ram size is gradually improving although the data volume is fixed
  

## CURRENT PHASE (2025-06-11)
- testing and optimized/collaborated receiver and distributor services
- migrate sqlite syntax to postgresql
- introduce pydantic + consolidating config files

## Configuration Schema
| Section | Source | Example |
|---------|--------|---------|
| Redis | ENV+Secrets | `REDIS_URL` |
| PostgreSQL | Secrets | `db_password` |

CURRENT PHASE CONCERN:

8. Dead Code Removal
Remove these unused components:

starter.py (replaced by service manager)

sqlite.py (replaced by db.py)

caching.py (functionality exists in utils)

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

