# MY TRADIND APP PROJECT

## CORE ARCHITECTURE
- **Database**: active: sqlite/pickle. will be migrate to PostgreSQL as expected to retain many api secrets
- **Message broker**: Redis
- **Platform**: Docker (just migrated to docker. previously deployed using regular approach in linux)
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
## CURRENT PHASE (2025-06-02)
- receiver basic functionalities has completed.just implemented work around for idle because of maintenance. seems work but have not tested it yet.
- running receiver in docker. still get error message for permission

## NEXT PHASE ()
receiver. need add additional things:
- docker optimization (speed, size, security. replace requirements.txt with pyproject.toml)
- unit testing
- how communicate failures
- listing possible alternatives (speed vs easy maintenance vs technology)
- improve documentation and familiarize my self with the code

other services
- copying everything at receiver for other services


# MEDIUM TERM
- migrate to postgresql
- implement threading (to process data from exchanges)
- code optimization

# LONG TERM
- add another exchanges (currently binance and deribit)
- invites others to use the service

