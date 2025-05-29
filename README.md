trading_system/
├── .env                          # Base env vars (overridden by OCI Vault in prod)
├── docker-compose.yml            # Multi-container orchestration
│
├── shared/                       # Shared library (built as wheel)
│   ├── pyproject.toml            # Shared dependencies: pydantic, numpy, etc.
│   ├── src/
│   │   ├── config_loader.py      # TOML + env loader
│   │   ├── time_utils.py         # Timezone handling
│   │   ├── data_models.py        # Pydantic schemas
│   │   ├── formulas.py           # OHLC calculations
│   │   └── cache.py              # LRU cache implementation
│   └── tests/
│
├── receiver/
│   ├── pyproject.toml            # Specific: websockets, aioredis
│   ├── src/
│   │   ├── deribit_ws.py         # Exchange-specific handlers
│   │   ├── binance_ws.py
│   │   └── main.py               # Entrypoint
│   ├── Dockerfile                # Multi-stage: shared wheel + receiver deps
│   └── tests/
│
├── distributor/
│   ├── pyproject.toml            # Specific: aioredis, msgpack
│   ├── src/
│   │   ├── redis_handler.py      # Async Redis client
│   │   ├── data_transformer.py   # Normalization logic
│   │   └── main.py
│   ├── Dockerfile
│   └── tests/
│
├── processor/
│   ├── pyproject.toml            # Specific: asyncpg, sqlalchemy
│   ├── src/
│   │   ├── db/
│   │   │   ├── postgres.py       # Async DB client
│   │   │   └── models.py         # SQLAlchemy ORM
│   │   ├── order_executor.py     # Exchange order API wrappers
│   │   └── main.py
│   ├── Dockerfile
│   └── tests/
│
├── maintenance/
│   ├── pyproject.toml            # Specific: rclone, apscheduler
│   ├── src/
│   │   ├── backup.py             # Cloud sync logic
│   │   └── db_cleaner.py         # DB optimization
│   ├── Dockerfile
│   └── tests/
│
├── communicator/
│   ├── pyproject.toml            # Specific: python-telegram-bot
│   ├── src/
│   │   └── telegram_bot.py       # Alert dispatcher
│   ├── Dockerfile
│   └── tests/
│
├── config/                       # Centralized configs (mounted to all containers)
│   ├── system.toml
│   ├── exchanges.toml
│   └── users.toml                # User access templates
│
└── scripts/
    ├── init_db.sh                # PostgreSQL setup
    └── build_wheels.sh           # Pre-build shared wheel