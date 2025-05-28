# trading-app

trading-system/
├── apps/
│   ├── receiver/trading-app/
├── apps/
│   ├── distributor/
│   │   ├── Dockerfile            # Distributor service Dockerfile
│   │   ├── dependencies.py       # Dependency injection setup
│   │   └── main.py               # Distributor entry point
│   ├── processor/
│   │   ├── Dockerfile            # Processor service Dockerfile
│   │   └── main.py               # Processor entry point
│   └── receiver/
│       ├── Dockerfile            # Receiver service Dockerfile
│       ├── dependencies.py       # Exchange connection setup
│       └── main.py               # Receiver entry point
├── infrastructure/
│   └── docker-compose.yml        # Full service orchestration
├── pyproject.toml                # Project dependencies (UV)
├── scripts/
│   ├── db_backup.sh              # Database backup script
│   └── user_setup.py             # User initialization script
├── shared/
│   ├── config/
│   │   ├── exchange_loader.py    # Exchange config loader
│   │   ├── exchanges/
│   │   │   ├── deribit.toml      # Deribit config
│   │   │   └── binance.toml      # Binance config
│   │   ├── settings.py           # Pydantic settings
│   │   └── strategy_loader.py    # Strategy config loader
│   └── lib/
│       ├── database.py           # PostgreSQL connection
│       ├── redis_manager.py      # Redis connection
│       └── models.py             # Pydantic/SQLAlchemy models
├── streaming_helper/             # Existing code (unchanged)
│   ├── exchanges/
│   │   ├── deribit/
│   │   │   └── deribit.py        # Deribit implementation
│   │   └── binance/
│   │       └── binance.py        # Binance implementation
│   └── utilities/
│       └── ...                   # Utility modules
└── tests/
    └── unit/
        ├── test_receiver.py
        └── test_distributor.py
│   │   ├── Dockerfile
│   │   └── main.py
│   ├── distributor/
│   ├── processor/
│   └── general_app/
├── shared/
│   ├── lib/
│   │   ├── time_utils.py
│   │   ├── data_models.py (Pydantic)
│   │   └── cache.py
│   ├── config/
│   │   ├── settings.toml
│   │   └── __init__.py
│   └── __init__.py
├── tests/
│   ├── unit/
│   └── conftest.py
├── pyproject.toml
├── docker-compose.yml
├── .env.template
└── .dockerignore