# Trading Application Deployment Guide

## Prerequisites
1. Install Docker and Docker Compose
2. Create a Deribit API account to get credentials

## Step 1: Set Environment Variables
Run these commands in your terminal **before deployment**:

```bash
# Deribit API credentials
export DERIBIT_CLIENT_ID="your_deribit_client_id"
export DERIBIT_CLIENT_SECRET="your_deribit_client_secret"

# PostgreSQL password (min 8 characters)
export DB_PASSWORD="your_secure_database_password"

# Optional: Set timezone (default UTC)
export TZ="UTC"


Step 1: Secure Secrets Initialization
Run the deployment script to create encrypted secrets:
bash
# 1. Make sure environment variables are set
export DERIBIT_CLIENT_ID="your_id"
export DERIBIT_CLIENT_SECRET="your_secret"
export DB_PASSWORD="your_db_password"  # Add this if missing

# 2. Create the db_password secret
mkdir -p ~/.app_secrets
echo "$DB_PASSWORD" > ~/.app_secrets/db_password.txt
chmod 600 ~/.app_secrets/*.txt

# 3. Start fresh
./deploy.sh
Create secrets file:

bash
echo "secure_password" > secrets/db_password.txt
chmod 600 secrets/db_password.txt

echo "your_secure_password" > secrets/db_password.txt
chmod 600 secrets/db_password.txt

bash
chmod +x deploy.sh  # Make executable
./deploy.sh         # Creates ~/.app_secrets with 700 permissions

Step 2: Database Setup
Initialize PostgreSQL with init.sql:

bash
docker compose up -d postgres  # Starts PostgreSQL first
Verify database health:

bash
docker compose logs postgres | grep "database system is ready"

Step 3: Build & Launch Services
bash
docker compose up -d --build  # Builds receiver, executor, Redis, PostgreSQL


latest folder structure:

[opc@instance-20250523-1627 trading-app]$ tree
.
├── config
│   ├── __init__.py
│   ├── settings.py
│   └── strategies.toml
├── core
│   ├── db
│   │   ├── __init__.py
│   │   ├── postgres.py
│   │   ├── redis.py
│   │   └── sqlite.py
│   ├── error_handler.py
│   ├── __init__.py
│   ├── security.py
│   └── service_manager.py
├── data
│   ├── databases
│   │   └── general
│   │       ├── btc-13jun25
│   │       ├── btc-25jul25
│   │       ├── btc-26dec25
│   │       ├── btc-26sep25
│   │       ├── btc-27jun25
│   │       ├── btc-27mar26
│   │       ├── btc-6jun25
│   │       ├── btc-fs-13jun25_perp
│   │       ├── btc-fs-25jul25_perp
│   │       ├── btc-fs-26dec25_perp
│   │       ├── btc-fs-26sep25_perp
│   │       ├── btc-fs-27jun25_perp
│   │       ├── btc-fs-27mar26_perp
│   │       ├── btc-fs-6jun25_perp
│   │       ├── btc-perpetual
│   │       ├── eth-13jun25
│   │       ├── eth-25jul25
│   │       ├── eth-26dec25
│   │       ├── eth-26sep25
│   │       ├── eth-27jun25
│   │       ├── eth-27mar26
│   │       ├── eth-6jun25
│   │       ├── eth-fs-13jun25_perp
│   │       ├── eth-fs-25jul25_perp
│   │       ├── eth-fs-26dec25_perp
│   │       ├── eth-fs-26sep25_perp
│   │       ├── eth-fs-27jun25_perp
│   │       ├── eth-fs-27mar26_perp
│   │       ├── eth-fs-6jun25_perp
│   │       └── eth-perpetual
│   └── trading.sqlite3
├── deploy.sh
├── docker-compose.yml
├── init.sql
├── PROJECT_BLUEPRINT.md
├── README.md
├── secrets -> /home/opc/.app_secrets
├── src
│   ├── scripts
│   │   ├── app_relayer
│   │   ├── app_scalping
│   │   ├── app_transactions
│   │   ├── deribit
│   │   │   ├── caching.py
│   │   │   ├── get_instrument_summary.py
│   │   │   ├── get_published_messages.py
│   │   │   ├── __init__.py
│   │   │   ├── restful_api
│   │   │   │   ├── connector.py
│   │   │   │   ├── end_point_params_template.py
│   │   │   │   └── __init__.py
│   │   │   ├── risk_management
│   │   │   │   └── __init__.py
│   │   │   ├── starter.py
│   │   │   ├── strategies
│   │   │   │   ├── basic_strategy.py
│   │   │   │   ├── cash_carry
│   │   │   │   │   ├── combo_auto.py
│   │   │   │   │   ├── futures_spread.py
│   │   │   │   │   ├── __init__.py
│   │   │   │   │   └── reassigning_labels.py
│   │   │   │   ├── deribit
│   │   │   │   │   ├── app_future_spreads.py
│   │   │   │   │   ├── app_hedging_spot.py
│   │   │   │   │   └── __init__.py
│   │   │   │   ├── hedging
│   │   │   │   │   ├── hedging_spot.py
│   │   │   │   │   └── __init__.py
│   │   │   │   ├── __init__.py
│   │   │   │   └── relabelling_trading_result.py
│   │   │   └── subscribing_to_channels.py
│   │   ├── __init__.py
│   │   ├── market_understanding
│   │   │   ├── __init__.py
│   │   │   └── price_action
│   │   │       ├── candles_analysis.py
│   │   │       └── __init__.py
│   │   └── telegram
│   │       ├── connector.py
│   │       ├── end_point_params_template.py
│   │       └── __init__.py
│   ├── services
│   │   ├── cleaner
│   │   │   ├── app_data_cleaning.py
│   │   │   ├── __init__.py
│   │   │   ├── managing_closed_transactions.py
│   │   │   ├── managing_delivered_transactions.py
│   │   │   ├── pruning_db.py
│   │   │   └── reconciling_db.py
│   │   ├── executor
│   │   │   ├── deribit
│   │   │   │   ├── cancelling_active_orders.py
│   │   │   │   ├── __init__.py
│   │   │   │   ├── main.py
│   │   │   │   └── processing_orders.py
│   │   │   ├── Dockerfile
│   │   │   ├── __init__.py
│   │   │   └── pyproject.toml
│   │   ├── __init__.py
│   │   └── receiver
│   │       ├── deribit
│   │       │   ├── allocating_ohlc.py
│   │       │   ├── deribit_ws.py
│   │       │   ├── distributing_ws_data.py
│   │       │   ├── __init__.py
│   │       │   └── main.py
│   │       ├── Dockerfile
│   │       ├── health_check.py
│   │       ├── __init__.py
│   │       └── pyproject.toml
│   └── shared
│       ├── config
│       │   ├── __init__.py
│       │   └── settings.py
│       ├── __init__.py
│       ├── logging.py
│       ├── pyproject.toml
│       └── utils
│           ├── error_handling.py
│           ├── __init__.py
│           ├── pickling.py
│           ├── string_modification.py
│           ├── system_tools.py
│           ├── template.py
│           └── time_modification.py
└── tests
    ├── integration
    │   └── __init__.py
    └── unit
        └── conftest.py

61 directories, 86 files