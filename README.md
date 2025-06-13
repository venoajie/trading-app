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

## Docker Management Commands

### Start Services
```bash
# Full system (all services)
docker compose --profile full up -d

# Only receiver + Redis
docker compose --profile receiver up -d

# Only distributor + Redis + Postgres
docker compose --profile distributor up -d
```

### Stop Services
```bash
# Stop services but preserve data
docker compose down

# Stop and destroy all data volumes
docker compose down -v
```

### Update Services
```bash
# 1. Pull latest code
git pull

# 2. Update single service
docker compose up -d --build --force-recreate receiver

# 3. Full system update
docker compose --profile full up -d --build --pull always
```

### View Logs
```bash
# Receiver logs
docker compose logs -f receiver

# Distributor logs
docker compose logs -f distributor

# Redis logs
docker compose logs -f redis
```

### Cleanup System
```bash
# Full cleanup (use before major updates)
docker compose down -v --remove-orphans
docker system prune -a --volumes --force
docker network prune -f
```


Decorators for Automatic Handling:
@error_handler.wrap_async
async def critical_async_task():
    # ... code that might fail ...

@error_handler.wrap_sync
def critical_sync_operation():
    # ... code that might fail ...


Basic error capture:

python
try:
    risky_operation()
except Exception as e:
    await error_handler.capture(
        e,
        context="Data processing",
        metadata={"file": "data.csv", "size": "128MB"},
        severity="CRITICAL"
    )


    Event reporting (non-error):

python
async def order_executed(order_details):
    await error_handler.capture(
        Exception("Order executed"),  # Fake exception for structure
        context="Trade execution",
        metadata=order_details,
        severity="INFO"
    )

Automatic service monitoring:

python
async def health_check():
    if not await check_service_health():
        await error_handler.capture(
            Exception("Service unhealthy"),
            context="Health check failed",
            severity="WARNING"
        )

to supress overcommit memory:
sudo sysctl vm.overcommit_memory=1
echo "vm.overcommit_memory=1" | sudo tee -a /etc/sysctl.conf


Running in Background:

bash
docker compose --profile receiver up --build -d


Verify with:

bash
docker compose ps

Managing Background Services:

View logs:

bash
docker compose logs -f
Stop services:

bash
docker compose --profile receiver down
