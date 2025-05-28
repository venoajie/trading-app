# trading-app

trading-system/
├── apps/
│   ├── receiver/
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