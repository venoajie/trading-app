# core\security.py

import os
from dotenv import load_dotenv

# from pydantic import SecretStr

load_dotenv()


def get_secret(secret_name: str) -> str:
    # 1. Check Docker secrets first
    secret_path = f"/run/secrets/{secret_name}"
    if os.path.exists(secret_path):
        with open(secret_path) as f:
            return f.read().strip()
            # return SecretStr(f.read().strip())

    # 2. Check environment variables
    if value := os.getenv(secret_name.upper()):
        # return SecretStr(value)
        return value

    if value := os.getenv(secret_name.upper().replace("-", "_")):
        # return SecretStr(value)
        return value

    raise ValueError(f"Missing secret: {secret_name}")
