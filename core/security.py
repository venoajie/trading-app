import os
from dotenv import load_dotenv
from pydantic import SecretStr

load_dotenv()

def get_secret(secret_name: str) -> SecretStr:
    """Retrieve secrets with type safety"""
    # Environment variables
    if value := os.getenv(secret_name.upper()):
        return SecretStr(value)
    
    # Docker secrets
    secret_path = f"/run/secrets/{secret_name}"
    if os.path.exists(secret_path):
        with open(secret_path) as f:
            return SecretStr(f.read().strip())
    
    # .env file
    return SecretStr(os.getenv(secret_name.upper(), ""))