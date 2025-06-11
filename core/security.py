"""
core/security.py
"""

import os
from dotenv import load_dotenv  # Add python-dotenv to requirements

load_dotenv()  # Load .env file if present

def get_secret(secret_name: str) -> str:
    """Retrieve secrets in priority order: 
    1. Environment variables
    2. Docker secrets
    3. .env file
    """
    # Check environment variables
    if value := os.getenv(secret_name.upper()):
        return value
    
    # Check Docker secrets (if running in container)
    secret_path = f"/run/secrets/{secret_name}"
    if os.path.exists(secret_path):
        with open(secret_path) as f:
            return f.read().strip()
    
    # Fallback to .env file
    return os.getenv(secret_name.upper(), "")