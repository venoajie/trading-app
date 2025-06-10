# src/core/security.py
import os

def get_secret(secret_name: str) -> str:
    """Load Docker secrets securely"""
    try:
        # Try Docker secrets path first
        with open(f'/run/secrets/{secret_name}', 'r') as secret_file:
            return secret_file.read().strip()
    except IOError:
        try:
            # Fallback to environment variables
            return os.environ[secret_name.upper()]
        except KeyError as e:
            raise RuntimeError(f"Critical secret missing: {secret_name}")