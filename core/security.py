# src/core/security.py
def get_secret(secret_name: str) -> str:
    """Load Docker secrets securely"""
    try:
        with open(f'/run/secrets/{secret_name}', 'r') as secret_file:
            return secret_file.read().strip()
    except IOError as e:
        from core.error_handler import handle_error
        handle_error(f"Secret {secret_name} not found: {str(e)}")
        raise RuntimeError(f"Critical secret missing: {secret_name}")