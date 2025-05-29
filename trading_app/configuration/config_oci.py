# configuration/config_oci.py
"""OCI Vault Integration for Secure Secret Retrieval"""

import base64
import oci
from typing import Optional

def get_oci_key(key_ocid: str) -> Optional[str]:
    """
    Retrieves secrets from OCI Vault
    
    Security Protocol:
    - Uses OCI SDK for authentication
    - Secrets are base64 decoded
    - Never logs or stores decrypted secrets
    
    Args:
        key_ocid: OCID of the secret to retrieve
    
    Returns:
        Decrypted secret string or None on failure
    """
    try:
        config = oci.config.from_file()
        secret_client = oci.secrets.SecretsClient(config)
        secret_bundle = secret_client.get_secret_bundle(secret_id=key_ocid)
        
        # Base64 decode the secret
        encoded_secret = secret_bundle.data.secret_bundle_content.content
        decoded_bytes = base64.b64decode(encoded_secret.encode("ascii"))
        return decoded_bytes.decode("ascii")
    
    except Exception as e:
        # TODO: Integrate with error handling system
        print(f"OCI Secret Retrieval Error: {str(e)}")
        return None