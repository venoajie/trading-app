#!/bin/bash

# 1. Validate environment variables
if [[ -z "$DERIBIT_CLIENT_ID" || -z "$DERIBIT_CLIENT_SECRET" ]]; then
    echo "ERROR: Missing required environment variables"
    exit 1
fi

# 2. Create secure secrets directory
SECRETS_DIR="$HOME/.app_secrets"
mkdir -p "$SECRETS_DIR"
chmod 700 "$SECRETS_DIR"

# 3. Store secrets with strict permissions
echo "$DERIBIT_CLIENT_ID" > "$SECRETS_DIR/client_id.txt"
echo "$DERIBIT_CLIENT_SECRET" > "$SECRETS_DIR/client_secret.txt"
chmod 600 "$SECRETS_DIR"/*.txt

# 4. Create project symlink
ln -sfn "$SECRETS_DIR" ./secrets

# 5. Launch application
docker compose up -d --build