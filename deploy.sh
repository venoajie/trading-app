#!/bin/bash
# deploy.sh

SECRETS_DIR="/path/to/persistent/secrets"

# Create secrets if not exists
mkdir -p $SECRETS_DIR
test -f "$SECRETS_DIR/client_id.txt" || echo "$DERIBIT_CLIENT_ID" > "$SECRETS_DIR/client_id.txt"
test -f "$SECRETS_DIR/client_secret.txt" || echo "$DERIBIT_CLIENT_SECRET" > "$SECRETS_DIR/client_secret.txt"

# Symlink to project structure
ln -s "$SECRETS_DIR" ./secrets

docker compose up -d