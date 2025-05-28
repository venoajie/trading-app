#!/bin/sh
set -e

# Wait for network dependencies (if needed)
# Example: wait-for-it.sh redis:6379 --timeout=30

# Start the application
exec python -u /app/apps/receiver/main.py