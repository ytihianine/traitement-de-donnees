#!/bin/bash

# Copy the centralised example.env to scripts/.env if it doesn't exist
SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPTS_DIR/.env"
EXAMPLE_FILE="$SCRIPTS_DIR/example.env"

if [ ! -f "$ENV_FILE" ]; then
    cp "$EXAMPLE_FILE" "$ENV_FILE"
    echo "Created: $ENV_FILE"
else
    echo "Skipped: $ENV_FILE (already exists)"
fi
