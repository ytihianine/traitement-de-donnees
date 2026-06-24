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

# Parcourir tous les sous-dossiers pour copier config-example.json vers config.json
find "$SCRIPTS_DIR" -type d | while read -r SUBDIR; do
    CONFIG_EXAMPLE_FILE="$SUBDIR/config-example.json"
    CONFIG_FILE="$SUBDIR/config.json"

    # Vérifier si config-example.json existe et si config.json n'existe pas
    if [ -f "$CONFIG_EXAMPLE_FILE" ] && [ ! -f "$CONFIG_FILE" ]; then
        cp "$CONFIG_EXAMPLE_FILE" "$CONFIG_FILE"
        echo "Created: $CONFIG_FILE"
    elif [ -f "$CONFIG_EXAMPLE_FILE" ]; then
        echo "Skipped: $CONFIG_FILE (already exists)"
    fi
done
