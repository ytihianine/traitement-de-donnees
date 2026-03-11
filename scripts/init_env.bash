#!/bin/bash

for example_env in scripts/**/example.env; do
    if [ -f "$example_env" ]; then
        dir=$(dirname "$example_env")
        env_file="$dir/.env"
        echo "Processing: $example_env"
        if [ ! -f "$env_file" ]; then
            cp "$example_env" "$env_file"
            echo "Created: $env_file"
        else
            echo "Skipped: $env_file (already exists)"
        fi
    fi
done
