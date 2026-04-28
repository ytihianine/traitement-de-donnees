#!/bin/bash

# Load env vars from centralised .env
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
set -a
source "$SCRIPT_DIR/../.env"
set +a

# Execute python script using project virtualenv if available
# Path to repo-level virtualenv Python (two levels up from this script)
VENV_PY="$SCRIPT_DIR/../../env/bin/python3"
if [ -x "$VENV_PY" ]; then
	"$VENV_PY" "$SCRIPT_DIR/main.py"
else
	python3 "$SCRIPT_DIR/main.py"
fi
