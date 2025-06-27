#!/bin/bash

set -e

# Use absolute paths
SCRIPT_DIR="/home/opc/recommend"
LOG_DIR="$SCRIPT_DIR/logs"
VENV_PYTHON="$SCRIPT_DIR/venv/bin/python"

# Ensure logs directory exists
mkdir -p "$LOG_DIR"

LOG_FILE="$LOG_DIR/$(date '+%Y-%m-%d_%H-%M-%S').log"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

cd "$SCRIPT_DIR"

log "Starting service"

log "Running main.py with virtualenv Python"
"$VENV_PYTHON" "$SCRIPT_DIR/main.py"

log "Script completed successfully"