#!/bin/bash

set -e

SCRIPT_DIR="/home/opc/recommend"
LOG_DIR="$SCRIPT_DIR/logs/mobile"
VENV_PYTHON="$SCRIPT_DIR/venv/bin/python"

mkdir -p "$LOG_DIR"

LOG_FILE="$LOG_DIR/$(date '+%Y-%m-%d_%H-%M-%S').log"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

cd "$SCRIPT_DIR"

log "Starting mobile service"

log "Running main.py with virtualenv Python for mobile recommendations"
"$VENV_PYTHON" "$SCRIPT_DIR/main.py" --mobile

log "Mobile script completed successfully" 