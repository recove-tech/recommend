#!/bin/bash

set -e

SCRIPT_DIR="/home/opc/recommend"
LOG_DIR="$SCRIPT_DIR/logs/run_sub"
VENV_PYTHON="$SCRIPT_DIR/venv/bin/python"

mkdir -p "$LOG_DIR"

LOG_FILE="$LOG_DIR/$(date '+%Y-%m-%d_%H-%M-%S').log"
LATEST_LOG="$LOG_DIR/latest.log"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE" | tee -a "$LATEST_LOG"
}

cd "$SCRIPT_DIR"

log "Subscribed users"

log "Running main.py..."
"$VENV_PYTHON" "$SCRIPT_DIR/main.py" --is_subscribed true 2>&1 | tee -a "$LOG_FILE" | tee -a "$LATEST_LOG" > /dev/null

log "Done."