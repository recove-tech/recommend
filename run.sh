#!/bin/bash

# Exit on any error
set -e

# Log file
LOG_FILE="recommend.log"

# Function to log messages
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Change to the project directory
cd "$(dirname "$0")"

log "Starting recommend service"

# Activate virtual environment
log "Activating virtual environment"
source venv/bin/activate

# Run the Python script
log "Running main.py"
python main.py

# Log completion
log "Script completed successfully"

# Deactivate virtual environment
deactivate