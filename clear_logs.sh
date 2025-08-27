#!/bin/bash

# Script to clear log files from logs/default and logs/mobile directories

# Function to safely remove files from a directory
clear_directory() {
    local dir="$1"
    if [ -d "$dir" ]; then
        echo "Clearing files from $dir..."
        rm -f "$dir"/*
        echo "Successfully cleared $dir"
    else
        echo "Warning: Directory $dir does not exist"
    fi
}

# Main script
echo "Starting log cleanup..."

# Clear logs/default
clear_directory "logs/default"

# Clear logs/mobile
clear_directory "logs/mobile"

echo "Log cleanup completed" 