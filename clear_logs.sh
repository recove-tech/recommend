#!/bin/bash
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

echo "Starting log cleanup..."

clear_directory "logs/run"
clear_directory "logs/run_sub"

echo "Log cleanup completed" 