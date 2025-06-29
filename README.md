# Recommend Service

This service runs a recommendation engine on an hourly schedule using systemd.

## Setup Instructions

1. Clone the repository and navigate to the project directory:
   ```bash
   cd /home/opc/recommend
   ```

2. Set up Python virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

3. Set proper permissions:
   ```bash
   # Make the script executable
   chmod 755 run.sh
   
   # Set proper SELinux context
   sudo chcon -t bin_t run.sh
   
   # Ensure directory permissions
   sudo chmod 755 /home/opc/recommend
   ```

4. Install systemd service and timer:
   ```bash
   # Copy service and timer files
   sudo cp recommend-hourly.service /etc/systemd/system/
   sudo cp recommend-hourly.timer /etc/systemd/system/
   
   # Reload systemd
   sudo systemctl daemon-reload
   
   # Enable and start the timer
   sudo systemctl enable recommend-hourly.timer
   sudo systemctl start recommend-hourly.timer
   ```

## Service Management

### Check Status
```bash
# Check service status
systemctl status recommend-hourly.service

# Check timer status
systemctl list-timers recommend-hourly.timer
```

### Logs
- Service logs are stored in `/home/opc/recommend/logs/`
- View systemd logs:
  ```bash
  journalctl -u recommend-hourly.service
  ```

### Manual Control
```bash
# Run the service manually
sudo systemctl start recommend-hourly.service

# Stop the timer (to pause scheduling)
sudo systemctl stop recommend-hourly.timer

# Disable the timer (to prevent it from running on boot)
sudo systemctl disable recommend-hourly.timer
```

### Troubleshooting
If the service fails:
1. Check permissions: `ls -l run.sh`
2. Verify SELinux context: `ls -Z run.sh`
3. Check service logs: `journalctl -u recommend-hourly.service -n 50`
4. Try running the script manually: `./run.sh`

## File Structure
- `run.sh`: Main script that runs the recommendation engine
- `main.py`: Python script containing the recommendation logic
- `recommend-hourly.service`: Systemd service configuration
- `recommend-hourly.timer`: Systemd timer configuration for hourly execution
- `logs/`: Directory containing execution logs