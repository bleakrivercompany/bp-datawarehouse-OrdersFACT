#!/bin/bash

# Define variables for clarity
PROJECT_DIR="~/bp_etl/bp-datawarehouse-OrdersFACT"
LOG_FILE="/tmp/etl_run_log_$(date +'%Y%m%d_%H%M%S').txt"
GCS_BUCKET="gs://cs-royalties-test"
GCS_DESTINATION_PATH="/run_logs/"

# Navigate to your project directory
cd "$PROJECT_DIR"

# Activate the virtual environment
source venv/bin/activate

# Execute the main Python script and redirect ALL output (stdout and stderr) to a log file
python3 main_orchestrator.py > "$LOG_FILE" 2>&1

# Deactivate the virtual environment
deactivate

# Check if the log file was created successfully before attempting to upload
if [ -f "$LOG_FILE" ]; then
    # Upload the log file to Google Cloud Storage
    echo "Uploading log file to GCS..."
    gsutil cp "$LOG_FILE" "$GCS_BUCKET/$GCS_DESTINATION_PATH"
    echo "Upload complete."

    # Optionally, remove the local log file to clean up
    rm "$LOG_FILE"
fi