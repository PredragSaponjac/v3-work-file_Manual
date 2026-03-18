#!/bin/bash
# Wait for pipeline process to finish, then run overnight 5 min later
PIPELINE_PID=32380
echo "[$(date '+%H:%M:%S')] Waiting for pipeline PID $PIPELINE_PID to finish..."

# Poll every 30 seconds
while kill -0 $PIPELINE_PID 2>/dev/null; do
    sleep 30
done

echo "[$(date '+%H:%M:%S')] Pipeline finished! Waiting 5 minutes before overnight run..."
sleep 300

echo "[$(date '+%H:%M:%S')] Starting overnight run..."
cd /c/Users/18329/Downloads/v3-work-file_Budget
PYTHONIOENCODING=utf-8 /c/Users/18329/anaconda3/python.exe overnight_v20.py --verbose 2>&1 | tee overnight_tonight.log
echo "[$(date '+%H:%M:%S')] Overnight run complete."
