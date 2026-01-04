# For truenas machines that don't run 24/7 and frequently miss scheduled snapshot tasks due to downtime.
# Run post-init to catch up on daily/weekly/monthly snapshot tasks. Depends on your configured snapshot tasks and their IDs!


#!/bin/bash
set -o pipefail

####################################
# CONFIG — SET YOUR TASK IDs HERE
####################################
DAILY_TASK_ID=2
WEEKLY_TASK_ID=3
MONTHLY_TASK_ID=4

# Intervals in seconds
DAY_IN_SEC=86400
WEEK_IN_SEC=604800
MONTH_IN_SEC=2592000 # 30 days

logger "snapcatch: starting snapshot catch-up script"

####################################
# HELPER: Get newest snapshot for SPECIFIC task
####################################
get_last_snap_ts() {
    local TASK_ID=$1
    local TASK_INFO
    TASK_INFO=$(midclt call pool.snapshottask.get_instance "${TASK_ID}" 2>/dev/null)
    
    if [ -z "$TASK_INFO" ]; then echo 0; return; fi

    local DATASET=$(echo "$TASK_INFO" | jq -r '.dataset')
    local SCHEMA=$(echo "$TASK_INFO" | jq -r '.naming_schema')
    
    # Extract the prefix before the first '%' (e.g., "auto-weekly-")
    local PREFIX=$(echo "$SCHEMA" | cut -d'%' -f1)

    # Search only this dataset for snapshots matching this task's prefix
    zfs list -H -t snapshot -o name,creation -p -S creation -d 1 "$DATASET" 2>/dev/null | \
    grep "@${PREFIX}" | \
    head -n 1 | awk '{print $2}' || echo 0
}

####################################
# HELPER: Run task and wait for completion
####################################
run_task_and_wait() {
    local TASK_ID=$1
    local MAX_AGE=$2
    local LABEL=$3
    
    local LAST_TS=$(get_last_snap_ts "$TASK_ID")
    local NOW=$(date +%s)
    local AGE=$((NOW - LAST_TS))

    if [ "$AGE" -ge "$MAX_AGE" ]; then
        logger "snapcatch: $LABEL snapshot missing → running task $TASK_ID"
        
        # Start task
        local RESULT
        RESULT=$(midclt call pool.snapshottask.run "$TASK_ID")
        
        # Handle the different return types
        if [ "$RESULT" == "null" ] || [ -z "$RESULT" ]; then
            logger "snapcatch: $LABEL task completed immediately (null return)"
            return
        fi

        # If it's a number, it's a Job ID we need to wait for
        if [[ "$RESULT" =~ ^[0-9]+$ ]]; then
            local JOB_ID=$RESULT
            logger "snapcatch: waiting for job $JOB_ID..."
            
            local COUNTER=0
            while [ $COUNTER -lt 300 ]; do
                local STATE=$(midclt call core.get_jobs "[[\"id\", \"=\", $JOB_ID]]" | jq -r '.[0].state')

                if [[ "$STATE" == "SUCCESS" ]]; then
                    logger "snapcatch: $LABEL task finished successfully"
                    return
                elif [[ "$STATE" == "FAILED" ]] || [[ "$STATE" == "ABORTED" ]]; then
                    logger "snapcatch: $LABEL task FAILED or was ABORTED"
                    return
                fi
                sleep 2
                ((COUNTER++))
            done
        else
            logger "snapcatch: ERROR - Unexpected response from task $TASK_ID: $RESULT"
        fi
    else
        logger "snapcatch: $LABEL snapshot is fresh"
    fi
}

####################################
# EXECUTION
####################################

run_task_and_wait "$DAILY_TASK_ID"   "$DAY_IN_SEC"   "Daily"
run_task_and_wait "$WEEKLY_TASK_ID"  "$WEEK_IN_SEC"  "Weekly"
run_task_and_wait "$MONTHLY_TASK_ID" "$MONTH_IN_SEC" "Monthly"

logger "snapcatch: done"
exit 0
