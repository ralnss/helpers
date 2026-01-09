#!/bin/bash
set -o pipefail

# Check if the first argument is "--prune"
PRUNE_MODE=false
if [ "$1" == "--prune" ]; then
    PRUNE_MODE=true
    logger "snapcatch: RUNNING IN PRUNE MODE - Unlinked snapshots will be destroyed"
fi

####################################
# CONFIG — SET YOUR TASK IDs HERE
####################################
HOURLY_TASK_ID=1
DAILY_TASK_ID=2
WEEKLY_TASK_ID=3
MONTHLY_TASK_ID=4

# Intervals in seconds
HOUR_IN_SEC=3600
DAY_IN_SEC=86400
WEEK_IN_SEC=604800
MONTH_IN_SEC=2592000 # 30 days

logger "snapcatch: Starting snapshot catch-up script"

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

    local TASK_INFO
    TASK_INFO=$(midclt call pool.snapshottask.get_instance "${TASK_ID}" 2>/dev/null)
    
    if [ -z "$TASK_INFO" ]; then
        logger "snapcatch: Task ID $TASK_ID not found"
        return
    fi

    # check if enabled
    local IS_ENABLED=$(echo "$TASK_INFO" | jq -r '.enabled')
    if [ "$IS_ENABLED" != "true" ]; then
        logger "snapcatch: $LABEL task (ID $TASK_ID) is disabled"
        return
    fi
    
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

run_task_and_wait "$HOURLY_TASK_ID"  "$HOUR_IN_SEC"  "Hourly"
run_task_and_wait "$DAILY_TASK_ID"   "$DAY_IN_SEC"   "Daily"
run_task_and_wait "$WEEKLY_TASK_ID"  "$WEEK_IN_SEC"  "Weekly"
run_task_and_wait "$MONTHLY_TASK_ID" "$MONTH_IN_SEC" "Monthly"

logger "snapcatch: checking missed snapshots done"

####################################
# SNAPSHOT TASK SANITY CHECKS
####################################

log_snap_counts() {
    local TASK_ID=$1
    local LABEL=$2
    
    # Retrieve task details using midclt
    local TASK_INFO
    TASK_INFO=$(midclt call pool.snapshottask.get_instance "${TASK_ID}" 2>/dev/null)
    
    if [ -z "$TASK_INFO" ]; then
        logger "snapcatch: Could not retrieve info for $LABEL task ID $TASK_ID"
        return
    fi

    local DATASET=$(echo "$TASK_INFO" | jq -r '.dataset')
    local SCHEMA=$(echo "$TASK_INFO" | jq -r '.naming_schema')
    
    # Extract the prefix (e.g., 'auto-daily-') before the first '%'
    local PREFIX=$(echo "$SCHEMA" | cut -d'%' -f1)

    # Sanity Check: Count snapshots matching this specific dataset and prefix
    # Uses 'zfs list' with '-d 1' to avoid counting child dataset snapshots unless recursive
    local COUNT
    COUNT=$(zfs list -t snapshot -o name -H | grep "^${DATASET}@${PREFIX}" | wc -l)

    logger "snapcatch: $LABEL ($DATASET) - prefix: \"@${PREFIX}\" - total snapshots: $COUNT"
    
    # Example Sanity Warning: Log if zero snapshots exist for an active task
    if [ "$COUNT" -eq 0 ]; then
        logger "snapcatch: No snapshots found for $LABEL task on $DATASET!"
    fi
}

logger "snapcatch: Running snapshot count sanity checks"

log_snap_counts "$HOURLY_TASK_ID"  "Hourly"
log_snap_counts "$DAILY_TASK_ID"   "Daily"
log_snap_counts "$WEEKLY_TASK_ID"  "Weekly"
log_snap_counts "$MONTHLY_TASK_ID" "Monthly"

####################################
# CONFIG - GLOBAL CHECKS
####################################
ROOT_POOL="storage1"

# Datasets to ignore (Regex format). 
# ALWAYS include ix-applications and .system to prevent breaking TrueNAS.
EXCLUDE_REGEX="${ROOT_POOL}/ix-applications|${ROOT_POOL}/\.system"

####################################
# GLOBAL & UNLINKED CHECKS
####################################

# 1. Recursive and Non-Recursive Counts
TOTAL_RECURSIVE=$(zfs list -t snapshot -H -r "$ROOT_POOL" | wc -l)
TOTAL_ROOT_ONLY=$(zfs list -t snapshot -H -d 1 "$ROOT_POOL" | wc -l)

logger "snapcatch: [GLOBAL] Total $ROOT_POOL snapshots (recursive): $TOTAL_RECURSIVE"
logger "snapcatch: [GLOBAL] $ROOT_POOL root-level snapshots: $TOTAL_ROOT_ONLY"


# 2. Identify Unlinked Snapshots (Excluding Protected Datasets)
# Fetch all active naming prefixes from TrueNAS API
ALL_PREFIXES=$(midclt call pool.snapshottask.query | jq -r '.[].naming_schema' | cut -d'%' -f1 | sort -u)
PREFIX_PATTERN=$(echo "$ALL_PREFIXES" | sed 's/^/@/' | tr '\n' '|' | sed 's/|$//')

# Find snapshots that don't match task prefixes AND aren't in excluded datasets
UNLINKED_SNAPS=$(zfs list -t snapshot -o name -H -r "$ROOT_POOL" | \
    grep -vE "$EXCLUDE_REGEX" | \
    grep -vE "$PREFIX_PATTERN")

# 3. Handle Pruning or Reporting
if [ -n "$UNLINKED_SNAPS" ]; then
    UNLINKED_COUNT=$(echo "$UNLINKED_SNAPS" | grep -v "^$" | wc -l)
    
    if [ "$PRUNE_MODE" = true ]; then
        logger "snapcatch: Destroying $UNLINKED_COUNT unlinked snapshots (Excluding: $EXCLUDE_REGEX)..."
        echo "$UNLINKED_SNAPS" | while read -r SNAP; do
            zfs destroy "$SNAP" && logger "snapcatch: destroyed $SNAP"
        done
    else
        logger "snapcatch: Found $UNLINKED_COUNT unlinked snapshots in $ROOT_POOL. (Excluded: $EXCLUDE_REGEX)"
    fi
else
    logger "snapcatch: No unlinked snapshots found in $ROOT_POOL."
fi

logger "snapcatch: All checks completed"

exit 0
