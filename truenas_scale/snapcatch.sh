#!/bin/bash
set -o pipefail

####################################
# CONFIGURATION
####################################
ROOT_POOL="storage1"

# Datasets to NEVER touch (Regex format)
EXCLUDE_REGEX="${ROOT_POOL}/ix-applications|${ROOT_POOL}/\.system"

# Toggle Pruning Mode via command line: ./snapcatch.sh --prune
PRUNE_MODE=false
if [ "$1" == "--prune" ]; then
    PRUNE_MODE=true
    logger "snapcatch: [START] Running in DESTRUCTIVE PRUNE MODE"
else
    logger "snapcatch: [START] Running in REPORT ONLY mode"
fi


####################################
# HELPER: Deduce interval from Cron Schedule
####################################
get_interval_from_schedule() {
    local SCHED=$1
    local MIN=$(echo "$SCHED" | jq -r '.minute')
    local HOUR=$(echo "$SCHED" | jq -r '.hour')
    local DOM=$(echo "$SCHED" | jq -r '.dom')
    local DOW=$(echo "$SCHED" | jq -r '.dow')

    # 1. Check for "Every X Minutes" (e.g., */15)
    if [[ "$MIN" == "*/"* ]]; then
        local STEP=$(echo "$MIN" | cut -d'/' -f2)
        echo $((STEP * 60)); return
    fi
    # 2. Check for "Every X Hours" (e.g., */2)
    if [[ "$HOUR" == "*/"* ]]; then
        local STEP=$(echo "$HOUR" | cut -d'/' -f2)
        echo $((STEP * 3600)); return
    fi
    # 3. Frequency Deduction
    if [[ "$HOUR" == "*" ]];    then echo 3600; return; fi    # Hourly
    if [[ "$DOW" != "*" ]];     then echo 604800; return; fi  # Weekly
    if [[ "$DOM" != "*" ]];     then echo 2592000; return; fi # Monthly
    
    echo 86400 # Default: Daily
}


####################################
# HELPER: Get newest snapshot for task
####################################
get_last_snap_ts() {
    local DATASET=$1
    local PREFIX=$2
    # Search specific dataset for latest snapshot matching prefix
    zfs list -H -t snapshot -o name,creation -p -S creation -d 1 "$DATASET" 2>/dev/null | \
    grep "@${PREFIX}" | head -n 1 | awk '{print $2}' || echo 0
}

####################################
# HELPER: Run task and wait
####################################
run_task_and_wait() {
    local TASK_ID=$1; local MAX_AGE=$2; local DATASET=$3; local PREFIX=$4
    
    local LAST_TS=$(get_last_snap_ts "$DATASET" "$PREFIX")
    local NOW=$(date +%s)
    local AGE=$((NOW - LAST_TS))

    if [ "$AGE" -ge "$MAX_AGE" ]; then
        logger "snapcatch: Catching up $DATASET (@$PREFIX) → Task $TASK_ID"
        local RESULT=$(midclt call pool.snapshottask.run "$TASK_ID")
        
        if [[ "$RESULT" =~ ^[0-9]+$ ]]; then
            local JOB_ID=$RESULT
            local COUNTER=0
            while [ $COUNTER -lt 300 ]; do
                local STATE=$(midclt call core.get_jobs "[[\"id\", \"=\", $JOB_ID]]" | jq -r '.[0].state')
                [[ "$STATE" == "SUCCESS" ]] && return
                [[ "$STATE" == "FAILED" || "$STATE" == "ABORTED" ]] && { logger "snapcatch: Job $JOB_ID FAILED"; return; }
                sleep 2; ((COUNTER++))
            done
        fi
    fi
}

####################################
# EXECUTION LOOP: ALL TASKS
####################################
TASKS_JSON=$(midclt call pool.snapshottask.query "[]")

echo "$TASKS_JSON" | jq -c '.[]' | while read -r task; do
    [ "$(echo "$task" | jq -r '.enabled')" != "true" ] && continue

    TASK_ID=$(echo "$task" | jq -r '.id')
    DATASET=$(echo "$task" | jq -r '.dataset')
    SCHEMA=$(echo "$task" | jq -r '.naming_schema')
    PREFIX=$(echo "$SCHEMA" | cut -d'%' -f1)
    
    INTERVAL=$(get_interval_from_schedule "$(echo "$task" | jq -c '.schedule')")
    
    run_task_and_wait "$TASK_ID" "$INTERVAL" "$DATASET" "$PREFIX"
done

####################################
# SANITY CHECKS & GLOBAL STATS
####################################
logger "snapcatch: Performing sanity checks for $ROOT_POOL"

# 1. Prefix Stats
ALL_PREFIXES=$(echo "$TASKS_JSON" | jq -r '.[].naming_schema' | cut -d'%' -f1 | sort -u)
PREFIX_PATTERN=$(echo "$ALL_PREFIXES" | sed 's/^/@/' | tr '\n' '|' | sed 's/|$//')

echo "$ALL_PREFIXES" | while read -r P; do
    [ -z "$P" ] && continue
    COUNT=$(zfs list -t snapshot -H -r "$ROOT_POOL" | grep "@$P" | wc -l)
    logger "snapcatch: [STATS] Prefix @$P: $COUNT snapshots"
done

# 2. Unlinked & Pruning
UNLINKED_SNAPS=$(zfs list -t snapshot -o name -H -r "$ROOT_POOL" | \
    grep -vE "$EXCLUDE_REGEX" | \
    grep -vE "$PREFIX_PATTERN")

UNLINKED_COUNT=$(echo "$UNLINKED_SNAPS" | grep -v "^$" | wc -l)

if [ "$UNLINKED_COUNT" -gt 0 ]; then
    if [ "$PRUNE_MODE" = true ]; then
        logger "snapcatch: [PRUNE] Destroying $UNLINKED_COUNT unlinked snapshots..."
        echo "$UNLINKED_SNAPS" | while read -r SNAP; do
            zfs destroy "$SNAP" && logger "snapcatch: [DESTROYED] $SNAP"
        done
    else
        logger "snapcatch: [NOTICE] $UNLINKED_COUNT unlinked snapshots found (manual/orphaned). Run with --prune to delete."
    fi
fi

# 3. Global Totals
TOTAL_RECURSIVE=$(zfs list -t snapshot -H -r "$ROOT_POOL" | wc -l)
TOTAL_NON_RECURSIVE=$(zfs list -t snapshot -H -d 1 "$ROOT_POOL" | wc -l)
logger "snapcatch: [GLOBAL] Total Pool: $TOTAL_RECURSIVE | Root Only: $TOTAL_NON_RECURSIVE"

logger "snapcatch: done"
exit 0
