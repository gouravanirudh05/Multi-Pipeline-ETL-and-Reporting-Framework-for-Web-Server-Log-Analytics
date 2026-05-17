#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

usage() {
    echo "Usage: $0 <log_file_path_or_json_array> <batch_mode> <batch_value> <run_uuid> [query]"
    echo "Legacy: $0 <log_file_path> <batch_size>"
}

if [[ "$#" -eq 2 ]]; then
    LOG_ARG="$1"
    BATCH_MODE="records"
    BATCH_VALUE="$2"
    RUN_UUID="hive-cli-$(date +%s)"
    QUERY="all"
elif [[ "$#" -ge 4 ]]; then
    LOG_ARG="$1"
    BATCH_MODE="$2"
    BATCH_VALUE="$3"
    RUN_UUID="$4"
    QUERY="${5:-all}"
else
    usage
    exit 1
fi

if [[ "$BATCH_MODE" != "records" && "$BATCH_MODE" != "time" ]]; then
    echo "ERROR: batch_mode must be records or time" >&2
    exit 1
fi
if [[ "$QUERY" != "all" && "$QUERY" != "q1" && "$QUERY" != "q2" && "$QUERY" != "q3" ]]; then
    echo "ERROR: query must be one of all, q1, q2, q3" >&2
    exit 1
fi
if ! [[ "$BATCH_VALUE" =~ ^[0-9]+$ ]] || [[ "$BATCH_VALUE" -le 0 ]]; then
    echo "ERROR: batch_value must be a positive integer" >&2
    exit 1
fi

HIVE_BIN="${HIVE_BIN:-}"
if [[ -z "$HIVE_BIN" && -n "${HIVE_HOME:-}" && -x "$HIVE_HOME/bin/hive" ]]; then
    HIVE_BIN="$HIVE_HOME/bin/hive"
fi
if [[ -z "$HIVE_BIN" ]]; then
    HIVE_BIN="$(command -v hive || true)"
fi
if [[ -z "$HIVE_BIN" && -x "$HOME/hive/bin/hive" ]]; then
    HIVE_BIN="$HOME/hive/bin/hive"
fi
if [[ -z "$HIVE_BIN" ]]; then
    echo "ERROR: hive command not found. Set HIVE_HOME/HIVE_BIN or add hive to PATH." >&2
    exit 1
fi

if command -v hdfs >/dev/null 2>&1; then
    DFS_CMD=(hdfs dfs)
elif command -v hadoop >/dev/null 2>&1; then
    DFS_CMD=(hadoop fs)
elif [[ -n "${HADOOP_HOME:-}" && -x "$HADOOP_HOME/bin/hdfs" ]]; then
    DFS_CMD=("$HADOOP_HOME/bin/hdfs" dfs)
elif [[ -n "${HADOOP_HOME:-}" && -x "$HADOOP_HOME/bin/hadoop" ]]; then
    DFS_CMD=("$HADOOP_HOME/bin/hadoop" fs)
elif [[ -x "$HOME/hadoop/bin/hdfs" ]]; then
    DFS_CMD=("$HOME/hadoop/bin/hdfs" dfs)
else
    echo "ERROR: hdfs/hadoop command not found. Start Hadoop and set HADOOP_HOME." >&2
    exit 1
fi

parse_paths() {
    local raw="$1"
    if [[ "$raw" == \[* ]]; then
        printf '%s\n' "$raw" \
            | sed -e 's/^\[//' -e 's/\]$//' -e 's/","/\n/g' -e 's/^"//' -e 's/"$//'
    else
        printf '%s\n' "$raw"
    fi
}

sanitize_name() {
    printf '%s' "$1" | tr -c 'A-Za-z0-9_' '_' | cut -c 1-96
}

mapfile -t LOG_FILES < <(parse_paths "$LOG_ARG")
for log_file in "${LOG_FILES[@]}"; do
    if [[ ! -f "$log_file" ]]; then
        echo "ERROR: Log file not found: $log_file" >&2
        exit 1
    fi
done

BASE_HDFS_DIR="${HDFS_WORK_DIR:-/tmp/nasa-etl/hive/$RUN_UUID}"
INPUT_DIR="$BASE_HDFS_DIR/input"
OUTPUT_DIR="$BASE_HDFS_DIR/output"
DATABASE="nasa_etl_$(sanitize_name "$RUN_UUID")"
LOCAL_OUTPUT="$(mktemp -d)"
START_SECONDS="$(date +%s)"

cleanup() {
    rm -rf "$LOCAL_OUTPUT"
}
trap cleanup EXIT

echo "Hive ETL started"
echo "Run UUID: $RUN_UUID"
echo "Batch mode: $BATCH_MODE"
echo "Batch value: $BATCH_VALUE"
echo "Input HDFS directory: $INPUT_DIR"

"${DFS_CMD[@]}" -rm -r -f "$BASE_HDFS_DIR" >/dev/null 2>&1 || true
"${DFS_CMD[@]}" -mkdir -p "$INPUT_DIR"
for log_file in "${LOG_FILES[@]}"; do
    echo "Uploading $(basename "$log_file") to HDFS"
    "${DFS_CMD[@]}" -put -f "$log_file" "$INPUT_DIR/"
done

run_hql() {
    local script_path="$1"
    echo "Running Hive script: $(basename "$script_path")"
    "$HIVE_BIN" \
        --hiveconf "mapreduce.framework.name=local" \
        --hiveconf "hive.exec.mode.local.auto=true" \
        --hivevar "DATABASE=$DATABASE" \
        --hivevar "INPUT_DIR=$INPUT_DIR" \
        --hivevar "OUTPUT_DIR=$OUTPUT_DIR" \
        --hivevar "BATCH_MODE=$BATCH_MODE" \
        --hivevar "BATCH_VALUE=$BATCH_VALUE" \
        -f "$script_path"
}

run_hql "$SCRIPT_DIR/setup.hql"
run_hql "$SCRIPT_DIR/metadata.hql"
if [[ "$QUERY" == "all" || "$QUERY" == "q1" ]]; then
    run_hql "$SCRIPT_DIR/q1_daily_traffic.hql"
fi
if [[ "$QUERY" == "all" || "$QUERY" == "q2" ]]; then
    run_hql "$SCRIPT_DIR/q2_top_resources.hql"
fi
if [[ "$QUERY" == "all" || "$QUERY" == "q3" ]]; then
    run_hql "$SCRIPT_DIR/q3_hourly_errors.hql"
fi
run_hql "$SCRIPT_DIR/cleanup.hql"

merge_output() {
    local hdfs_path="$1"
    local local_file="$2"
    if "${DFS_CMD[@]}" -test -e "$hdfs_path"; then
        "${DFS_CMD[@]}" -getmerge "$hdfs_path" "$local_file"
    else
        : > "$local_file"
    fi
}

merge_output "$OUTPUT_DIR/batch_metadata" "$LOCAL_OUTPUT/batch_metadata.tsv"
merge_output "$OUTPUT_DIR/malformed_records" "$LOCAL_OUTPUT/malformed_records.tsv"
[[ "$QUERY" == "all" || "$QUERY" == "q1" ]] && merge_output "$OUTPUT_DIR/q1" "$LOCAL_OUTPUT/q1.tsv" || : > "$LOCAL_OUTPUT/q1.tsv"
[[ "$QUERY" == "all" || "$QUERY" == "q2" ]] && merge_output "$OUTPUT_DIR/q2" "$LOCAL_OUTPUT/q2.tsv" || : > "$LOCAL_OUTPUT/q2.tsv"
[[ "$QUERY" == "all" || "$QUERY" == "q3" ]] && merge_output "$OUTPUT_DIR/q3" "$LOCAL_OUTPUT/q3.tsv" || : > "$LOCAL_OUTPUT/q3.tsv"

RUNTIME_SECONDS="$(( $(date +%s) - START_SECONDS ))"
bash "$PROJECT_ROOT/scripts/load_tsv_to_postgres.sh" \
    "hive" "$RUN_UUID" "$BATCH_MODE" "$BATCH_VALUE" "$LOCAL_OUTPUT" "$QUERY" "$RUNTIME_SECONDS"

echo "Hive ETL completed"
echo "Output HDFS directory: $OUTPUT_DIR"
