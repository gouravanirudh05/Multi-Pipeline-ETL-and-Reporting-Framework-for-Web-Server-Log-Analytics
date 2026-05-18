#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
UDF_SOURCE_PATH="$SCRIPT_DIR/udfs/log_parser.py"

usage() {
    echo "Usage: $0 <log_file_path_or_json_array> <batch_mode> <batch_value> <run_uuid> [query]"
    echo "Legacy: $0 <log_file_path> <batch_size>"
}

if [[ "$#" -eq 2 ]]; then
    LOG_ARG="$1"
    BATCH_MODE="records"
    BATCH_VALUE="$2"
    RUN_UUID="pig-cli-$(date +%s)"
    QUERY="all"
    AGGREGATION_MODE="global"
elif [[ "$#" -ge 4 ]]; then
    LOG_ARG="$1"
    BATCH_MODE="$2"
    BATCH_VALUE="$3"
    RUN_UUID="$4"
    QUERY="${5:-all}"
    AGGREGATION_MODE="${6:-global}"
else
    usage
    exit 1
fi
if [[ "${AGGREGATION_MODE:-global}" != "global" && "${AGGREGATION_MODE:-global}" != "per_batch" ]]; then
    echo "ERROR: aggregation_mode must be global or per_batch" >&2
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

PIG_BIN="${PIG_BIN:-}"
if [[ -z "$PIG_BIN" && -n "${PIG_HOME:-}" && -x "$PIG_HOME/bin/pig" ]]; then
    PIG_BIN="$PIG_HOME/bin/pig"
fi
if [[ -z "$PIG_BIN" ]]; then
    PIG_BIN="$(command -v pig || true)"
fi
if [[ -z "$PIG_BIN" && -x "$HOME/pig/bin/pig" ]]; then
    PIG_BIN="$HOME/pig/bin/pig"
fi
if [[ -z "$PIG_BIN" ]]; then
    echo "ERROR: pig command not found. Set PIG_HOME/PIG_BIN or add pig to PATH." >&2
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

mapfile -t LOG_FILES < <(parse_paths "$LOG_ARG")
for log_file in "${LOG_FILES[@]}"; do
    if [[ ! -f "$log_file" ]]; then
        echo "ERROR: Log file not found: $log_file" >&2
        exit 1
    fi
done

LOCAL_STAGE_DIR="$(mktemp -d /tmp/nasa-pig-input.XXXXXX)"
UDF_PATH="$LOCAL_STAGE_DIR/log_parser.py"

BASE_HDFS_DIR="${HDFS_WORK_DIR:-/tmp/nasa-etl/pig/$RUN_UUID}"
INPUT_DIR="$BASE_HDFS_DIR/input"
OUTPUT_DIR="$BASE_HDFS_DIR/output"
LOCAL_OUTPUT="$(mktemp -d)"
COMBINED_PIG="$(mktemp)"
START_SECONDS="$(date +%s)"

cleanup() {
    rm -rf "$LOCAL_OUTPUT"
    rm -rf "$LOCAL_STAGE_DIR"
    rm -f "$COMBINED_PIG"
}
trap cleanup EXIT

echo "Pig ETL started"
echo "Run UUID: $RUN_UUID"
echo "Batch mode: $BATCH_MODE"
echo "Batch value: $BATCH_VALUE"
echo "Aggregation mode: ${AGGREGATION_MODE:-global}"
echo "Input HDFS directory: $INPUT_DIR"

cp "$UDF_SOURCE_PATH" "$UDF_PATH"

"${DFS_CMD[@]}" -rm -r -f "$BASE_HDFS_DIR" >/dev/null 2>&1 || true
"${DFS_CMD[@]}" -mkdir -p "$INPUT_DIR"
for log_file in "${LOG_FILES[@]}"; do
    staged_file="$log_file"
    if [[ "$log_file" == *" "* ]]; then
        staged_file="$LOCAL_STAGE_DIR/$(basename "$log_file")"
        cp "$log_file" "$staged_file"
    fi
    echo "Uploading $(basename "$log_file") to HDFS"
    "${DFS_CMD[@]}" -put -f "$staged_file" "$INPUT_DIR/"
done

build_pig_script() {
    : > "$COMBINED_PIG"
    local q1_script="$SCRIPT_DIR/q1_daily_traffic.pig"
    local q2_script="$SCRIPT_DIR/q2_top_resources.pig"
    local q3_script="$SCRIPT_DIR/q3_hourly_errors.pig"
    if [[ "${AGGREGATION_MODE:-global}" == "per_batch" ]]; then
        q1_script="$SCRIPT_DIR/q1_daily_traffic_per_batch.pig"
        q2_script="$SCRIPT_DIR/q2_top_resources_per_batch.pig"
        q3_script="$SCRIPT_DIR/q3_hourly_errors_per_batch.pig"
    fi
    cat "$SCRIPT_DIR/common.pig" >> "$COMBINED_PIG"
    printf '\n' >> "$COMBINED_PIG"
    cat "$SCRIPT_DIR/metadata.pig" >> "$COMBINED_PIG"
    printf '\n' >> "$COMBINED_PIG"
    if [[ "$QUERY" == "all" || "$QUERY" == "q1" ]]; then
        cat "$q1_script" >> "$COMBINED_PIG"
        printf '\n' >> "$COMBINED_PIG"
    fi
    if [[ "$QUERY" == "all" || "$QUERY" == "q2" ]]; then
        cat "$q2_script" >> "$COMBINED_PIG"
        printf '\n' >> "$COMBINED_PIG"
    fi
    if [[ "$QUERY" == "all" || "$QUERY" == "q3" ]]; then
        cat "$q3_script" >> "$COMBINED_PIG"
        printf '\n' >> "$COMBINED_PIG"
    fi
}

build_pig_script

"$PIG_BIN" \
    -Dmapreduce.framework.name=local \
    -param "INPUT=$INPUT_DIR" \
    -param "OUTPUT=$OUTPUT_DIR" \
    -param "UDF_PATH=$UDF_PATH" \
    -param "BATCH_VALUE=$BATCH_VALUE" \
    -param "BATCH_BY_TIME=$([[ "$BATCH_MODE" == "time" ]] && echo 1 || echo 0)" \
    -param "AGGREGATION_MODE=${AGGREGATION_MODE:-global}" \
    "$COMBINED_PIG"

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
    "pig" "$RUN_UUID" "$BATCH_MODE" "$BATCH_VALUE" "$LOCAL_OUTPUT" "${AGGREGATION_MODE:-global}" "$QUERY" "$RUNTIME_SECONDS"

echo "Pig ETL completed"
echo "Output HDFS directory: $OUTPUT_DIR"
