#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_DIR="$SCRIPT_DIR/src/main/java"
TARGET_DIR="$SCRIPT_DIR/target"
CLASSES_DIR="$TARGET_DIR/classes"
JAR_PATH="$TARGET_DIR/nasa-log-mapreduce.jar"
SOURCES_FILE="$TARGET_DIR/sources.txt"

HADOOP_BIN="${HADOOP_BIN:-}"
if [[ -z "$HADOOP_BIN" ]]; then
  HADOOP_BIN="$(command -v hadoop || true)"
fi
if [[ -z "$HADOOP_BIN" && -x "$HOME/hadoop/bin/hadoop" ]]; then
  HADOOP_BIN="$HOME/hadoop/bin/hadoop"
fi

if [[ -z "$HADOOP_BIN" ]]; then
  echo "ERROR: hadoop command not found in PATH" >&2
  exit 1
fi

if command -v hdfs >/dev/null 2>&1; then
  DFS_CMD=(hdfs dfs)
elif [[ -n "${HADOOP_HOME:-}" && -x "$HADOOP_HOME/bin/hdfs" ]]; then
  DFS_CMD=("$HADOOP_HOME/bin/hdfs" dfs)
else
  DFS_CMD=("$HADOOP_BIN" fs)
fi

if ! command -v javac >/dev/null 2>&1; then
  echo "ERROR: javac command not found in PATH" >&2
  exit 1
fi

if [[ "$#" -lt 4 ]]; then
  echo "Usage: $0 <log_file_path_or_json_array> <batch_mode> <batch_value> <run_uuid> [query]" >&2
  exit 1
fi

LOG_ARG="$1"
BATCH_MODE="$2"
BATCH_VALUE="$3"
RUN_UUID="$4"
QUERY="${5:-all}"

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

BASE_HDFS_DIR="${HDFS_WORK_DIR:-/tmp/nasa-etl/mapreduce/$RUN_UUID}"
INPUT_DIR="$BASE_HDFS_DIR/input"

echo "Staging MapReduce input logs in HDFS..."
"${DFS_CMD[@]}" -rm -r -f "$BASE_HDFS_DIR" >/dev/null 2>&1 || true
"${DFS_CMD[@]}" -mkdir -p "$INPUT_DIR"
for log_file in "${LOG_FILES[@]}"; do
  echo "Uploading $(basename "$log_file") to HDFS"
  "${DFS_CMD[@]}" -put -f "$log_file" "$INPUT_DIR/"
done

rm -rf "$CLASSES_DIR"
mkdir -p "$CLASSES_DIR"
find "$SRC_DIR" -name "*.java" | sort > "$SOURCES_FILE"

echo "Compiling Java MapReduce pipeline..."
javac -source 8 -target 8 -cp "$("$HADOOP_BIN" classpath)" -d "$CLASSES_DIR" @"$SOURCES_FILE"

echo "Packaging MapReduce jar..."
jar cf "$JAR_PATH" -C "$CLASSES_DIR" .

echo "Running Hadoop MapReduce jobs..."
"$HADOOP_BIN" jar "$JAR_PATH" edu.nosql.etl.NasaLogMapReduce \
  -Dmapreduce.framework.name=local \
  "$INPUT_DIR" "$BATCH_MODE" "$BATCH_VALUE" "$RUN_UUID" "$QUERY"
