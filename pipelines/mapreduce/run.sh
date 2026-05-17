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

if ! command -v javac >/dev/null 2>&1; then
  echo "ERROR: javac command not found in PATH" >&2
  exit 1
fi

rm -rf "$CLASSES_DIR"
mkdir -p "$CLASSES_DIR"
find "$SRC_DIR" -name "*.java" | sort > "$SOURCES_FILE"

echo "Compiling Java MapReduce pipeline..."
javac -source 8 -target 8 -cp "$("$HADOOP_BIN" classpath)" -d "$CLASSES_DIR" @"$SOURCES_FILE"

echo "Packaging MapReduce jar..."
jar cf "$JAR_PATH" -C "$CLASSES_DIR" .

echo "Running Hadoop MapReduce jobs..."
"$HADOOP_BIN" jar "$JAR_PATH" edu.nosql.etl.NasaLogMapReduce "$@"
