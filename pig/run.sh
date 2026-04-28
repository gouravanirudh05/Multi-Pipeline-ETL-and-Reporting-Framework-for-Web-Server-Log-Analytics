#!/usr/bin/env bash
# run.sh
# -----------------------------------------------------------------------
# One-shot setup + run script for the Pig ETL pipeline.
#
# USAGE:
#   chmod +x run.sh
#   ./run.sh <log_file_path> <batch_size>
#
# EXAMPLE:
#   ./run.sh /data/NASA_access_log_Jul95 10000
#
# WHAT THIS SCRIPT DOES (in order):
#   Phase 1 — System checks  : Verify OS packages (Java, wget, python3)
#   Phase 2 — Pig install    : Download and unpack Apache Pig 0.17.0
#                              if not already present
#   Phase 3 — Python deps    : Install psycopg2-binary via pip3
#   Phase 4 — Env export     : Set JAVA_HOME, PIG_HOME, PATH for this
#                              shell session
#   Phase 5 — Smoke test     : Run `pig -version` to confirm Pig works
#   Phase 6 — Pipeline run   : Execute orchestrator.py with your args
#
# NOTES:
#   - Pig 0.17.0 is the last stable release; it bundles Jython 2.7
#     which is required for Python UDFs.
#   - Local mode (-x local) means NO Hadoop cluster is needed.
#     Pig reads/writes directly from the local filesystem.
#   - This script is idempotent: running it again skips the download
#     if pig/ directory already exists beside this script.
# -----------------------------------------------------------------------

set -euo pipefail   # exit on error, unset var, or pipe failure

# -----------------------------------------------------------------------
# 0. Argument validation
# -----------------------------------------------------------------------
if [ "$#" -lt 2 ]; then
    echo "Usage: ./run.sh <log_file_path> <batch_size>"
    echo "Example: ./run.sh /data/NASA_access_log_Jul95 10000"
    exit 1
fi

LOG_FILE="$1"
BATCH_SIZE="$2"

if [ ! -f "$LOG_FILE" ]; then
    echo "ERROR: Log file not found: $LOG_FILE"
    exit 1
fi

# Resolve the directory where this script lives so all relative paths work
# regardless of where you call ./run.sh from.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIG_DIR="$SCRIPT_DIR/pig"

echo "========================================================"
echo " Pig ETL Pipeline — NASA HTTP Log Analytics"
echo "========================================================"
echo " Script dir : $SCRIPT_DIR"
echo " Log file   : $LOG_FILE"
echo " Batch size : $BATCH_SIZE"
echo "========================================================"

# -----------------------------------------------------------------------
# Phase 1 — System package checks
# -----------------------------------------------------------------------
echo ""
echo "[Phase 1] Checking system dependencies..."

# Check Java — Pig requires Java 8 or 11.
# We prefer Java 11 (openjdk-11-jdk) on Ubuntu.
if ! command -v java &>/dev/null; then
    echo "  Java not found. Installing openjdk-11-jdk..."
    sudo apt-get update -qq
    sudo apt-get install -y openjdk-11-jdk
else
    JAVA_VER=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}')
    echo "  Java found: $JAVA_VER"
fi

# Check wget (needed to download Pig)
if ! command -v wget &>/dev/null; then
    echo "  wget not found. Installing..."
    sudo apt-get install -y wget
fi

# Check python3
if ! command -v python3 &>/dev/null; then
    echo "  python3 not found. Installing..."
    sudo apt-get install -y python3 python3-pip
else
    echo "  Python3 found: $(python3 --version)"
fi

# -----------------------------------------------------------------------
# Phase 2 — Download and unpack Apache Pig 0.17.0
# -----------------------------------------------------------------------
echo ""
echo "[Phase 2] Setting up Apache Pig..."

PIG_VERSION="0.17.0"
PIG_TARBALL="pig-${PIG_VERSION}.tar.gz"
# Apache mirror — change to a closer mirror if this is slow
PIG_URL="https://archive.apache.org/dist/pig/pig-${PIG_VERSION}/${PIG_TARBALL}"

if [ -d "$PIG_DIR" ]; then
    echo "  Pig directory already exists at $PIG_DIR — skipping download."
else
    echo "  Downloading Pig ${PIG_VERSION}..."
    wget -q --show-progress -O "/tmp/$PIG_TARBALL" "$PIG_URL"

    echo "  Unpacking..."
    # Pig tarball extracts to pig-0.17.0/; rename it to pig/ for simplicity
    tar -xzf "/tmp/$PIG_TARBALL" -C "$SCRIPT_DIR"
    mv "$SCRIPT_DIR/pig-${PIG_VERSION}" "$PIG_DIR"
    rm "/tmp/$PIG_TARBALL"
    echo "  Pig installed at $PIG_DIR"
fi

# -----------------------------------------------------------------------
# Phase 3 — Python dependencies
# -----------------------------------------------------------------------
echo ""
echo "[Phase 3] Installing Python dependencies..."

# psycopg2-binary is the PostgreSQL adapter for Python.
# --break-system-packages is required on Ubuntu 23+ where pip is
# managed by the system package manager.
pip3 install --quiet psycopg2-binary --break-system-packages
echo "  psycopg2-binary installed."

# -----------------------------------------------------------------------
# Phase 4 — Export environment variables for this session
# -----------------------------------------------------------------------
echo ""
echo "[Phase 4] Configuring environment..."

# Locate JAVA_HOME automatically
# `update-java-alternatives` is the Ubuntu way to find the active JVM.
if [ -z "${JAVA_HOME:-}" ]; then
    # Try the standard Ubuntu path for OpenJDK 11
    if [ -d "/usr/lib/jvm/java-11-openjdk-amd64" ]; then
        export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
    elif [ -d "/usr/lib/jvm/java-11-openjdk-arm64" ]; then
        # ARM64 (e.g. Raspberry Pi / Apple Silicon emulation)
        export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-arm64"
    else
        # Fallback: ask java itself where it lives
        export JAVA_HOME="$(java -XshowSettings:all -version 2>&1 \
            | grep 'java.home' | awk '{print $3}')"
    fi
fi

export PIG_HOME="$PIG_DIR"
# Prepend Pig's bin to PATH so `pig` resolves correctly
export PATH="$PIG_HOME/bin:$PATH"

echo "  JAVA_HOME = $JAVA_HOME"
echo "  PIG_HOME  = $PIG_HOME"
echo ""
echo "  TIP: To make these permanent, add the following to ~/.bashrc:"
echo "    export JAVA_HOME=$JAVA_HOME"
echo "    export PIG_HOME=$PIG_HOME"
echo "    export PATH=\$PIG_HOME/bin:\$PATH"

# -----------------------------------------------------------------------
# Phase 5 — Smoke test: confirm Pig can start
# -----------------------------------------------------------------------
echo ""
echo "[Phase 5] Smoke-testing Pig installation..."

PIG_VERSION_OUTPUT=$(pig -version 2>&1 | head -1)
if echo "$PIG_VERSION_OUTPUT" | grep -q "Apache Pig"; then
    echo "  OK: $PIG_VERSION_OUTPUT"
else
    echo "  ERROR: Pig smoke test failed. Output was:"
    echo "  $PIG_VERSION_OUTPUT"
    echo ""
    echo "  Common fixes:"
    echo "    1. Make sure JAVA_HOME points to a valid JDK (not JRE)."
    echo "    2. Check that $PIG_HOME/bin/pig is executable:"
    echo "       chmod +x $PIG_HOME/bin/pig"
    exit 1
fi

# -----------------------------------------------------------------------
# Phase 6 — Run the pipeline
# -----------------------------------------------------------------------
echo ""
echo "[Phase 6] Running ETL pipeline..."
echo ""

python3 "$SCRIPT_DIR/orchestrator.py" "$LOG_FILE" "$BATCH_SIZE"