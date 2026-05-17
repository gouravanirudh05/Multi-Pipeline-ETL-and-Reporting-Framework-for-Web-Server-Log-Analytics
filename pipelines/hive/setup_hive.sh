#!/usr/bin/env bash
# setup_hive.sh — One-time installer for Apache Hive 3.1.3
# -------------------------------------------------------------------
# Installs Hive to ~/hive (matching the ~/hadoop convention).
# Hive 3.1.3 is the last 3.x release, compatible with Hadoop 3.x.
# Uses embedded Derby for the metastore — no external DB needed.
#
# IMPORTANT: Hive 3.1.3 ships guava-19.0.jar which conflicts with
# Hadoop 3.4.x (which uses guava-27.0-jre).  This script fixes
# the conflict by replacing the Hive jar with the Hadoop one.
#
# Usage:
#   chmod +x pipelines/hive/setup_hive.sh
#   ./pipelines/hive/setup_hive.sh
# -------------------------------------------------------------------

set -euo pipefail

HIVE_VERSION="3.1.3"
INSTALL_DIR="$HOME/hive"
TARBALL="apache-hive-${HIVE_VERSION}-bin.tar.gz"
DOWNLOAD_URL="https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/${TARBALL}"

echo "========================================================"
echo " Apache Hive ${HIVE_VERSION} — Setup"
echo "========================================================"

# ── Pre-flight checks ──────────────────────────────────────────────
if ! command -v java &>/dev/null; then
    echo "ERROR: Java is required but not found in PATH."
    echo "  Install with: sudo apt install openjdk-8-jdk"
    exit 1
fi

JAVA_VER=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}')
echo "  Java: $JAVA_VER"

HADOOP_HOME="${HADOOP_HOME:-$HOME/hadoop}"
if [ ! -d "$HADOOP_HOME" ]; then
    echo "ERROR: HADOOP_HOME not found at $HADOOP_HOME"
    echo "  Hive requires Hadoop. Set HADOOP_HOME or install Hadoop."
    exit 1
fi
echo "  Hadoop: $HADOOP_HOME"

# ── Check if already installed ────────────────────────────────────
if [ -d "$INSTALL_DIR" ] && [ -x "$INSTALL_DIR/bin/hive" ]; then
    echo ""
    echo "  Hive is already installed at $INSTALL_DIR"
    HIVE_VER=$("$INSTALL_DIR/bin/hive" --version 2>&1 | head -1 || true)
    echo "  Version: $HIVE_VER"
    echo ""
    echo "  To reinstall, remove ~/hive first:  rm -rf ~/hive"
    echo ""
    echo "  Add to ~/.bashrc if not already:"
    echo "    export HIVE_HOME=\$HOME/hive"
    echo "    export PATH=\$HIVE_HOME/bin:\$PATH"
    exit 0
fi

# ── Download ──────────────────────────────────────────────────────
echo ""
echo "[1/5] Downloading Apache Hive ${HIVE_VERSION}..."

if ! command -v wget &>/dev/null; then
    echo "  wget not found, trying curl..."
    curl -L -o "/tmp/$TARBALL" "$DOWNLOAD_URL"
else
    wget -q --show-progress -O "/tmp/$TARBALL" "$DOWNLOAD_URL"
fi

# ── Extract ───────────────────────────────────────────────────────
echo ""
echo "[2/5] Extracting to $INSTALL_DIR..."

tar -xzf "/tmp/$TARBALL" -C "$HOME"
mv "$HOME/apache-hive-${HIVE_VERSION}-bin" "$INSTALL_DIR"
rm -f "/tmp/$TARBALL"

# ── Fix Guava conflict ───────────────────────────────────────────
echo ""
echo "[3/5] Fixing Guava version conflict with Hadoop..."

# Hive ships guava-19.0.jar, but Hadoop 3.x needs guava-27.0+
# Remove the old one and copy Hadoop's version
HIVE_GUAVA=$(find "$INSTALL_DIR/lib" -name "guava-*.jar" 2>/dev/null | head -1)
HADOOP_GUAVA=$(find "$HADOOP_HOME/share/hadoop/common/lib" -name "guava-*.jar" 2>/dev/null | head -1)

if [ -n "$HIVE_GUAVA" ] && [ -n "$HADOOP_GUAVA" ]; then
    echo "  Removing: $(basename "$HIVE_GUAVA")"
    rm -f "$HIVE_GUAVA"
    echo "  Copying:  $(basename "$HADOOP_GUAVA") from Hadoop"
    cp "$HADOOP_GUAVA" "$INSTALL_DIR/lib/"
elif [ -n "$HIVE_GUAVA" ]; then
    echo "  WARNING: Could not find Hadoop's guava jar. Skipping fix."
    echo "  You may encounter java.lang.NoSuchMethodError at runtime."
else
    echo "  No guava conflict detected."
fi

# Also fix SLF4J duplicate binding warning
SLF4J_LOG4J=$(find "$INSTALL_DIR/lib" -name "log4j-slf4j-impl-*.jar" 2>/dev/null | head -1)
if [ -n "$SLF4J_LOG4J" ]; then
    echo "  Removing duplicate SLF4J binding: $(basename "$SLF4J_LOG4J")"
    mv "$SLF4J_LOG4J" "${SLF4J_LOG4J}.bak"
fi

# ── Initialize Derby metastore ────────────────────────────────────
echo ""
echo "[4/5] Initializing Derby metastore..."

export HIVE_HOME="$INSTALL_DIR"
export PATH="$HIVE_HOME/bin:$PATH"

# Set JAVA_HOME if not already set
if [ -z "${JAVA_HOME:-}" ]; then
    if [ -d "/usr/lib/jvm/java-8-openjdk-amd64" ]; then
        export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"
    elif [ -d "/usr/lib/jvm/java-11-openjdk-amd64" ]; then
        export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
    fi
fi

# schematool initializes the embedded Derby database
"$HIVE_HOME/bin/schematool" -initSchema -dbType derby 2>&1 | tail -5 || {
    echo "  WARNING: schematool returned non-zero. This is often OK if"
    echo "  the metastore was already initialized. Continuing..."
}

# ── Verify ────────────────────────────────────────────────────────
echo ""
echo "[5/5] Verifying installation..."

HIVE_VER=$(hive --version 2>&1 | head -1 || true)
if echo "$HIVE_VER" | grep -qi "hive"; then
    echo "  ✓ $HIVE_VER"
else
    echo "  WARNING: hive --version returned unexpected output:"
    echo "  $HIVE_VER"
    echo "  This may still work. Continuing..."
fi

# ── Instructions ──────────────────────────────────────────────────
echo ""
echo "Add these lines to your ~/.bashrc:"
echo ""
echo "    export HIVE_HOME=\$HOME/hive"
echo "    export PATH=\$HIVE_HOME/bin:\$PATH"
echo ""
echo "  Then run:  source ~/.bashrc"
echo ""
echo "========================================================"
echo " Hive ${HIVE_VERSION} installed successfully at $INSTALL_DIR"
echo "========================================================"
