#!/usr/bin/env bash
# setup_pig.sh — One-time installer for Apache Pig 0.17.0
# -------------------------------------------------------------------
# Installs Pig to ~/pig (matching the ~/hadoop convention).
# Pig 0.17.0 is the last stable release. It includes Jython 2.7
# for Python UDFs. The project runner executes Pig against Hadoop/HDFS.
#
# Usage:
#   chmod +x pipelines/pig/setup_pig.sh
#   ./pipelines/pig/setup_pig.sh
# -------------------------------------------------------------------

set -euo pipefail

PIG_VERSION="0.17.0"
INSTALL_DIR="$HOME/pig"
TARBALL="pig-${PIG_VERSION}.tar.gz"
DOWNLOAD_URL="https://archive.apache.org/dist/pig/pig-${PIG_VERSION}/${TARBALL}"

echo "========================================================"
echo " Apache Pig ${PIG_VERSION} — Setup"
echo "========================================================"

# ── Pre-flight checks ──────────────────────────────────────────────
if ! command -v java &>/dev/null; then
    echo "ERROR: Java is required but not found in PATH."
    echo "  Install with: sudo apt install openjdk-8-jdk"
    exit 1
fi

JAVA_VER=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}')
echo "  Java: $JAVA_VER"

# ── Check if already installed ────────────────────────────────────
if [ -d "$INSTALL_DIR" ] && [ -x "$INSTALL_DIR/bin/pig" ]; then
    echo ""
    echo "  Pig is already installed at $INSTALL_DIR"
    PIG_VER=$("$INSTALL_DIR/bin/pig" -version 2>&1 | head -1 || true)
    echo "  Version: $PIG_VER"
    echo ""
    echo "  To reinstall, remove ~/pig first:  rm -rf ~/pig"
    echo ""
    echo "  Add to ~/.bashrc if not already:"
    echo "    export PIG_HOME=\$HOME/pig"
    echo "    export PATH=\$PIG_HOME/bin:\$PATH"
    exit 0
fi

# ── Download ──────────────────────────────────────────────────────
echo ""
echo "[1/4] Downloading Apache Pig ${PIG_VERSION}..."

if ! command -v wget &>/dev/null; then
    echo "  wget not found, trying curl..."
    curl -L -o "/tmp/$TARBALL" "$DOWNLOAD_URL"
else
    wget -q --show-progress -O "/tmp/$TARBALL" "$DOWNLOAD_URL"
fi

# ── Extract ───────────────────────────────────────────────────────
echo ""
echo "[2/4] Extracting to $INSTALL_DIR..."

tar -xzf "/tmp/$TARBALL" -C "$HOME"
mv "$HOME/pig-${PIG_VERSION}" "$INSTALL_DIR"
rm -f "/tmp/$TARBALL"

# ── Verify ────────────────────────────────────────────────────────
echo ""
echo "[3/4] Verifying installation..."

export PIG_HOME="$INSTALL_DIR"
export PATH="$PIG_HOME/bin:$PATH"

# Set JAVA_HOME if not already set
if [ -z "${JAVA_HOME:-}" ]; then
    if [ -d "/usr/lib/jvm/java-8-openjdk-amd64" ]; then
        export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"
    elif [ -d "/usr/lib/jvm/java-11-openjdk-amd64" ]; then
        export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
    fi
fi

PIG_VER=$(pig -version 2>&1 | head -1 || true)
if echo "$PIG_VER" | grep -q "Apache Pig"; then
    echo "  ✓ $PIG_VER"
else
    echo "  WARNING: pig -version returned unexpected output:"
    echo "  $PIG_VER"
    echo "  This may still work. Continuing..."
fi

# ── Instructions ──────────────────────────────────────────────────
echo ""
echo "[4/4] Add these lines to your ~/.bashrc:"
echo ""
echo "    export PIG_HOME=\$HOME/pig"
echo "    export PATH=\$PIG_HOME/bin:\$PATH"
echo ""
echo "  Then run:  source ~/.bashrc"
echo ""
echo "========================================================"
echo " Pig ${PIG_VERSION} installed successfully at $INSTALL_DIR"
echo "========================================================"
