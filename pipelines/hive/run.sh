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

if [[ "$BATCH_MODE" != "records" && "$BATCH_MODE" != "time" && "$BATCH_MODE" != "calendar_month" ]]; then
    echo "ERROR: batch_mode must be records, time, or calendar_month" >&2
    exit 1
fi
if [[ "$QUERY" != "all" && "$QUERY" != "q1" && "$QUERY" != "q2" && "$QUERY" != "q3" ]]; then
    echo "ERROR: query must be one of all, q1, q2, q3" >&2
    exit 1
fi
if [[ "$BATCH_MODE" == "calendar_month" ]]; then
    BATCH_VALUE=1
elif ! [[ "$BATCH_VALUE" =~ ^[0-9]+$ ]] || [[ "$BATCH_VALUE" -le 0 ]]; then
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

JAVA8_HOME="${JAVA8_HOME:-/usr/lib/jvm/java-8-openjdk-amd64}"
if [[ -d "$JAVA8_HOME" ]]; then
    export JAVA_HOME="$JAVA8_HOME"
    export PATH="$JAVA_HOME/bin:$PATH"
fi

if ! command -v java >/dev/null 2>&1; then
    echo "ERROR: java command not found in PATH." >&2
    exit 1
fi

JAVA_VERSION="$(java -version 2>&1 | awk -F '\"' '/version/ {print $2}')"
if [[ "$JAVA_VERSION" != 1.8* ]]; then
    echo "ERROR: Hive 3.1.3 requires Java 8, but found Java $JAVA_VERSION" >&2
    echo "Install OpenJDK 8 and set JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 before running Hive." >&2
    exit 1
fi

# Some environments inherit Hive/Hadoop debug flags that attach JDWP on port 8000.
# Clear them for non-interactive ETL runs so Hive starts normally.
unset DEBUG
unset HIVE_MAIN_CLIENT_DEBUG_OPTS
unset HIVE_CHILD_CLIENT_DEBUG_OPTS
unset HIVE_DEBUG_RECURSIVE
for var_name in HADOOP_CLIENT_OPTS HADOOP_OPTS HIVE_OPTS JAVA_TOOL_OPTIONS JDK_JAVA_OPTIONS; do
    current_value="${!var_name-}"
    if [[ -n "$current_value" && "$current_value" == *"jdwp"* ]]; then
        unset "$var_name"
    fi
done

# Hive local MapReduce needs more headroom than the default 256 MB on this dataset.
export HADOOP_HEAPSIZE="${HADOOP_HEAPSIZE:-2048}"
export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS:-} -Xmx2048m"

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

LOCAL_STAGE_DIR="$(mktemp -d /tmp/nasa-hive-input.XXXXXX)"
TEMP_HADOOP_CONF_DIR="$(mktemp -d /tmp/nasa-hive-conf.XXXXXX)"
LOCAL_HIVE_STATE_DIR="${HIVE_STATE_DIR:-$HOME/.hive}"
LOCAL_HIVE_WAREHOUSE_DIR="${LOCAL_HIVE_STATE_DIR}/warehouse"
LOCAL_HIVE_SCRATCH_DIR="${LOCAL_HIVE_STATE_DIR}/scratch"
LOCAL_HIVE_TMP_DIR="${LOCAL_HIVE_STATE_DIR}/tmp"
LOCAL_HIVE_METASTORE_DIR="${LOCAL_HIVE_STATE_DIR}/metastore_db"

BASE_HDFS_DIR="${HDFS_WORK_DIR:-/tmp/nasa-etl/hive/$RUN_UUID}"
INPUT_DIR="$BASE_HDFS_DIR/input"
OUTPUT_DIR="$BASE_HDFS_DIR/output"
DATABASE="nasa_etl_$(sanitize_name "$RUN_UUID")"
LOCAL_OUTPUT="$(mktemp -d)"
START_SECONDS="$(date +%s)"

cleanup() {
    rm -rf "$LOCAL_OUTPUT"
    rm -rf "$LOCAL_STAGE_DIR"
    rm -rf "$TEMP_HADOOP_CONF_DIR"
}
trap cleanup EXIT

echo "Hive ETL started"
echo "Run UUID: $RUN_UUID"
echo "Batch mode: $BATCH_MODE"
echo "Batch value: $BATCH_VALUE"
echo "Aggregation mode: ${AGGREGATION_MODE:-global}"
echo "Input HDFS directory: $INPUT_DIR"

SOURCE_HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-${HADOOP_HOME:-$HOME/hadoop}/etc/hadoop}"
if [[ -d "$SOURCE_HADOOP_CONF_DIR" ]]; then
    cp -a "$SOURCE_HADOOP_CONF_DIR/." "$TEMP_HADOOP_CONF_DIR/"
    if [[ -f "$TEMP_HADOOP_CONF_DIR/hadoop-env.sh" ]]; then
        sed -i '/^[[:space:]]*export JAVA_HOME=/d' "$TEMP_HADOOP_CONF_DIR/hadoop-env.sh"
        printf '\nexport JAVA_HOME=%q\n' "$JAVA_HOME" >> "$TEMP_HADOOP_CONF_DIR/hadoop-env.sh"
    fi
fi

mkdir -p "$LOCAL_HIVE_WAREHOUSE_DIR" "$LOCAL_HIVE_SCRATCH_DIR" "$LOCAL_HIVE_TMP_DIR"
cat > "$TEMP_HADOOP_CONF_DIR/hive-site.xml" <<EOF
<?xml version="1.0"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
  <property>
    <name>mapreduce.framework.name</name>
    <value>local</value>
  </property>
  <property>
    <name>hive.exec.mode.local.auto</name>
    <value>true</value>
  </property>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>/user/hive/warehouse</value>
  </property>
  <property>
    <name>hive.exec.scratchdir</name>
    <value>/tmp/hive</value>
  </property>
  <property>
    <name>hive.exec.local.scratchdir</name>
    <value>${LOCAL_HIVE_SCRATCH_DIR}</value>
  </property>
  <property>
    <name>hive.downloaded.resources.dir</name>
    <value>${LOCAL_HIVE_TMP_DIR}</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:derby:;databaseName=${LOCAL_HIVE_METASTORE_DIR};create=true</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>org.apache.derby.jdbc.EmbeddedDriver</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>APP</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>mine</value>
  </property>
  <property>
    <name>datanucleus.schema.autoCreateAll</name>
    <value>true</value>
  </property>
  <property>
    <name>hive.metastore.schema.verification</name>
    <value>false</value>
  </property>
</configuration>
EOF
export HADOOP_CONF_DIR="$TEMP_HADOOP_CONF_DIR"
export HIVE_CONF_DIR="$TEMP_HADOOP_CONF_DIR"

if [[ ! -d "$LOCAL_HIVE_METASTORE_DIR" ]] && [[ -x "${HIVE_HOME:-$(cd "$(dirname "$HIVE_BIN")/.." && pwd)}/bin/schematool" ]]; then
    SCHEMATOOL_BIN="${HIVE_HOME:-$(cd "$(dirname "$HIVE_BIN")/.." && pwd)}/bin/schematool"
    echo "Initializing embedded Hive metastore"
    "$SCHEMATOOL_BIN" -dbType derby -initSchema >/dev/null 2>&1 || true
fi

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

run_hql() {
    local script_path="$1"
    echo "Running Hive script: $(basename "$script_path")"
    "$HIVE_BIN" \
        --hiveconf "mapreduce.framework.name=local" \
        --hiveconf "hive.exec.mode.local.auto=true" \
        --hiveconf "mapreduce.task.io.sort.mb=32" \
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
    run_hql "$SCRIPT_DIR/q1_daily_traffic$( [[ "${AGGREGATION_MODE:-global}" == "per_batch" ]] && printf "_per_batch" ).hql"
fi
if [[ "$QUERY" == "all" || "$QUERY" == "q2" ]]; then
    run_hql "$SCRIPT_DIR/q2_top_resources$( [[ "${AGGREGATION_MODE:-global}" == "per_batch" ]] && printf "_per_batch" ).hql"
fi
if [[ "$QUERY" == "all" || "$QUERY" == "q3" ]]; then
    run_hql "$SCRIPT_DIR/q3_hourly_errors$( [[ "${AGGREGATION_MODE:-global}" == "per_batch" ]] && printf "_per_batch" ).hql"
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
    "hive" "$RUN_UUID" "$BATCH_MODE" "$BATCH_VALUE" "$LOCAL_OUTPUT" "${AGGREGATION_MODE:-global}" "$QUERY" "$RUNTIME_SECONDS"

echo "Hive ETL completed"
echo "Output HDFS directory: $OUTPUT_DIR"
