#!/usr/bin/env bash
set -euo pipefail

export HADOOP_OPTS="${HADOOP_OPTS:-} -Djava.library.path=${HADOOP_HOME}/lib/native"

wait_for_postgres() {
  until PGPASSWORD="${PGPASSWORD:-welcome}" pg_isready \
      -h "${PGHOST:-postgres}" \
      -p "${PGPORT:-5432}" \
      -U "${PGUSER:-sathish}" \
      -d "${PGDATABASE:-nosql_etl_db}" >/dev/null 2>&1; do
    echo "Waiting for PostgreSQL..."
    sleep 2
  done
}

format_hdfs_once() {
  if [[ ! -f /hadoop-data/dfs/name/current/VERSION ]]; then
    echo "Formatting HDFS namenode..."
    hdfs namenode -format -force -nonInteractive
  fi
}

start_hdfs_only() {
  echo "Starting HDFS only; YARN is intentionally not started."
  hdfs --daemon start namenode
  hdfs --daemon start datanode

  until hdfs dfs -ls / >/dev/null 2>&1; do
    echo "Waiting for HDFS..."
    sleep 2
  done

  hdfs dfs -mkdir -p /tmp/nasa-etl /tmp/hive /user/hive/warehouse || true
  hdfs dfs -chmod -R 1777 /tmp/hive || true
}

wait_for_postgres
format_hdfs_once
start_hdfs_only

exec uvicorn backend.server:app --host 0.0.0.0 --port 5050
