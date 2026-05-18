#!/usr/bin/env bash
set -euo pipefail

PIPELINE="$1"
RUN_UUID="$2"
BATCH_MODE="$3"
BATCH_VALUE="$4"
OUTPUT_DIR="$5"
QUERY="${6:-all}"
RUNTIME_SECONDS="${7:-0}"

PGHOST="${PGHOST:-127.0.0.1}"
PGPORT="${PGPORT:-5432}"
PGDATABASE="${PGDATABASE:-nosql_etl_db}"
PGUSER="${PGUSER:-sathish}"
PGPASSWORD="${PGPASSWORD:-welcome}"
export PGPASSWORD

SQL_FILE="$(mktemp)"

cat > "$SQL_FILE" <<SQL
\\set ON_ERROR_STOP on

CREATE TABLE IF NOT EXISTS etl_runs (
    run_uuid VARCHAR(64) PRIMARY KEY,
    pipeline VARCHAR(20),
    batch_size INTEGER,
    total_records INTEGER,
    total_batches INTEGER,
    avg_batch_size NUMERIC(10,2),
    malformed_count INTEGER,
    runtime_seconds NUMERIC(10,3),
    status VARCHAR(20),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);
ALTER TABLE etl_runs ADD COLUMN IF NOT EXISTS batch_mode VARCHAR(20) DEFAULT 'records';
ALTER TABLE etl_runs ADD COLUMN IF NOT EXISTS batch_interval_seconds INTEGER;

CREATE TABLE IF NOT EXISTS run_metadata (
    run_id VARCHAR(64) PRIMARY KEY,
    pipeline_name VARCHAR(20),
    query_name VARCHAR(20),
    batch_size INTEGER,
    average_batch_size NUMERIC(10,2),
    records_processed INTEGER,
    malformed_record_count INTEGER,
    runtime NUMERIC(10,3),
    execution_timestamp TIMESTAMPTZ,
    status VARCHAR(20),
    batch_mode VARCHAR(20),
    batch_interval_seconds INTEGER,
    total_batches INTEGER
);

CREATE TABLE IF NOT EXISTS batch_metadata (
    id SERIAL PRIMARY KEY,
    run_uuid VARCHAR(64),
    pipeline VARCHAR(20),
    batch_id INTEGER,
    batch_size INTEGER,
    records_processed INTEGER,
    malformed_count INTEGER DEFAULT 0,
    started_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS malformed_record_summary (
    id SERIAL PRIMARY KEY,
    run_uuid VARCHAR(64),
    pipeline VARCHAR(20),
    batch_id INTEGER,
    malformed_count INTEGER,
    recorded_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS malformed_records (
    id SERIAL PRIMARY KEY,
    run_uuid VARCHAR(64),
    pipeline VARCHAR(20),
    batch_id INTEGER,
    raw_line TEXT,
    reason TEXT,
    recorded_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS query_results (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(64),
    pipeline_name VARCHAR(20),
    query_name VARCHAR(20),
    batch_id INTEGER,
    result_key TEXT,
    result_value JSONB,
    execution_timestamp TIMESTAMPTZ DEFAULT NOW()
);
ALTER TABLE query_results ADD COLUMN IF NOT EXISTS result_scope VARCHAR(20) DEFAULT 'aggregate';
CREATE TABLE IF NOT EXISTS daily_traffic (
    id SERIAL PRIMARY KEY,
    pipeline VARCHAR(20),
    run_uuid VARCHAR(64),
    batch_id INTEGER,
    executed_at TIMESTAMPTZ DEFAULT NOW(),
    log_date DATE,
    status_code INTEGER,
    request_count BIGINT,
    total_bytes BIGINT
);
ALTER TABLE daily_traffic ADD COLUMN IF NOT EXISTS result_scope VARCHAR(20) DEFAULT 'aggregate';
CREATE TABLE IF NOT EXISTS top_resources (
    id SERIAL PRIMARY KEY,
    pipeline VARCHAR(20),
    run_uuid VARCHAR(64),
    batch_id INTEGER,
    executed_at TIMESTAMPTZ DEFAULT NOW(),
    resource_path TEXT,
    request_count BIGINT,
    total_bytes BIGINT,
    distinct_host_count BIGINT
);
ALTER TABLE top_resources ADD COLUMN IF NOT EXISTS result_scope VARCHAR(20) DEFAULT 'aggregate';
CREATE TABLE IF NOT EXISTS hourly_errors (
    id SERIAL PRIMARY KEY,
    pipeline VARCHAR(20),
    run_uuid VARCHAR(64),
    batch_id INTEGER,
    executed_at TIMESTAMPTZ DEFAULT NOW(),
    log_date DATE,
    log_hour SMALLINT,
    error_request_count BIGINT,
    total_request_count BIGINT,
    error_rate NUMERIC(6,4),
    distinct_error_hosts BIGINT
);
ALTER TABLE hourly_errors ADD COLUMN IF NOT EXISTS result_scope VARCHAR(20) DEFAULT 'aggregate';
CREATE INDEX IF NOT EXISTS idx_batch_run ON batch_metadata(run_uuid, batch_id);
CREATE INDEX IF NOT EXISTS idx_run_metadata_run ON run_metadata(run_id);
CREATE INDEX IF NOT EXISTS idx_query_results_run ON query_results(run_id, query_name);
CREATE INDEX IF NOT EXISTS idx_malformed_summary_run ON malformed_record_summary(run_uuid, batch_id);
CREATE INDEX IF NOT EXISTS idx_malformed_records_run ON malformed_records(run_uuid, batch_id);
CREATE INDEX IF NOT EXISTS idx_daily_pipeline_date ON daily_traffic(pipeline, log_date);
CREATE INDEX IF NOT EXISTS idx_resource_pipeline ON top_resources(pipeline, request_count DESC);
CREATE INDEX IF NOT EXISTS idx_error_pipeline_date ON hourly_errors(pipeline, log_date, log_hour);

INSERT INTO etl_runs
    (run_uuid, pipeline, batch_size, batch_mode, batch_interval_seconds, status, started_at)
VALUES
    ('$RUN_UUID', '$PIPELINE', $(if [[ "$BATCH_MODE" == "records" ]]; then printf "%s" "$BATCH_VALUE"; else printf "NULL"; fi), '$BATCH_MODE', $(if [[ "$BATCH_MODE" == "time" ]]; then printf "%s" "$BATCH_VALUE"; else printf "NULL"; fi), 'running', NOW())
ON CONFLICT (run_uuid) DO UPDATE SET
    pipeline = EXCLUDED.pipeline,
    batch_size = EXCLUDED.batch_size,
    batch_mode = EXCLUDED.batch_mode,
    batch_interval_seconds = EXCLUDED.batch_interval_seconds,
    status = EXCLUDED.status;

DELETE FROM daily_traffic WHERE run_uuid = '$RUN_UUID';
DELETE FROM top_resources WHERE run_uuid = '$RUN_UUID';
DELETE FROM hourly_errors WHERE run_uuid = '$RUN_UUID';
DELETE FROM batch_metadata WHERE run_uuid = '$RUN_UUID';
DELETE FROM malformed_record_summary WHERE run_uuid = '$RUN_UUID';
DELETE FROM malformed_records WHERE run_uuid = '$RUN_UUID';
DELETE FROM query_results WHERE run_id = '$RUN_UUID';

CREATE TEMP TABLE tmp_batch_metadata (
    batch_id INTEGER,
    batch_size INTEGER,
    records_processed BIGINT,
    malformed_count BIGINT
);
CREATE TEMP TABLE tmp_malformed_records (
    batch_id INTEGER,
    raw_line TEXT,
    reason TEXT
);
CREATE TEMP TABLE tmp_q1 (
    batch_id INTEGER,
    log_date DATE,
    status_code INTEGER,
    request_count BIGINT,
    total_bytes BIGINT
);
CREATE TEMP TABLE tmp_q2 (
    batch_id INTEGER,
    resource_path TEXT,
    request_count BIGINT,
    total_bytes BIGINT,
    distinct_host_count BIGINT
);
CREATE TEMP TABLE tmp_q3 (
    batch_id INTEGER,
    log_date DATE,
    log_hour SMALLINT,
    error_request_count BIGINT,
    total_request_count BIGINT,
    error_rate NUMERIC,
    distinct_error_hosts BIGINT
);
SQL

copy_if_present() {
    local table="$1"
    local file="$2"
    if [[ -s "$file" ]]; then
        printf "\\copy %s FROM '%s' WITH (FORMAT csv, DELIMITER E'\\t', QUOTE E'\\b')\n" "$table" "$file" >> "$SQL_FILE"
    fi
}

copy_if_present tmp_batch_metadata "$OUTPUT_DIR/batch_metadata.tsv"
copy_if_present tmp_malformed_records "$OUTPUT_DIR/malformed_records.tsv"
[[ "$QUERY" == "all" || "$QUERY" == "q1" ]] && copy_if_present tmp_q1 "$OUTPUT_DIR/q1.tsv"
[[ "$QUERY" == "all" || "$QUERY" == "q2" ]] && copy_if_present tmp_q2 "$OUTPUT_DIR/q2.tsv"
[[ "$QUERY" == "all" || "$QUERY" == "q3" ]] && copy_if_present tmp_q3 "$OUTPUT_DIR/q3.tsv"

cat >> "$SQL_FILE" <<SQL
INSERT INTO batch_metadata
    (run_uuid, pipeline, batch_id, batch_size, records_processed, malformed_count)
SELECT '$RUN_UUID', '$PIPELINE', batch_id, batch_size, records_processed, malformed_count
FROM tmp_batch_metadata;

INSERT INTO malformed_record_summary
    (run_uuid, pipeline, batch_id, malformed_count)
SELECT '$RUN_UUID', '$PIPELINE', batch_id, malformed_count
FROM tmp_batch_metadata
WHERE malformed_count > 0;

INSERT INTO malformed_records
    (run_uuid, pipeline, batch_id, raw_line, reason)
SELECT '$RUN_UUID', '$PIPELINE', batch_id, raw_line, reason
FROM tmp_malformed_records;

INSERT INTO daily_traffic
    (pipeline, run_uuid, batch_id, result_scope, log_date, status_code, request_count, total_bytes)
SELECT '$PIPELINE', '$RUN_UUID', batch_id, 'aggregate',
       log_date, status_code, request_count, total_bytes
FROM tmp_q1
WHERE batch_id = 0;

INSERT INTO daily_traffic
    (pipeline, run_uuid, batch_id, result_scope, log_date, status_code, request_count, total_bytes)
SELECT '$PIPELINE', '$RUN_UUID', batch_id, 'per_batch',
       log_date, status_code, request_count, total_bytes
FROM tmp_q1
WHERE batch_id <> 0;

INSERT INTO top_resources
    (pipeline, run_uuid, batch_id, result_scope, resource_path, request_count, total_bytes, distinct_host_count)
SELECT '$PIPELINE', '$RUN_UUID', batch_id, 'aggregate',
       resource_path, request_count, total_bytes, distinct_host_count
FROM tmp_q2
WHERE batch_id = 0;

INSERT INTO top_resources
    (pipeline, run_uuid, batch_id, result_scope, resource_path, request_count, total_bytes, distinct_host_count)
SELECT '$PIPELINE', '$RUN_UUID', batch_id, 'per_batch',
       resource_path, request_count, total_bytes, distinct_host_count
FROM tmp_q2
WHERE batch_id <> 0;

INSERT INTO hourly_errors
    (pipeline, run_uuid, batch_id, result_scope, log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts)
SELECT '$PIPELINE', '$RUN_UUID', batch_id, 'aggregate',
       log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts
FROM tmp_q3
WHERE batch_id = 0;

INSERT INTO hourly_errors
    (pipeline, run_uuid, batch_id, result_scope, log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts)
SELECT '$PIPELINE', '$RUN_UUID', batch_id, 'per_batch',
       log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts
FROM tmp_q3
WHERE batch_id <> 0;

UPDATE etl_runs
SET total_records = COALESCE((SELECT sum(records_processed) FROM tmp_batch_metadata), 0),
    total_batches = COALESCE((SELECT count(*) FROM tmp_batch_metadata), 0),
    avg_batch_size = COALESCE((SELECT avg(records_processed) FROM tmp_batch_metadata), 0),
    malformed_count = COALESCE((SELECT sum(malformed_count) FROM tmp_batch_metadata), 0),
    runtime_seconds = $RUNTIME_SECONDS,
    status = 'completed',
    completed_at = NOW(),
    batch_mode = '$BATCH_MODE',
    batch_interval_seconds = $(if [[ "$BATCH_MODE" == "time" ]]; then printf "%s" "$BATCH_VALUE"; else printf "NULL"; fi)
WHERE run_uuid = '$RUN_UUID';

INSERT INTO run_metadata (
    run_id, pipeline_name, query_name, batch_size, average_batch_size,
    records_processed, malformed_record_count, runtime, execution_timestamp,
    status, batch_mode, batch_interval_seconds, total_batches
)
SELECT run_uuid, pipeline, '$QUERY', batch_size, avg_batch_size, total_records,
       malformed_count, runtime_seconds, COALESCE(started_at, NOW()), status,
       batch_mode, batch_interval_seconds, total_batches
FROM etl_runs
WHERE run_uuid = '$RUN_UUID'
ON CONFLICT (run_id) DO UPDATE SET
    pipeline_name = EXCLUDED.pipeline_name,
    query_name = EXCLUDED.query_name,
    batch_size = EXCLUDED.batch_size,
    average_batch_size = EXCLUDED.average_batch_size,
    records_processed = EXCLUDED.records_processed,
    malformed_record_count = EXCLUDED.malformed_record_count,
    runtime = EXCLUDED.runtime,
    execution_timestamp = EXCLUDED.execution_timestamp,
    status = EXCLUDED.status,
    batch_mode = EXCLUDED.batch_mode,
    batch_interval_seconds = EXCLUDED.batch_interval_seconds,
    total_batches = EXCLUDED.total_batches;

INSERT INTO query_results (run_id, pipeline_name, query_name, batch_id, result_scope, result_key, result_value)
SELECT '$RUN_UUID', '$PIPELINE', 'q1_daily_traffic', batch_id, result_scope,
       concat(log_date::text, ':', status_code::text),
       jsonb_build_object('log_date', log_date, 'status_code', status_code, 'request_count', request_count, 'total_bytes', total_bytes)
FROM daily_traffic
WHERE run_uuid = '$RUN_UUID';

INSERT INTO query_results (run_id, pipeline_name, query_name, batch_id, result_scope, result_key, result_value)
SELECT '$RUN_UUID', '$PIPELINE', 'q2_top_resources', batch_id, result_scope,
       resource_path,
       jsonb_build_object('resource_path', resource_path, 'request_count', request_count, 'total_bytes', total_bytes, 'distinct_host_count', distinct_host_count)
FROM top_resources
WHERE run_uuid = '$RUN_UUID';

INSERT INTO query_results (run_id, pipeline_name, query_name, batch_id, result_scope, result_key, result_value)
SELECT '$RUN_UUID', '$PIPELINE', 'q3_hourly_errors', batch_id, result_scope,
       concat(log_date::text, ':', log_hour::text),
       jsonb_build_object('log_date', log_date, 'log_hour', log_hour, 'error_request_count', error_request_count, 'total_request_count', total_request_count, 'error_rate', error_rate, 'distinct_error_hosts', distinct_error_hosts)
FROM hourly_errors
WHERE run_uuid = '$RUN_UUID';
SQL

psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -f "$SQL_FILE"
rm -f "$SQL_FILE"
