from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import subprocess
import json
import psycopg2
import psycopg2.extras
import os
import sys
import uuid as uuid_lib
from datetime import datetime

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))

DB_CONFIG = {
    "dbname": os.environ.get("PGDATABASE", "nosql_etl_db"),
    "user": os.environ.get("PGUSER", "sathish"),
    "password": os.environ.get("PGPASSWORD", "welcome"),
    "host": os.environ.get("PGHOST", "localhost"),
    "port": int(os.environ.get("PGPORT", "5432")),
}

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

def _build_pipeline_env():
    """Build env dict ensuring JAVA_HOME, HADOOP_HOME, PIG_HOME, HIVE_HOME are set."""
    env = os.environ.copy()
    home = os.path.expanduser("~")
    defaults = {
        "JAVA_HOME": ["/usr/lib/jvm/java-8-openjdk-amd64", "/usr/lib/jvm/java-11-openjdk-amd64"],
        "HADOOP_HOME": [os.path.join(home, "hadoop")],
        "PIG_HOME": [os.path.join(home, "pig")],
        "HIVE_HOME": [os.path.join(home, "hive")],
    }
    for var, candidates in defaults.items():
        if var not in env:
            for path in candidates:
                if os.path.isdir(path):
                    env[var] = path
                    break
    # Ensure tool bin dirs are on PATH
    extra_paths = []
    for var in ["HADOOP_HOME", "PIG_HOME", "HIVE_HOME", "JAVA_HOME"]:
        if var in env:
            extra_paths.append(os.path.join(env[var], "bin"))
    if extra_paths:
        env["PATH"] = ":".join(extra_paths) + ":" + env.get("PATH", "/usr/bin:/bin")
    return env

def initialize_postgres():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
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
        )
    """)
    cur.execute("""
        ALTER TABLE etl_runs
        ADD COLUMN IF NOT EXISTS batch_mode VARCHAR(20) DEFAULT 'records'
    """)
    cur.execute("""
        ALTER TABLE etl_runs
        ADD COLUMN IF NOT EXISTS batch_interval_seconds INTEGER
    """)
    cur.execute("""
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
        )
    """)
    cur.execute("""
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
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS malformed_record_summary (
            id SERIAL PRIMARY KEY,
            run_uuid VARCHAR(64),
            pipeline VARCHAR(20),
            batch_id INTEGER,
            malformed_count INTEGER,
            recorded_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS query_results (
            id SERIAL PRIMARY KEY,
            run_id VARCHAR(64),
            pipeline_name VARCHAR(20),
            query_name VARCHAR(20),
            batch_id INTEGER,
            result_key TEXT,
            result_value JSONB,
            execution_timestamp TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    cur.execute("""
        ALTER TABLE query_results
        ADD COLUMN IF NOT EXISTS result_scope VARCHAR(20) DEFAULT 'aggregate'
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS malformed_records (
            id SERIAL PRIMARY KEY,
            run_uuid VARCHAR(64),
            pipeline VARCHAR(20),
            batch_id INTEGER,
            raw_line TEXT,
            reason TEXT,
            recorded_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    cur.execute("""
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
        )
    """)
    cur.execute("""
        ALTER TABLE daily_traffic
        ADD COLUMN IF NOT EXISTS result_scope VARCHAR(20) DEFAULT 'aggregate'
    """)
    cur.execute("""
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
        )
    """)
    cur.execute("""
        ALTER TABLE top_resources
        ADD COLUMN IF NOT EXISTS result_scope VARCHAR(20) DEFAULT 'aggregate'
    """)
    cur.execute("""
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
        )
    """)
    cur.execute("""
        ALTER TABLE hourly_errors
        ADD COLUMN IF NOT EXISTS result_scope VARCHAR(20) DEFAULT 'aggregate'
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_batch_run ON batch_metadata(run_uuid, batch_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_run_metadata_run ON run_metadata(run_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_query_results_run ON query_results(run_id, query_name)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_malformed_summary_run ON malformed_record_summary(run_uuid, batch_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_malformed_records_run ON malformed_records(run_uuid, batch_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_daily_pipeline_date ON daily_traffic(pipeline, log_date)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_resource_pipeline ON top_resources(pipeline, request_count DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_error_pipeline_date ON hourly_errors(pipeline, log_date, log_hour)")
    conn.commit()
    cur.close()
    conn.close()

def sync_reporting_tables(run_uuid):
    """Populate evaluator-facing run_metadata and query_results from detailed tables."""
    initialize_postgres()
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO run_metadata (
            run_id, pipeline_name, query_name, batch_size, average_batch_size,
            records_processed, malformed_record_count, runtime,
            execution_timestamp, status, batch_mode, batch_interval_seconds,
            total_batches
        )
        SELECT
            run_uuid,
            pipeline,
            'all',
            batch_size,
            avg_batch_size,
            total_records,
            malformed_count,
            runtime_seconds,
            COALESCE(started_at, NOW()),
            status,
            batch_mode,
            batch_interval_seconds,
            total_batches
        FROM etl_runs
        WHERE run_uuid = %s
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
            total_batches = EXCLUDED.total_batches
    """, (run_uuid,))

    cur.execute("DELETE FROM query_results WHERE run_id = %s", (run_uuid,))
    cur.execute("""
        INSERT INTO query_results (
            run_id, pipeline_name, query_name, batch_id, result_scope, result_key, result_value, execution_timestamp
        )
        SELECT run_uuid, pipeline, 'q1_daily_traffic', batch_id, result_scope,
               concat(log_date::text, ':', status_code::text),
               jsonb_build_object(
                   'log_date', log_date,
                   'status_code', status_code,
                   'request_count', request_count,
                   'total_bytes', total_bytes
               ),
               executed_at
        FROM daily_traffic
        WHERE run_uuid = %s
    """, (run_uuid,))
    cur.execute("""
        INSERT INTO query_results (
            run_id, pipeline_name, query_name, batch_id, result_scope, result_key, result_value, execution_timestamp
        )
        SELECT run_uuid, pipeline, 'q2_top_resources', batch_id, result_scope,
               resource_path,
               jsonb_build_object(
                   'resource_path', resource_path,
                   'request_count', request_count,
                   'total_bytes', total_bytes,
                   'distinct_host_count', distinct_host_count
               ),
               executed_at
        FROM top_resources
        WHERE run_uuid = %s
    """, (run_uuid,))
    cur.execute("""
        INSERT INTO query_results (
            run_id, pipeline_name, query_name, batch_id, result_scope, result_key, result_value, execution_timestamp
        )
        SELECT run_uuid, pipeline, 'q3_hourly_errors', batch_id, result_scope,
               concat(log_date::text, ':', log_hour::text),
               jsonb_build_object(
                   'log_date', log_date,
                   'log_hour', log_hour,
                   'error_request_count', error_request_count,
                   'total_request_count', total_request_count,
                   'error_rate', error_rate,
                   'distinct_error_hosts', distinct_error_hosts
               ),
               executed_at
        FROM hourly_errors
        WHERE run_uuid = %s
    """, (run_uuid,))

    conn.commit()
    cur.close()
    conn.close()

# ============ STATUS ============
@app.get("/api/status")
def status():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        conn.close()
        return {"db": "ok", "recent_runs": []}
    except Exception as e:
        return {"db": "error", "error": str(e)}

# ============ RUN ============
@app.post("/api/run")
async def run_pipeline(req: Request):
    body = await req.json()
    pipeline = body.get("pipeline")
    batch_mode = body.get("batch_mode", "records")
    batch_size = int(body.get("batch_size", 10000) or 10000)
    batch_interval_seconds = int(body.get("batch_interval_seconds", 3600) or 3600)
    query = body.get("query", "all")
    log_files = body.get("log_files", [])

    if not pipeline or not log_files:
        return {"error": "Missing pipeline or log_files"}
    if batch_mode not in {"records", "time"}:
        return {"error": "batch_mode must be 'records' or 'time'"}
    if batch_size <= 0:
        return {"error": "batch_size must be greater than 0"}
    if batch_interval_seconds <= 0:
        return {"error": "batch_interval_seconds must be greater than 0"}
    if query not in {"all", "q1", "q2", "q3"}:
        return {"error": "query must be one of: all, q1, q2, q3"}

    batch_value = batch_interval_seconds if batch_mode == "time" else batch_size

    # Generate UUID for this run
    run_uuid = str(uuid_lib.uuid4())
    log_file_paths = [
        path if os.path.isabs(path) else os.path.join(PROJECT_ROOT, path)
        for path in log_files
    ]

    # Create run entry in database
    initialize_postgres()
    conn = get_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
            INSERT INTO etl_runs 
            (pipeline, run_uuid, batch_size, batch_mode, batch_interval_seconds, status, started_at)
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
        """, (
            pipeline,
            run_uuid,
            batch_size,
            batch_mode,
            batch_interval_seconds if batch_mode == "time" else None,
            "running"
        ))

        conn.commit()
    except Exception as e:
        conn.rollback()
        cur.close()
        conn.close()
        return {"error": f"DB error: {str(e)}"}
    finally:
        cur.close()
        conn.close()

    # Build command based on pipeline type
    if pipeline == "mongodb":
        cmd = [
            "node",
            os.path.join(PROJECT_ROOT, "pipelines", "mongo","mongodb_pipeline.js"),
            json.dumps(log_file_paths),
            batch_mode,
            str(batch_value),
            run_uuid,
            query
        ]
    elif pipeline == "pig":
        cmd = [
            "bash",
            os.path.join(PROJECT_ROOT, "pig", "run.sh"),
            json.dumps(log_file_paths),
            batch_mode,
            str(batch_value),
            run_uuid,
            query
        ]
    elif pipeline == "mapreduce":
        cmd = [
            "bash",
            os.path.join(PROJECT_ROOT, "pipelines", "mapreduce", "run.sh"),
            json.dumps(log_file_paths),
            batch_mode,
            str(batch_value),
            run_uuid,
            query
        ]
    elif pipeline == "hive":
        cmd = [
            "bash",
            os.path.join(PROJECT_ROOT, "pipelines", "hive", "run.sh"),
            json.dumps(log_file_paths),
            batch_mode,
            str(batch_value),
            run_uuid,
            query
        ]
    else:
        return {"error": f"Unknown pipeline: {pipeline}"}

    def stream():
        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                env=_build_pipeline_env()
            )

            # Stream process output
            for line in process.stdout:
                if line.strip():
                    yield f"data: {json.dumps({'type': 'log', 'message': line.strip()})}\n\n"

            process.wait()

            # Check exit code
            if process.returncode == 0:
                # Mark run as completed
                conn = get_conn()
                cur = conn.cursor()
                cur.execute("""
                    UPDATE etl_runs 
                    SET status = %s, completed_at = NOW()
                    WHERE run_uuid = %s
                """, ("completed", run_uuid))
                conn.commit()
                cur.close()
                conn.close()

                sync_reporting_tables(run_uuid)
                yield f"data: {json.dumps({'type': 'done', 'message': run_uuid})}\n\n"
            else:
                # Mark run as failed
                conn = get_conn()
                cur = conn.cursor()
                cur.execute("""
                    UPDATE etl_runs 
                    SET status = %s, completed_at = NOW()
                    WHERE run_uuid = %s
                """, ("failed", run_uuid))
                conn.commit()
                cur.close()
                conn.close()

                yield f"data: {json.dumps({'type': 'error', 'message': 'Pipeline exited with code ' + str(process.returncode)})}\n\n"

        except Exception as e:
            # Mark run as failed
            try:
                conn = get_conn()
                cur = conn.cursor()
                cur.execute("""
                    UPDATE etl_runs 
                    SET status = %s, completed_at = NOW()
                    WHERE run_uuid = %s
                """, ("failed", run_uuid))
                conn.commit()
                cur.close()
                conn.close()
            except:
                pass

            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"

    return StreamingResponse(stream(), media_type="text/event-stream")

# ============ RESULTS ============
@app.get("/api/run/{run_uuid}")
def get_results(run_uuid: str):
    initialize_postgres()
    sync_reporting_tables(run_uuid)
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        # Get run metadata
        cur.execute("""
            SELECT 
                pipeline, run_uuid, batch_size, batch_mode, batch_interval_seconds,
                total_records, total_batches, avg_batch_size, 
                malformed_count, runtime_seconds, status, 
                started_at, completed_at
            FROM etl_runs 
            WHERE run_uuid = %s
        """, (run_uuid,))
        
        run_row = cur.fetchone()
        if not run_row:
            return {"error": "Run not found"}

        run = dict(run_row)

        cur.execute("""
            SELECT
                run_id, pipeline_name, query_name, batch_size,
                average_batch_size, records_processed, malformed_record_count,
                runtime, execution_timestamp, status, batch_mode,
                batch_interval_seconds, total_batches
            FROM run_metadata
            WHERE run_id = %s
        """, (run_uuid,))
        run_metadata = [dict(row) for row in cur.fetchall()]

        # Get Q1 results
        cur.execute("""
            SELECT log_date, status_code, request_count, total_bytes
            FROM daily_traffic 
            WHERE run_uuid = %s
              AND COALESCE(result_scope, 'aggregate') = 'aggregate'
            ORDER BY log_date DESC
            LIMIT 50
        """, (run_uuid,))
        q1 = [dict(row) for row in cur.fetchall()]

        # Get Q2 results
        cur.execute("""
            SELECT resource_path, request_count, total_bytes, distinct_host_count
            FROM top_resources 
            WHERE run_uuid = %s
              AND COALESCE(result_scope, 'aggregate') = 'aggregate'
            ORDER BY request_count DESC
            LIMIT 20
        """, (run_uuid,))
        q2 = [dict(row) for row in cur.fetchall()]

        # Get Q3 results
        cur.execute("""
            SELECT log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts
            FROM hourly_errors 
            WHERE run_uuid = %s
              AND COALESCE(result_scope, 'aggregate') = 'aggregate'
            ORDER BY log_date DESC, log_hour DESC
            LIMIT 50
        """, (run_uuid,))
        q3 = [dict(row) for row in cur.fetchall()]

        cur.execute("""
            SELECT batch_id, log_date, status_code, request_count, total_bytes
            FROM daily_traffic
            WHERE run_uuid = %s
              AND result_scope = 'per_batch'
            ORDER BY batch_id, log_date DESC, status_code
            LIMIT 150
        """, (run_uuid,))
        q1_batches = [dict(row) for row in cur.fetchall()]

        cur.execute("""
            SELECT batch_id, resource_path, request_count, total_bytes, distinct_host_count
            FROM top_resources
            WHERE run_uuid = %s
              AND result_scope = 'per_batch'
            ORDER BY batch_id, request_count DESC, resource_path
            LIMIT 150
        """, (run_uuid,))
        q2_batches = [dict(row) for row in cur.fetchall()]

        cur.execute("""
            SELECT batch_id, log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts
            FROM hourly_errors
            WHERE run_uuid = %s
              AND result_scope = 'per_batch'
            ORDER BY batch_id, log_date DESC, log_hour DESC
            LIMIT 150
        """, (run_uuid,))
        q3_batches = [dict(row) for row in cur.fetchall()]

        cur.execute("""
            SELECT batch_id, batch_size, records_processed, malformed_count,
                   started_at, completed_at
            FROM batch_metadata
            WHERE run_uuid = %s
            ORDER BY batch_id
            LIMIT 100
        """, (run_uuid,))
        batches = [dict(row) for row in cur.fetchall()]

        cur.execute("""
            SELECT batch_id, malformed_count, recorded_at
            FROM malformed_record_summary
            WHERE run_uuid = %s
            ORDER BY batch_id
            LIMIT 100
        """, (run_uuid,))
        malformed_summary = [dict(row) for row in cur.fetchall()]

        cur.execute("""
            SELECT query_name, batch_id, result_scope, result_key, result_value, execution_timestamp
            FROM query_results
            WHERE run_id = %s
            ORDER BY query_name, id
            LIMIT 150
        """, (run_uuid,))
        query_results = [dict(row) for row in cur.fetchall()]

        cur.execute("""
            SELECT batch_id, raw_line, reason, recorded_at
            FROM malformed_records
            WHERE run_uuid = %s
            ORDER BY batch_id, id
            LIMIT 100
        """, (run_uuid,))
        malformed_records = [dict(row) for row in cur.fetchall()]

        cur.close()
        conn.close()

        return {
            "run": run,
            "q1": q1,
            "q2": q2,
            "q3": q3,
            "q1_batches": q1_batches,
            "q2_batches": q2_batches,
            "q3_batches": q3_batches,
            "run_metadata": run_metadata,
            "batches": batches,
            "malformed_summary": malformed_summary,
            "query_results": query_results,
            "malformed_records": malformed_records
        }

    except Exception as e:
        cur.close()
        conn.close()
        return {"error": str(e)}

# ============ HISTORY ============
@app.get("/api/runs")
def get_runs():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        cur.execute("""
            SELECT 
                pipeline, run_uuid, batch_size, batch_mode, batch_interval_seconds,
                total_records, total_batches, avg_batch_size,
                malformed_count, runtime_seconds, status,
                started_at, completed_at
            FROM etl_runs 
            ORDER BY started_at DESC 
            LIMIT 20
        """)

        rows = [dict(row) for row in cur.fetchall()]
        cur.close()
        conn.close()

        return rows

    except Exception as e:
        cur.close()
        conn.close()
        return {"error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5050)
