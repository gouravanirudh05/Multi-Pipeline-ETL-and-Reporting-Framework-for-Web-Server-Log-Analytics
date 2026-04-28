from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import subprocess
import json
import psycopg2
import psycopg2.extras
import os
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
    "dbname": "nosql_etl_db",
    "user": "sathish",
    "password": "welcome",
    "host": "localhost",
    "port": 5432
}

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

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
    batch_size = body.get("batch_size", 10000)
    log_files = body.get("log_files", [])

    if not pipeline or not log_files:
        return {"error": "Missing pipeline or log_files"}

    # Generate UUID for this run
    run_uuid = str(uuid_lib.uuid4())
    log_file = os.path.join(PROJECT_ROOT, log_files[0])

    # Create run entry in database
    conn = get_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
            INSERT INTO etl_runs 
            (pipeline, run_uuid, batch_size, status, started_at)
            VALUES (%s, %s, %s, %s, NOW())
            RETURNING run_id
        """, (pipeline, run_uuid, batch_size, "running"))

        run_id = cur.fetchone()[0]
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
            log_file,
            str(batch_size),
            str(run_id),
            run_uuid
        ]
    elif pipeline == "pig":
        cmd = ["echo", "Pig pipeline not yet implemented"]
    elif pipeline == "mapreduce":
        cmd = ["echo", "MapReduce pipeline not yet implemented"]
    elif pipeline == "hive":
        cmd = ["echo", "Hive pipeline not yet implemented"]
    else:
        return {"error": f"Unknown pipeline: {pipeline}"}

    def stream():
        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )

            # Stream stdout
            for line in process.stdout:
                if line.strip():
                    yield f"data: {json.dumps({'type': 'log', 'message': line.strip()})}\n\n"

            # Stream stderr
            for line in process.stderr:
                if line.strip():
                    yield f"data: {json.dumps({'type': 'log', 'message': '⚠️ ' + line.strip()})}\n\n"

            process.wait()

            # Check exit code
            if process.returncode == 0:
                # Mark run as completed
                conn = get_conn()
                cur = conn.cursor()
                cur.execute("""
                    UPDATE etl_runs 
                    SET status = %s, completed_at = NOW()
                    WHERE run_id = %s
                """, ("completed", run_id))
                conn.commit()
                cur.close()
                conn.close()

                yield f"data: {json.dumps({'type': 'done', 'message': run_uuid})}\n\n"
            else:
                # Mark run as failed
                conn = get_conn()
                cur = conn.cursor()
                cur.execute("""
                    UPDATE etl_runs 
                    SET status = %s, completed_at = NOW()
                    WHERE run_id = %s
                """, ("failed", run_id))
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
                    WHERE run_id = %s
                """, ("failed", run_id))
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
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        # Get run metadata
        cur.execute("""
            SELECT 
                run_id, pipeline, run_uuid, batch_size, 
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

        # Get Q1 results
        cur.execute("""
            SELECT log_date, status_code, request_count, total_bytes
            FROM daily_traffic 
            WHERE run_uuid = %s
            ORDER BY log_date DESC
            LIMIT 50
        """, (run_uuid,))
        q1 = [dict(row) for row in cur.fetchall()]

        # Get Q2 results
        cur.execute("""
            SELECT resource_path, request_count, total_bytes, distinct_host_count
            FROM top_resources 
            WHERE run_uuid = %s
            ORDER BY request_count DESC
            LIMIT 20
        """, (run_uuid,))
        q2 = [dict(row) for row in cur.fetchall()]

        # Get Q3 results
        cur.execute("""
            SELECT log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts
            FROM hourly_errors 
            WHERE run_uuid = %s
            ORDER BY log_date DESC, log_hour DESC
            LIMIT 50
        """, (run_uuid,))
        q3 = [dict(row) for row in cur.fetchall()]

        cur.close()
        conn.close()

        return {
            "run": run,
            "q1": q1,
            "q2": q2,
            "q3": q3
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
                run_id, pipeline, run_uuid, batch_size,
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