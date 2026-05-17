#!/usr/bin/env python3
import csv
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

import psycopg2


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PIG_SCRIPT = os.path.join(SCRIPT_DIR, "etl.pig")
UDF_PATH = os.path.join(SCRIPT_DIR, "udfs", "log_parser.py")

PG_CONFIG = {
    "host": os.environ.get("PGHOST", "127.0.0.1"),
    "port": int(os.environ.get("PGPORT", "5432")),
    "dbname": os.environ.get("PGDATABASE", "nosql_etl_db"),
    "user": os.environ.get("PGUSER", "sathish"),
    "password": os.environ.get("PGPASSWORD", "welcome"),
}


def find_pig_bin():
    explicit = os.environ.get("PIG_BIN")
    if explicit and os.path.isfile(explicit):
        return explicit

    pig_home = os.environ.get("PIG_HOME")
    if pig_home:
        candidate = os.path.join(pig_home, "bin", "pig")
        if os.path.isfile(candidate):
            return candidate

    path_candidate = shutil.which("pig")
    if path_candidate:
        return path_candidate

    home_candidate = os.path.join(os.path.expanduser("~"), "pig", "bin", "pig")
    if os.path.isfile(home_candidate):
        return home_candidate

    return None


def parse_path_list(raw):
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return parsed
    except Exception:
        pass
    return [raw]


def pg_connect():
    return psycopg2.connect(**PG_CONFIG)


def init_postgres(conn):
    with conn.cursor() as cur:
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
        cur.execute("ALTER TABLE etl_runs ADD COLUMN IF NOT EXISTS batch_mode VARCHAR(20) DEFAULT 'records'")
        cur.execute("ALTER TABLE etl_runs ADD COLUMN IF NOT EXISTS batch_interval_seconds INTEGER")
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
        cur.execute("CREATE INDEX IF NOT EXISTS idx_batch_run ON batch_metadata(run_uuid, batch_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_malformed_summary_run ON malformed_record_summary(run_uuid, batch_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_malformed_records_run ON malformed_records(run_uuid, batch_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_daily_pipeline_date ON daily_traffic(pipeline, log_date)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_resource_pipeline ON top_resources(pipeline, request_count DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_error_pipeline_date ON hourly_errors(pipeline, log_date, log_hour)")
    conn.commit()


def start_run(conn, run_uuid, batch_mode, batch_value):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO etl_runs
                (run_uuid, pipeline, batch_size, batch_mode, batch_interval_seconds, status, started_at)
            VALUES (%s, 'pig', %s, %s, %s, 'running', NOW())
            ON CONFLICT (run_uuid) DO UPDATE
            SET pipeline = EXCLUDED.pipeline,
                batch_size = EXCLUDED.batch_size,
                batch_mode = EXCLUDED.batch_mode,
                batch_interval_seconds = EXCLUDED.batch_interval_seconds,
                status = EXCLUDED.status
        """, (
            run_uuid,
            batch_value if batch_mode == "records" else None,
            batch_mode,
            batch_value if batch_mode == "time" else None,
        ))
        for table in (
            "daily_traffic",
            "top_resources",
            "hourly_errors",
            "batch_metadata",
            "malformed_record_summary",
            "malformed_records",
        ):
            cur.execute(f"DELETE FROM {table} WHERE run_uuid = %s", (run_uuid,))
    conn.commit()


def finish_run(conn, run_uuid, total_records, total_batches, avg_batch_size,
               malformed_count, runtime_seconds, batch_mode, batch_value):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE etl_runs
            SET total_records = %s,
                total_batches = %s,
                avg_batch_size = %s,
                malformed_count = %s,
                runtime_seconds = %s,
                status = 'completed',
                completed_at = NOW(),
                batch_mode = %s,
                batch_interval_seconds = %s
            WHERE run_uuid = %s
        """, (
            total_records,
            total_batches,
            avg_batch_size,
            malformed_count,
            runtime_seconds,
            batch_mode,
            batch_value if batch_mode == "time" else None,
            run_uuid,
        ))
    conn.commit()


def fail_run(conn, run_uuid):
    with conn.cursor() as cur:
        cur.execute("UPDATE etl_runs SET status = 'failed', completed_at = NOW() WHERE run_uuid = %s", (run_uuid,))
    conn.commit()


def _build_pig_env():
    env = os.environ.copy()
    if "JAVA_HOME" not in env:
        for java_path in ["/usr/lib/jvm/java-8-openjdk-amd64", "/usr/lib/jvm/java-11-openjdk-amd64"]:
            if os.path.isdir(java_path):
                env["JAVA_HOME"] = java_path
                break
    if "PIG_HOME" not in env:
        home_pig = os.path.join(os.path.expanduser("~"), "pig")
        if os.path.isdir(home_pig):
            env["PIG_HOME"] = home_pig
    if "HADOOP_HOME" not in env:
        home_hadoop = os.path.join(os.path.expanduser("~"), "hadoop")
        if os.path.isdir(home_hadoop):
            env["HADOOP_HOME"] = home_hadoop
    return env


def run_pig(pig_bin, input_path, output_dir, batch_mode, batch_value):
    cmd = [
        pig_bin,
        "-x", "local",
        "-param", f"INPUT={input_path}",
        "-param", f"OUTPUT={output_dir}",
        "-param", f"UDF_PATH={UDF_PATH}",
        "-param", f"BATCH_VALUE={batch_value}",
        "-param", f"BATCH_BY_TIME={1 if batch_mode == 'time' else 0}",
        PIG_SCRIPT,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, env=_build_pig_env())


def read_part_rows(directory):
    if not os.path.isdir(directory):
        return
    for name in sorted(os.listdir(directory)):
        if name.startswith("part"):
            path = os.path.join(directory, name)
            with open(path, "r", encoding="utf-8") as fh:
                reader = csv.reader(fh, delimiter="\t")
                for row in reader:
                    if row:
                        yield row


def load_pig_metadata(conn, run_uuid, output_dir):
    total_records = 0
    total_batches = 0
    malformed_count = 0
    with conn.cursor() as cur:
        for row in read_part_rows(os.path.join(output_dir, "batch_metadata")):
            if len(row) < 4:
                continue
            batch_id = int(row[0])
            batch_size = int(row[1])
            records_processed = int(row[2])
            batch_malformed = int(row[3])
            total_batches += 1
            total_records += records_processed
            malformed_count += batch_malformed
            cur.execute("""
                INSERT INTO batch_metadata
                    (run_uuid, pipeline, batch_id, batch_size, records_processed, malformed_count)
                VALUES (%s, 'pig', %s, %s, %s, %s)
            """, (run_uuid, batch_id, batch_size, records_processed, batch_malformed))
            if batch_malformed:
                cur.execute("""
                    INSERT INTO malformed_record_summary
                        (run_uuid, pipeline, batch_id, malformed_count)
                    VALUES (%s, 'pig', %s, %s)
                """, (run_uuid, batch_id, batch_malformed))

        for row in read_part_rows(os.path.join(output_dir, "malformed_records")):
            if len(row) < 3:
                continue
            cur.execute("""
                INSERT INTO malformed_records
                    (run_uuid, pipeline, batch_id, raw_line, reason)
                VALUES (%s, 'pig', %s, %s, %s)
            """, (run_uuid, int(row[0]), row[1], row[2]))
    conn.commit()
    return total_records, total_batches, malformed_count


def load_results(conn, run_uuid, batch_id, output_dir, query="all"):
    with conn.cursor() as cur:
        if query in {"all", "q1"}:
            for row in read_part_rows(os.path.join(output_dir, "q1")):
                if len(row) >= 4:
                    cur.execute("""
                        INSERT INTO daily_traffic
                            (pipeline, run_uuid, batch_id, log_date, status_code, request_count, total_bytes)
                        VALUES ('pig', %s, %s, %s, %s, %s, %s)
                    """, (run_uuid, batch_id, row[0], int(row[1]), int(row[2]), int(row[3])))

        if query in {"all", "q2"}:
            for row in read_part_rows(os.path.join(output_dir, "q2")):
                if len(row) >= 4:
                    cur.execute("""
                        INSERT INTO top_resources
                            (pipeline, run_uuid, batch_id, resource_path, request_count, total_bytes, distinct_host_count)
                        VALUES ('pig', %s, %s, %s, %s, %s, %s)
                    """, (run_uuid, batch_id, row[0], int(row[1]), int(row[2]), int(row[3])))

        if query in {"all", "q3"}:
            for row in read_part_rows(os.path.join(output_dir, "q3")):
                if len(row) >= 6:
                    cur.execute("""
                        INSERT INTO hourly_errors
                            (pipeline, run_uuid, batch_id, log_date, log_hour,
                             error_request_count, total_request_count, error_rate, distinct_error_hosts)
                        VALUES ('pig', %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        run_uuid,
                        batch_id,
                        row[0],
                        int(row[1]),
                        int(row[2]),
                        int(row[3]),
                        float(row[4]),
                        int(row[5]) if row[5] else 0,
                    ))
    conn.commit()


def run_pipeline(log_file_paths, batch_mode, batch_value, run_uuid, query="all"):
    if batch_mode not in {"records", "time"}:
        raise ValueError("batch_mode must be records or time")
    if query not in {"all", "q1", "q2", "q3"}:
        raise ValueError("query must be one of all, q1, q2, q3")
    if batch_value <= 0:
        raise ValueError("batch_value must be greater than 0")

    pig_bin = find_pig_bin()
    if not pig_bin:
        raise FileNotFoundError("Pig executable not found. Install Pig or set PIG_HOME/PIG_BIN.")

    for log_path in log_file_paths:
        if not os.path.isfile(log_path):
            raise FileNotFoundError(f"Log file not found: {log_path}")

    start_time = time.time()
    output_dir = tempfile.mkdtemp(prefix="pig_output_")
    conn = pg_connect()

    try:
        init_postgres(conn)
        start_run(conn, run_uuid, batch_mode, batch_value)

        print("Pig ETL started")
        print(f"Run UUID: {run_uuid}")
        print(f"Batch mode: {batch_mode}")
        print(f"Batch value: {batch_value}")
        print(f"Input files: {log_file_paths}")

        result = run_pig(pig_bin, ",".join(log_file_paths), output_dir, batch_mode, batch_value)
        if result.stdout.strip():
            print(result.stdout.strip())
        if result.returncode != 0:
            if result.stderr.strip():
                print(result.stderr.strip())
            raise RuntimeError(f"Pig failed with exit code {result.returncode}")

        total_records, total_batches, malformed_count = load_pig_metadata(conn, run_uuid, output_dir)
        avg_batch_size = total_records / total_batches if total_batches else 0
        load_results(conn, run_uuid, total_batches, output_dir, query)
        runtime_seconds = time.time() - start_time
        finish_run(
            conn,
            run_uuid,
            total_records,
            total_batches,
            avg_batch_size,
            malformed_count,
            runtime_seconds,
            batch_mode,
            batch_value,
        )

        print("Pig ETL completed")
        print(f"Total Records: {total_records}")
        print(f"Malformed Records: {malformed_count}")
        print(f"Total Batches: {total_batches}")
        print(f"Avg Batch Size: {avg_batch_size:.2f}")
        print(f"Runtime: {runtime_seconds:.2f} sec")
    except Exception:
        try:
            fail_run(conn, run_uuid)
        except Exception:
            pass
        raise
    finally:
        conn.close()
        shutil.rmtree(output_dir, ignore_errors=True)


def main():
    args = sys.argv[1:]
    query = "all"
    if len(args) == 2:
        log_file_paths = [args[0]]
        batch_mode = "records"
        batch_value = int(args[1])
        run_uuid = f"pig-cli-{int(time.time())}"
    elif len(args) >= 4:
        log_file_paths = parse_path_list(args[0])
        batch_mode = args[1]
        batch_value = int(args[2])
        run_uuid = args[3]
        query = args[4] if len(args) >= 5 else "all"
    else:
        print("Usage: python3 orchestrator.py <log_file_path_or_json_array> <batch_mode> <batch_value> <run_uuid>")
        print("Legacy: python3 orchestrator.py <log_file_path> <batch_size>")
        sys.exit(1)

    run_pipeline(log_file_paths, batch_mode, batch_value, run_uuid, query)


if __name__ == "__main__":
    main()
