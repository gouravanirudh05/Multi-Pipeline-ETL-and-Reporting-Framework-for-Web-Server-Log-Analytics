#!/usr/bin/env python3
# orchestrator.py
#
# Pig Pipeline Orchestrator for NASA HTTP Log ETL
# -----------------------------------------------
# Usage:
#   python3 orchestrator.py <log_file_path> <batch_size>
#
# What this script does:
#   1. Splits the raw log file into chunk files of <batch_size> lines each
#   2. For each chunk, invokes Pig in local mode with etl.pig
#   3. Reads the TSV output files Pig writes for each query
#   4. Inserts aggregated results into PostgreSQL (same schema as MongoDB pipeline)
#   5. Updates the pipeline_runs row with final metadata
#
# Prerequisites (see setup.sh):
#   - Java 11 installed, JAVA_HOME set
#   - Apache Pig 0.17 unpacked, PIG_HOME set
#   - psycopg2 installed  (pip3 install psycopg2-binary)
#   - PostgreSQL running with the nosql_etl_db database and tables created

import os
import sys
import subprocess
import time
import csv
import shutil
import tempfile

import psycopg2

# ------------------------------------------------------------------ #
# Configuration — edit these if your PostgreSQL credentials differ
# ------------------------------------------------------------------ #
PG_CONFIG = {
    "host":     "127.0.0.1",
    "port":     5432,
    "dbname":   "nosql_etl_db",
    "user":     "sathish",
    "password": "welcome",
}

# Paths derived relative to this script's location
SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
PIG_SCRIPT  = os.path.join(SCRIPT_DIR, "etl.pig")
UDF_PATH    = os.path.join(SCRIPT_DIR, "udfs", "log_parser.py")

# PIG_HOME must be set in environment (done by setup.sh / .bashrc)
# Fallback: look for it next to this script
PIG_HOME    = os.environ.get("PIG_HOME",
                              os.path.join(SCRIPT_DIR, "pig"))
PIG_BIN     = os.path.join(PIG_HOME, "bin", "pig")


# ------------------------------------------------------------------ #
# PostgreSQL helpers
# ------------------------------------------------------------------ #

def pg_connect():
    return psycopg2.connect(**PG_CONFIG)


def init_postgres(conn):
    """
    Ensure all four result tables exist.
    Mirrors the schema used by the MongoDB pipeline so reports can
    query both pipelines from the same tables.
    """
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id              SERIAL PRIMARY KEY,
                pipeline_name       VARCHAR(50),
                batch_size          INT,
                total_batches       INT,
                avg_batch_size      FLOAT,
                malformed_records   INT,
                runtime_seconds     FLOAT,
                execution_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS daily_traffic_summary (
                run_id          INT,
                batch_id        INT,
                log_date        DATE,
                status_code     INT,
                request_count   BIGINT,
                total_bytes     BIGINT
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS top_resources (
                run_id              INT,
                batch_id            INT,
                resource_path       TEXT,
                request_count       BIGINT,
                total_bytes         BIGINT,
                distinct_host_count BIGINT
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS hourly_error_analysis (
                run_id                INT,
                batch_id              INT,
                log_date              DATE,
                log_hour              INT,
                error_request_count   BIGINT,
                total_request_count   BIGINT,
                error_rate            FLOAT,
                distinct_error_hosts  BIGINT
            );
        """)
    conn.commit()


def create_run(conn, batch_size):
    """Insert a placeholder pipeline_runs row and return its run_id."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO pipeline_runs
                (pipeline_name, batch_size, total_batches,
                 avg_batch_size, malformed_records, runtime_seconds)
            VALUES (%s, %s, 0, 0, 0, 0)
            RETURNING run_id
        """, ("Pig", batch_size))
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id


def update_run(conn, run_id, total_batches, avg_batch_size,
               malformed_records, runtime_seconds):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE pipeline_runs
            SET total_batches    = %s,
                avg_batch_size   = %s,
                malformed_records= %s,
                runtime_seconds  = %s
            WHERE run_id = %s
        """, (total_batches, avg_batch_size,
              malformed_records, runtime_seconds, run_id))
    conn.commit()


# ------------------------------------------------------------------ #
# Chunk file helpers
# ------------------------------------------------------------------ #

def split_log_file(log_path, batch_size, tmp_dir):
    """
    Read log_path line by line and write sequential chunk files:
        tmp_dir/chunk_0001.log, chunk_0002.log, ...

    Returns a list of (chunk_file_path, line_count) tuples.
    """
    chunks = []
    chunk_index = 1
    lines_in_chunk = 0
    current_file = None
    current_path = None

    with open(log_path, "r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            if lines_in_chunk == 0:
                current_path = os.path.join(
                    tmp_dir, f"chunk_{chunk_index:04d}.log")
                current_file = open(current_path, "w", encoding="utf-8")

            current_file.write(line)
            lines_in_chunk += 1

            if lines_in_chunk >= batch_size:
                current_file.close()
                chunks.append((current_path, lines_in_chunk))
                chunk_index += 1
                lines_in_chunk = 0
                current_file = None

    # Final partial chunk
    if current_file is not None:
        current_file.close()
        chunks.append((current_path, lines_in_chunk))

    return chunks


# ------------------------------------------------------------------ #
# Pig invocation
# ------------------------------------------------------------------ #

def run_pig(chunk_path, output_dir):
    """
    Invoke Pig in local mode for one chunk file.

    Returns (returncode, stdout, stderr).

    Pig is called as a subprocess so we capture its output.
    The -x local flag tells Pig to run without HDFS/MapReduce;
    all I/O is on the local filesystem.
    """
    cmd = [
        PIG_BIN,
        "-x", "local",                     # local mode — no Hadoop needed
        "-param", f"INPUT={chunk_path}",
        "-param", f"OUTPUT={output_dir}",
        "-param", f"UDF_JAR={UDF_PATH}",
        PIG_SCRIPT,
    ]
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
    )
    return result.returncode, result.stdout, result.stderr


# ------------------------------------------------------------------ #
# TSV result readers
# ------------------------------------------------------------------ #

def read_tsv(directory, filename_prefix="part-"):
    """
    Pig writes output as one or more part-r-NNNNN files inside a directory.
    Collect all of them and yield rows as lists of strings.
    """
    if not os.path.isdir(directory):
        return

    for fname in sorted(os.listdir(directory)):
        if fname.startswith(filename_prefix) or fname.startswith("part"):
            fpath = os.path.join(directory, fname)
            with open(fpath, "r", encoding="utf-8") as fh:
                reader = csv.reader(fh, delimiter='\t')
                for row in reader:
                    if row:  # skip empty lines
                        yield row


def count_malformed(output_dir):
    """
    Count lines written to the malformed output directory.
    Each line represents one record the UDF could not parse.
    """
    mal_dir = os.path.join(output_dir, "malformed")
    count = 0
    for row in read_tsv(mal_dir):
        count += 1
    return count


# ------------------------------------------------------------------ #
# PostgreSQL loaders for each query result
# ------------------------------------------------------------------ #

def load_q1(conn, run_id, batch_id, output_dir):
    """
    Q1 TSV columns: log_date, status_code, request_count, total_bytes
    """
    q1_dir = os.path.join(output_dir, "q1")
    rows_loaded = 0
    with conn.cursor() as cur:
        for row in read_tsv(q1_dir):
            if len(row) < 4:
                continue
            try:
                cur.execute("""
                    INSERT INTO daily_traffic_summary
                        (run_id, batch_id, log_date, status_code,
                         request_count, total_bytes)
                    VALUES (%s,%s,%s,%s,%s,%s)
                """, (run_id, batch_id,
                      row[0],       # log_date  (string, PG casts to DATE)
                      int(row[1]),  # status_code
                      int(row[2]),  # request_count
                      int(row[3]))) # total_bytes
                rows_loaded += 1
            except (ValueError, IndexError):
                pass  # skip malformed aggregate rows
    conn.commit()
    return rows_loaded


def load_q2(conn, run_id, batch_id, output_dir):
    """
    Q2 TSV columns: resource_path, request_count, total_bytes,
                    distinct_host_count
    """
    q2_dir = os.path.join(output_dir, "q2")
    rows_loaded = 0
    with conn.cursor() as cur:
        for row in read_tsv(q2_dir):
            if len(row) < 4:
                continue
            try:
                cur.execute("""
                    INSERT INTO top_resources
                        (run_id, batch_id, resource_path, request_count,
                         total_bytes, distinct_host_count)
                    VALUES (%s,%s,%s,%s,%s,%s)
                """, (run_id, batch_id,
                      row[0],        # resource_path
                      int(row[1]),   # request_count
                      int(row[2]),   # total_bytes
                      int(row[3])))  # distinct_host_count
                rows_loaded += 1
            except (ValueError, IndexError):
                pass
    conn.commit()
    return rows_loaded


def load_q3(conn, run_id, batch_id, output_dir):
    """
    Q3 TSV columns: log_date, log_hour, error_request_count,
                    total_request_count, error_rate, distinct_error_hosts
    """
    q3_dir = os.path.join(output_dir, "q3")
    rows_loaded = 0
    with conn.cursor() as cur:
        for row in read_tsv(q3_dir):
            if len(row) < 6:
                continue
            try:
                cur.execute("""
                    INSERT INTO hourly_error_analysis
                        (run_id, batch_id, log_date, log_hour,
                         error_request_count, total_request_count,
                         error_rate, distinct_error_hosts)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """, (run_id, batch_id,
                      row[0],          # log_date
                      int(row[1]),     # log_hour
                      int(row[2]),     # error_request_count
                      int(row[3]),     # total_request_count
                      float(row[4]),   # error_rate
                      int(row[5])))    # distinct_error_hosts
                rows_loaded += 1
            except (ValueError, IndexError):
                pass
    conn.commit()
    return rows_loaded


# ------------------------------------------------------------------ #
# Main pipeline
# ------------------------------------------------------------------ #

def run_pipeline(log_file_path, batch_size):

    if not os.path.isfile(log_file_path):
        print(f"ERROR: log file not found: {log_file_path}")
        sys.exit(1)

    if not os.path.isfile(PIG_BIN):
        print(f"ERROR: pig binary not found at {PIG_BIN}")
        print("       Make sure setup.sh has been run and PIG_HOME is exported.")
        sys.exit(1)

    # ---- Timer starts NOW (after validation, as per project spec) ---- #
    start_time = time.time()

    # Connect to PostgreSQL
    conn = pg_connect()
    init_postgres(conn)
    run_id = create_run(conn, batch_size)
    print(f"Created pipeline run with run_id={run_id}")

    # Working directories
    tmp_dir = tempfile.mkdtemp(prefix="pig_chunks_")
    out_base = tempfile.mkdtemp(prefix="pig_output_")
    print(f"Chunk directory : {tmp_dir}")
    print(f"Output directory: {out_base}")

    # Split log file into chunks
    print(f"\nSplitting log file into batches of {batch_size} lines...")
    chunks = split_log_file(log_file_path, batch_size, tmp_dir)
    print(f"Total chunks created: {len(chunks)}")

    total_malformed  = 0
    total_valid      = 0
    total_batches    = 0

    for batch_id, (chunk_path, chunk_line_count) in enumerate(chunks, start=1):
        print(f"\n--- Batch {batch_id} / {len(chunks)} "
              f"({chunk_line_count} raw lines) ---")

        output_dir = os.path.join(out_base, f"batch_{batch_id:04d}")
        os.makedirs(output_dir, exist_ok=True)

        # Run Pig on this chunk
        returncode, stdout, stderr = run_pig(chunk_path, output_dir)

        if returncode != 0:
            # Print Pig's stderr so the user can debug
            print(f"  [WARN] Pig exited with code {returncode} on batch {batch_id}")
            print("  --- Pig stderr (last 30 lines) ---")
            for ln in stderr.strip().splitlines()[-30:]:
                print(f"  {ln}")
            # Continue to next batch rather than aborting the whole run
            continue

        # Count malformed records from Pig's malformed output dir
        mal_count = count_malformed(output_dir)
        valid_count = chunk_line_count - mal_count
        total_malformed += mal_count
        total_valid     += valid_count
        total_batches   += 1

        # Load query results into PostgreSQL
        q1_rows = load_q1(conn, run_id, batch_id, output_dir)
        q2_rows = load_q2(conn, run_id, batch_id, output_dir)
        q3_rows = load_q3(conn, run_id, batch_id, output_dir)

        print(f"  Malformed : {mal_count}")
        print(f"  Valid     : {valid_count}")
        print(f"  Q1 rows loaded: {q1_rows}")
        print(f"  Q2 rows loaded: {q2_rows}")
        print(f"  Q3 rows loaded: {q3_rows}")

    # ---- Timer ends when results are in PostgreSQL ---- #
    runtime_seconds = time.time() - start_time
    avg_batch_size  = (total_valid / total_batches) if total_batches > 0 else 0

    update_run(conn, run_id,
               total_batches, avg_batch_size,
               total_malformed, runtime_seconds)

    conn.close()

    # Clean up temp files
    shutil.rmtree(tmp_dir, ignore_errors=True)
    shutil.rmtree(out_base, ignore_errors=True)

    print("\n===== PIPELINE COMPLETE =====")
    print(f"Run ID           : {run_id}")
    print(f"Total Batches    : {total_batches}")
    print(f"Valid Records    : {total_valid}")
    print(f"Malformed Records: {total_malformed}")
    print(f"Avg Batch Size   : {avg_batch_size:.2f}")
    print(f"Runtime          : {runtime_seconds:.2f} sec")


# ------------------------------------------------------------------ #
# Entry point
# ------------------------------------------------------------------ #
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 orchestrator.py <log_file_path> <batch_size>")
        sys.exit(1)

    log_file_path = sys.argv[1]
    batch_size    = int(sys.argv[2])

    run_pipeline(log_file_path, batch_size)