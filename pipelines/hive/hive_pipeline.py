#!/usr/bin/env python3
import csv
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time

import psycopg2


PG_CONFIG = {
    "host": os.environ.get("PGHOST", "127.0.0.1"),
    "port": int(os.environ.get("PGPORT", "5432")),
    "dbname": os.environ.get("PGDATABASE", "nosql_etl_db"),
    "user": os.environ.get("PGUSER", "sathish"),
    "password": os.environ.get("PGPASSWORD", "welcome"),
}

def find_hive_bin():
    explicit = os.environ.get("HIVE_BIN")
    if explicit and os.path.isfile(explicit):
        return explicit

    hive_home = os.environ.get("HIVE_HOME")
    if hive_home:
        candidate = os.path.join(hive_home, "bin", "hive")
        if os.path.isfile(candidate):
            return candidate

    path_candidate = shutil.which("hive")
    if path_candidate:
        return path_candidate

    # Standard install location (matches ~/hadoop convention)
    home_candidate = os.path.join(os.path.expanduser("~"), "hive", "bin", "hive")
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


def hql_string(value):
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def safe_database_name(run_uuid):
    cleaned = re.sub(r"[^A-Za-z0-9_]", "_", run_uuid)
    return f"nasa_etl_{cleaned}"[:120]


def build_hql(run_uuid, log_file_paths, batch_mode, batch_value, output_dir):
    database = safe_database_name(run_uuid)
    q1_dir = os.path.join(output_dir, "q1")
    q2_dir = os.path.join(output_dir, "q2")
    q3_dir = os.path.join(output_dir, "q3")
    batch_dir = os.path.join(output_dir, "batch_metadata")
    malformed_summary_dir = os.path.join(output_dir, "malformed_summary")
    malformed_records_dir = os.path.join(output_dir, "malformed_records")

    log_regex = r'^(\S+) \S+ \S+ \[(.*?)\] "(\S+) (.*?) (\S+)" (\d{3}) (\S+)'
    ts_regex = r'^(\d{2})/([A-Za-z]{3})/(\d{4}):(\d{2}):(\d{2}):(\d{2})(?:\s+([+-])(\d{2})(\d{2}))?$'
    load_statements = "\n".join(
        f"LOAD DATA LOCAL INPATH {hql_string(path)} INTO TABLE raw_logs;"
        for path in log_file_paths
    )
    valid_condition = """
  host <> ''
  AND method <> ''
  AND resource_path <> ''
  AND protocol <> ''
  AND month_num IS NOT NULL
  AND day_text <> ''
  AND year_text <> ''
  AND hour_text <> ''
  AND minute_text <> ''
  AND second_text <> ''
  AND status_text RLIKE '^\\\\d{3}$'
  AND (bytes_text = '-' OR bytes_text RLIKE '^\\\\d+$')
"""
    epoch_expr = "unix_timestamp(concat(year_text, '-', month_num, '-', day_text, ' ', hour_text, ':', minute_text, ':', second_text), 'yyyy-MM-dd HH:mm:ss')"
    if batch_mode == "time":
        batch_expr = f"""
  CASE
    WHEN is_valid = 1 THEN (floor(cast(({epoch_expr} - min_epoch) AS double) / cast({batch_value} AS double)) + 1)
    ELSE 1
  END
"""
    else:
        batch_expr = f"(floor(cast((record_number - 1) AS double) / cast({batch_value} AS double)) + 1)"

    return f"""
SET hive.exec.mode.local.auto=true;
SET hive.cli.print.header=false;
SET mapreduce.framework.name=local;

DROP DATABASE IF EXISTS {database} CASCADE;
CREATE DATABASE {database};
USE {database};

CREATE TABLE raw_logs(line STRING);
{load_statements}

CREATE TABLE raw_indexed AS
SELECT
  row_number() OVER (ORDER BY line) AS record_number,
  line
FROM raw_logs;

CREATE TABLE extracted AS
SELECT
  record_number,
  line,
  regexp_extract(line, {hql_string(log_regex)}, 1) AS host,
  regexp_extract(line, {hql_string(log_regex)}, 2) AS timestamp_text,
  regexp_extract(line, {hql_string(log_regex)}, 3) AS method,
  regexp_extract(line, {hql_string(log_regex)}, 4) AS resource_path,
  regexp_extract(line, {hql_string(log_regex)}, 5) AS protocol,
  regexp_extract(line, {hql_string(log_regex)}, 6) AS status_text,
  regexp_extract(line, {hql_string(log_regex)}, 7) AS bytes_text
FROM raw_indexed;

CREATE TABLE parsed AS
SELECT
  record_number,
  line,
  host,
  timestamp_text,
  method,
  resource_path,
  protocol,
  status_text,
  bytes_text,
  regexp_extract(timestamp_text, {hql_string(ts_regex)}, 1) AS day_text,
  regexp_extract(timestamp_text, {hql_string(ts_regex)}, 2) AS month_text,
  regexp_extract(timestamp_text, {hql_string(ts_regex)}, 3) AS year_text,
  regexp_extract(timestamp_text, {hql_string(ts_regex)}, 4) AS hour_text,
  regexp_extract(timestamp_text, {hql_string(ts_regex)}, 5) AS minute_text,
  regexp_extract(timestamp_text, {hql_string(ts_regex)}, 6) AS second_text
FROM extracted;

CREATE TABLE normalized AS
SELECT
  *,
  CASE month_text
    WHEN 'Jan' THEN '01'
    WHEN 'Feb' THEN '02'
    WHEN 'Mar' THEN '03'
    WHEN 'Apr' THEN '04'
    WHEN 'May' THEN '05'
    WHEN 'Jun' THEN '06'
    WHEN 'Jul' THEN '07'
    WHEN 'Aug' THEN '08'
    WHEN 'Sep' THEN '09'
    WHEN 'Oct' THEN '10'
    WHEN 'Nov' THEN '11'
    WHEN 'Dec' THEN '12'
    ELSE NULL
  END AS month_num
FROM parsed;

CREATE TABLE normalized_validated AS
SELECT
  *,
  CASE WHEN {valid_condition} THEN 1 ELSE 0 END AS is_valid
FROM normalized;

CREATE TABLE valid_epoch AS
SELECT
  min({epoch_expr}) AS min_epoch
FROM normalized_validated
WHERE is_valid = 1;

CREATE TABLE annotated_logs AS
SELECT
  n.*,
  {batch_expr} AS batch_id
FROM normalized_validated n
CROSS JOIN valid_epoch;

CREATE TABLE valid_logs AS
SELECT
  batch_id,
  host,
  concat(year_text, '-', month_num, '-', day_text) AS log_date,
  cast(hour_text AS int) AS log_hour,
  method,
  resource_path,
  protocol,
  cast(status_text AS int) AS status_code,
  CASE WHEN bytes_text = '-' THEN 0 ELSE cast(bytes_text AS bigint) END AS bytes_transferred,
  {epoch_expr} AS epoch_seconds
FROM annotated_logs
WHERE is_valid = 1;

INSERT OVERWRITE LOCAL DIRECTORY {hql_string(batch_dir)}
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
SELECT
  batch_id,
  {batch_value} AS batch_size,
  count(1) AS records_processed,
  sum(CASE WHEN is_valid = 1 THEN 0 ELSE 1 END) AS malformed_count
FROM annotated_logs
GROUP BY batch_id;

INSERT OVERWRITE LOCAL DIRECTORY {hql_string(malformed_summary_dir)}
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
SELECT batch_id, count(1)
FROM annotated_logs
WHERE is_valid = 0
GROUP BY batch_id;

INSERT OVERWRITE LOCAL DIRECTORY {hql_string(malformed_records_dir)}
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
SELECT batch_id, line, 'parse_failed'
FROM annotated_logs
WHERE is_valid = 0;

INSERT OVERWRITE LOCAL DIRECTORY {hql_string(q1_dir)}
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
SELECT log_date, status_code, count(1), sum(bytes_transferred)
FROM valid_logs
GROUP BY log_date, status_code;

INSERT OVERWRITE LOCAL DIRECTORY {hql_string(q2_dir)}
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
SELECT resource_path, count(1) AS request_count, sum(bytes_transferred) AS total_bytes, count(DISTINCT host) AS distinct_host_count
FROM valid_logs
GROUP BY resource_path
ORDER BY request_count DESC, resource_path ASC
LIMIT 20;

INSERT OVERWRITE LOCAL DIRECTORY {hql_string(q3_dir)}
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
SELECT
  log_date,
  log_hour,
  sum(CASE WHEN status_code >= 400 AND status_code <= 599 THEN 1 ELSE 0 END) AS error_request_count,
  count(1) AS total_request_count,
  cast(sum(CASE WHEN status_code >= 400 AND status_code <= 599 THEN 1 ELSE 0 END) AS double) / cast(count(1) AS double) AS error_rate,
  count(DISTINCT CASE WHEN status_code >= 400 AND status_code <= 599 THEN host ELSE NULL END) AS distinct_error_hosts
FROM valid_logs
GROUP BY log_date, log_hour;

DROP DATABASE IF EXISTS {database} CASCADE;
"""


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
            VALUES (%s, 'hive', %s, %s, %s, 'running', NOW())
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
        cur.execute("DELETE FROM daily_traffic WHERE run_uuid = %s", (run_uuid,))
        cur.execute("DELETE FROM top_resources WHERE run_uuid = %s", (run_uuid,))
        cur.execute("DELETE FROM hourly_errors WHERE run_uuid = %s", (run_uuid,))
        cur.execute("DELETE FROM batch_metadata WHERE run_uuid = %s", (run_uuid,))
        cur.execute("DELETE FROM malformed_record_summary WHERE run_uuid = %s", (run_uuid,))
        cur.execute("DELETE FROM malformed_records WHERE run_uuid = %s", (run_uuid,))
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


def read_output_rows(directory):
    if not os.path.isdir(directory):
        return
    for name in sorted(os.listdir(directory)):
        if name.startswith(".") or name.startswith("_"):
            continue
        path = os.path.join(directory, name)
        if not os.path.isfile(path):
            continue
        with open(path, "r", encoding="utf-8") as fh:
            reader = csv.reader(fh, delimiter="\t")
            for row in reader:
                if row:
                    yield row


def load_hive_metadata(conn, run_uuid, output_dir):
    total_records = 0
    total_batches = 0
    malformed_count = 0
    with conn.cursor() as cur:
        for row in read_output_rows(os.path.join(output_dir, "batch_metadata")):
            if len(row) < 4:
                continue
            batch_id = int(float(row[0]))
            batch_size = int(row[1])
            records_processed = int(row[2])
            batch_malformed = int(row[3])
            total_batches += 1
            total_records += records_processed
            malformed_count += batch_malformed
            cur.execute("""
                INSERT INTO batch_metadata
                    (run_uuid, pipeline, batch_id, batch_size, records_processed, malformed_count)
                VALUES (%s, 'hive', %s, %s, %s, %s)
            """, (run_uuid, batch_id, batch_size, records_processed, batch_malformed))
            if batch_malformed:
                cur.execute("""
                    INSERT INTO malformed_record_summary
                        (run_uuid, pipeline, batch_id, malformed_count)
                    VALUES (%s, 'hive', %s, %s)
                """, (run_uuid, batch_id, batch_malformed))
        for row in read_output_rows(os.path.join(output_dir, "malformed_records")):
            if len(row) < 3:
                continue
            cur.execute("""
                INSERT INTO malformed_records
                    (run_uuid, pipeline, batch_id, raw_line, reason)
                VALUES (%s, 'hive', %s, %s, %s)
            """, (run_uuid, int(float(row[0])), row[1], row[2]))
    conn.commit()
    return total_records, total_batches, malformed_count


def load_results(conn, run_uuid, batch_id, output_dir, query="all"):
    with conn.cursor() as cur:
        if query in {"all", "q1"}:
            for row in read_output_rows(os.path.join(output_dir, "q1")):
                if len(row) < 4:
                    continue
                cur.execute("""
                    INSERT INTO daily_traffic
                        (pipeline, run_uuid, batch_id, log_date, status_code, request_count, total_bytes)
                    VALUES ('hive', %s, %s, %s, %s, %s, %s)
                """, (run_uuid, batch_id, row[0], int(row[1]), int(row[2]), int(row[3])))

        if query in {"all", "q2"}:
            for row in read_output_rows(os.path.join(output_dir, "q2")):
                if len(row) < 4:
                    continue
                cur.execute("""
                    INSERT INTO top_resources
                        (pipeline, run_uuid, batch_id, resource_path, request_count, total_bytes, distinct_host_count)
                    VALUES ('hive', %s, %s, %s, %s, %s, %s)
                """, (run_uuid, batch_id, row[0], int(row[1]), int(row[2]), int(row[3])))

        if query in {"all", "q3"}:
            for row in read_output_rows(os.path.join(output_dir, "q3")):
                if len(row) < 6:
                    continue
                cur.execute("""
                    INSERT INTO hourly_errors
                        (pipeline, run_uuid, batch_id, log_date, log_hour,
                         error_request_count, total_request_count, error_rate, distinct_error_hosts)
                    VALUES ('hive', %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    run_uuid,
                    batch_id,
                    row[0],
                    int(row[1]),
                    int(row[2]),
                    int(row[3]),
                    float(row[4]),
                    int(row[5]),
                ))
    conn.commit()


def _build_hive_env():
    """Build environment dict with JAVA_HOME, HADOOP_HOME, HIVE_HOME for subprocess."""
    env = os.environ.copy()
    env.pop("DEBUG", None)
    # Ensure JAVA_HOME is set
    if "JAVA_HOME" not in env:
        for java_path in [
            "/usr/lib/jvm/java-8-openjdk-amd64",
            "/usr/lib/jvm/java-11-openjdk-amd64",
        ]:
            if os.path.isdir(java_path):
                env["JAVA_HOME"] = java_path
                break
    # Ensure HADOOP_HOME is set
    if "HADOOP_HOME" not in env:
        home_hadoop = os.path.join(os.path.expanduser("~"), "hadoop")
        if os.path.isdir(home_hadoop):
            env["HADOOP_HOME"] = home_hadoop
    # Ensure HIVE_HOME is set
    if "HIVE_HOME" not in env:
        home_hive = os.path.join(os.path.expanduser("~"), "hive")
        if os.path.isdir(home_hive):
            env["HIVE_HOME"] = home_hive
            
    # Increase heap space to prevent OOM errors in local MapReduce jobs
    if "HADOOP_CLIENT_OPTS" in env:
        env["HADOOP_CLIENT_OPTS"] = env["HADOOP_CLIENT_OPTS"] + " -Xmx2g"
    else:
        env["HADOOP_CLIENT_OPTS"] = "-Xmx2g"
        
    return env


def run_hive(hive_bin, hql_path):
    # Run Hive from a temp directory with a custom hive-site.xml that forces
    # local filesystem mode and an embedded Derby metastore.  Each invocation
    # gets its own working directory so concurrent runs don't collide.
    cwd = tempfile.mkdtemp(prefix="hive_cwd_")
    warehouse_dir = os.path.join(cwd, "warehouse")
    metastore_db = os.path.join(cwd, "metastore_db")
    conf_dir = os.path.join(cwd, "conf")
    os.makedirs(warehouse_dir, exist_ok=True)
    os.makedirs(conf_dir, exist_ok=True)

    derby_url = f"jdbc:derby:;databaseName={metastore_db};create=true"

    hive_site = f"""<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>file:///</value>
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
    <value>{warehouse_dir}</value>
  </property>
  <property>
    <name>hive.exec.scratchdir</name>
    <value>{os.path.join(cwd, "scratch")}</value>
  </property>
  <property>
    <name>hive.exec.local.scratchdir</name>
    <value>{os.path.join(cwd, "local_scratch")}</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>{derby_url}</value>
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
  <property>
    <name>hive.metastore.uris</name>
    <value/>
  </property>
  <property>
    <name>hive.server2.enable.doAs</name>
    <value>false</value>
  </property>
</configuration>
"""
    with open(os.path.join(conf_dir, "hive-site.xml"), "w") as fh:
        fh.write(hive_site)

    env = _build_hive_env()
    env["HIVE_CONF_DIR"] = conf_dir

    result = subprocess.run(
        [hive_bin, "-f", hql_path],
        capture_output=True,
        text=True,
        env=env,
        cwd=cwd,
    )
    shutil.rmtree(cwd, ignore_errors=True)
    return result


def run_pipeline(log_file_paths, batch_mode, batch_value, run_uuid, query="all"):
    if batch_mode not in {"records", "time"}:
        raise ValueError("batch_mode must be records or time")
    if query not in {"all", "q1", "q2", "q3"}:
        raise ValueError("query must be one of all, q1, q2, q3")
    if batch_value <= 0:
        raise ValueError("batch_value must be greater than 0")

    hive_bin = find_hive_bin()
    if not hive_bin:
        raise FileNotFoundError("Hive executable not found. Install Hive or set HIVE_HOME/HIVE_BIN.")

    start_time = time.time()
    work_dir = tempfile.mkdtemp(prefix="hive_etl_")
    output_dir = os.path.join(work_dir, "output")
    os.makedirs(output_dir, exist_ok=True)
    hql_path = os.path.join(work_dir, "pipeline.hql")
    conn = pg_connect()

    try:
        init_postgres(conn)
        start_run(conn, run_uuid, batch_mode, batch_value)
        for log_path in log_file_paths:
            if not os.path.isfile(log_path):
                raise FileNotFoundError(f"Log file not found: {log_path}")

        print("Hive ETL started")
        print(f"Run UUID: {run_uuid}")
        print(f"Batch mode: {batch_mode}")
        print(f"Batch value: {batch_value}")
        print(f"Input files: {log_file_paths}")

        with open(hql_path, "w", encoding="utf-8") as fh:
            fh.write(build_hql(run_uuid, log_file_paths, batch_mode, batch_value, output_dir))

        result = run_hive(hive_bin, hql_path)
        if result.stdout.strip():
            print(result.stdout.strip())
        if result.returncode != 0:
            if result.stderr.strip():
                print(result.stderr.strip())
            raise RuntimeError(f"Hive failed with exit code {result.returncode}")

        total_records, total_batches, malformed_count = load_hive_metadata(conn, run_uuid, output_dir)
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

        print("Hive ETL completed")
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
        shutil.rmtree(work_dir, ignore_errors=True)


def main():
    args = sys.argv[1:]
    query = "all"
    if len(args) == 2:
        log_file_paths = [args[0]]
        batch_mode = "records"
        batch_value = int(args[1])
        run_uuid = f"hive-cli-{int(time.time())}"
    elif len(args) >= 4:
        log_file_paths = parse_path_list(args[0])
        batch_mode = args[1]
        batch_value = int(args[2])
        run_uuid = args[3]
        query = args[4] if len(args) >= 5 else "all"
    else:
        print("Usage: python3 hive_pipeline.py <log_file_path_or_json_array> <batch_mode> <batch_value> <run_uuid>")
        print("Legacy: python3 hive_pipeline.py <log_file_path> <batch_size>")
        sys.exit(1)

    run_pipeline(log_file_paths, batch_mode, batch_value, run_uuid, query)


if __name__ == "__main__":
    main()
