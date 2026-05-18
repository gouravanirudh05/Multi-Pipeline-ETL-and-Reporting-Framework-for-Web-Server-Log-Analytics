# Multi-Pipeline ETL and Reporting Framework for Web Server Log Analytics

This repository implements a NoSQL end-semester project for processing NASA HTTP
access logs through multiple data-processing pipelines. The system provides one
common interface for running ETL and analytics using Apache Pig, Java
MapReduce, MongoDB, and Apache Hive, then stores execution metadata and query
outputs in a relational reporting database.

The project is built around a simple idea: the same raw web server logs should
be processed with the same parsing rules, batching semantics, and analytical
queries across all four execution technologies.

## Project Summary

- **Dataset:** NASA HTTP access logs for July 1995 and August 1995
- **Pipelines:** Pig, MapReduce, MongoDB, Hive
- **Frontend:** `index.html`
- **Backend:** FastAPI server in `backend/server.py`
- **Reporting database:** PostgreSQL
- **Queries implemented:** Daily traffic summary, top requested resources,
  hourly error analysis
- **Batching modes:** Calendar-month batching through the UI
## Features

- Unified browser interface for selecting pipeline, query, input files, batch
  mode, and aggregation mode.
- Four execution backends:
  - Apache Pig scripts
  - Java Hadoop MapReduce jobs
  - MongoDB aggregation pipeline
  - Apache HiveQL scripts
- Raw log ingestion and cleaning inside each selected pipeline.
- Shared parsing model based on the NASA access-log format.
- Batch-level metadata capture.
- Malformed-record counting and sample malformed-record storage.
- Common PostgreSQL reporting schema for all pipelines.
- Run history and result viewing through the frontend.

## Dataset

Use the NASA HTTP access logs:

- `NASA_access_log_Jul95`
- `NASA_access_log_Aug95`

The local working copy may use these filenames:

```text
access_log_Jul95
access_log_Aug95
```

The raw log format is:

```text
host - - [timestamp] "method resource protocol" status_code bytes
```

Each pipeline extracts the same logical fields:

- host
- timestamp
- log date
- log hour
- HTTP method
- resource path
- protocol
- status code
- bytes transferred
- batch ID

Malformed records are counted during pipeline execution. Missing byte values
represented as `-` are treated as zero bytes.

## Analytical Queries

| Query | Name | Output |
|---|---|---|
| Q1 | Daily Traffic Summary | `log_date`, `status_code`, `request_count`, `total_bytes` |
| Q2 | Top Requested Resources | `resource_path`, `request_count`, `total_bytes`, `distinct_host_count` |
| Q3 | Hourly Error Analysis | `log_date`, `log_hour`, `error_request_count`, `total_request_count`, `error_rate`, `distinct_error_hosts` |

All three queries are implemented across all four pipelines:

```text
3 queries x 4 pipelines = 12 execution combinations
```

## Architecture

```text
index.html
    |
    | POST /api/run
    v
backend/server.py  (FastAPI, port 5050)
    |
    +-- pipelines/pig/run.sh
    |      -> HDFS -> Pig -> PostgreSQL
    |
    +-- pipelines/mapreduce/run.sh
    |      -> HDFS -> Java MapReduce -> PostgreSQL
    |
    +-- pipelines/mongo/mongodb_pipeline.js
    |      -> MongoDB -> PostgreSQL
    |
    +-- pipelines/hive/run.sh
           -> HDFS -> HiveQL -> PostgreSQL
```

All pipelines write to the same reporting tables:

- `etl_runs`
- `batch_metadata`
- `malformed_record_summary`
- `malformed_records`
- `daily_traffic`
- `top_resources`
- `hourly_errors`

## Prerequisites

Install or configure the following tools depending on which pipelines you want
to run.

| Tool | Used For |
|---|---|
| Java 8 | Hadoop, Pig, Hive, MapReduce |
| Hadoop / HDFS | Pig, Hive, MapReduce |
| Apache Pig | Pig pipeline |
| Apache Hive | Hive pipeline |
| Python 3 | FastAPI backend |
| PostgreSQL | Reporting database |
| Node.js and npm | MongoDB pipeline |
| MongoDB | MongoDB pipeline |

Python dependencies:

```bash
pip install -r requirements.txt
```

MongoDB pipeline dependencies:

```bash
cd pipelines/mongo
npm install
cd ../..
```

## Environment Setup

Set the Hadoop-related environment variables before running Hadoop-based
pipelines:

```bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=$HOME/hadoop
export PIG_HOME=$HOME/pig
export HIVE_HOME=$HOME/hive
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PIG_HOME/bin:$HIVE_HOME/bin:$PATH
```

Start HDFS:

```bash
start-dfs.sh
```

Optional one-time setup helpers:

```bash
./pipelines/pig/setup_pig.sh
./pipelines/hive/setup_hive.sh
```

## PostgreSQL Setup

The code defaults to the following PostgreSQL connection:

```text
database: nosql_etl_db
user:     sathish
password: welcome
host:     localhost
port:     5432
```

Create the user and database if they do not already exist:

```bash
sudo -u postgres psql -c "CREATE USER sathish WITH PASSWORD 'welcome';"
sudo -u postgres psql -c "CREATE DATABASE nosql_etl_db OWNER sathish;"
```

Tables are created automatically by the backend and pipeline runners when
needed.

You can override database settings with environment variables:

```bash
export PGHOST=localhost
export PGPORT=5432
export PGDATABASE=nosql_etl_db
export PGUSER=sathish
export PGPASSWORD=welcome
```

## Running Through the Web Interface

Start the backend:

```bash
cd backend
python3 server.py
```

The backend runs at:

```text
http://localhost:5050
```

Open the frontend:

```bash
cd ..
xdg-open index.html
```

In the UI:

1. Select a pipeline: Pig, MapReduce, MongoDB, or Hive.
2. Enter one or more log file paths, one per line:

   ```text
   /home/gourav-anirudh/Desktop/Nosql_Final_Project/Multi-Pipeline-ETL-and-Reporting-Framework-for-Web-Server-Log-Analytics/access_log_Jul95
   /home/gourav-anirudh/Desktop/Nosql_Final_Project/Multi-Pipeline-ETL-and-Reporting-Framework-for-Web-Server-Log-Analytics/access_log_Aug95
   ```

3. Select query: `All`, `Q1`, `Q2`, or `Q3`.
4. Select batching mode:
   - `1 Month` for calendar-month batching
   - time-window mode where exposed by the UI
5. Select aggregation mode:
   - `global`
   - `per_batch`
6. Click **Run**.
7. View logs in the terminal panel.
8. View run metadata and query outputs in the results panel.

## CLI Usage

All runners follow this general argument format:

```bash
<runner> '<json_log_file_array>' <batch_mode> <batch_value> <run_uuid> [query] [aggregation_mode]
```

Allowed query values:

```text
all, q1, q2, q3
```

Allowed aggregation modes:

```text
global, per_batch
```

Allowed batch modes in runner scripts:

```text
records, time, calendar_month
```

For `calendar_month`, the runner normalizes the batch value internally, so
`calendar_month` can be passed as the value for readability.

### Pig

```bash
./pipelines/pig/run.sh '["access_log_Jul95","access_log_Aug95"]' calendar_month calendar_month pig-demo all global
```

### MapReduce

```bash
./pipelines/mapreduce/run.sh '["access_log_Jul95","access_log_Aug95"]' calendar_month calendar_month mr-demo all global
```

### MongoDB

Start MongoDB first:

```bash
sudo systemctl start mongod
```

Then run:

```bash
cd pipelines/mongo
node mongodb_pipeline.js '["../../access_log_Jul95","../../access_log_Aug95"]' calendar_month calendar_month mongo-demo all global
cd ../..
```

### Hive

```bash
./pipelines/hive/run.sh '["access_log_Jul95","access_log_Aug95"]' calendar_month calendar_month hive-demo all global
```

## Querying Results

Open PostgreSQL:

```bash
PGPASSWORD=welcome psql -h localhost -U sathish -d nosql_etl_db
```

Recent runs:

```sql
SELECT run_uuid, pipeline, batch_mode, aggregation_mode,
       total_records, total_batches, avg_batch_size,
       malformed_count, runtime_seconds, status,
       started_at, completed_at
FROM etl_runs
ORDER BY started_at DESC;
```

Runtime trend for completed full-dataset runs:

```sql
SELECT pipeline,
       COUNT(*) AS completed_full_runs,
       ROUND(AVG(runtime_seconds), 3) AS avg_runtime_seconds,
       ROUND(MIN(runtime_seconds), 3) AS min_runtime_seconds,
       ROUND(MAX(runtime_seconds), 3) AS max_runtime_seconds,
       ROUND(AVG(total_records::numeric / NULLIF(runtime_seconds, 0)), 2)
           AS avg_throughput_records_per_sec
FROM etl_runs
WHERE status = 'completed'
  AND total_records = 3461613
  AND batch_mode = 'calendar_month'
  AND aggregation_mode = 'global'
  AND runtime_seconds IS NOT NULL
GROUP BY pipeline
ORDER BY avg_runtime_seconds ASC;
```

Fetch results for one run:

```sql
SELECT * FROM batch_metadata WHERE run_uuid = '<run_uuid>' ORDER BY batch_id;
SELECT * FROM daily_traffic WHERE run_uuid = '<run_uuid>' ORDER BY log_date, status_code;
SELECT * FROM top_resources WHERE run_uuid = '<run_uuid>' ORDER BY request_count DESC;
SELECT * FROM hourly_errors WHERE run_uuid = '<run_uuid>' ORDER BY log_date, log_hour;
```

## Reporting Schema

### `etl_runs`

Stores one row per pipeline execution:

- run UUID
- pipeline name
- batch mode
- aggregation mode
- total records
- total batches
- average batch size
- malformed count
- runtime
- status
- start and completion timestamps

### `batch_metadata`

Stores batch-level statistics:

- run UUID
- pipeline
- batch ID
- batch size
- records processed
- malformed count
- timestamps

### Query Output Tables

- `daily_traffic` stores Q1 results.
- `top_resources` stores Q2 results.
- `hourly_errors` stores Q3 results.

### Malformed Record Tables

- `malformed_record_summary` stores malformed counts per run and batch.
- `malformed_records` stores sample malformed rows and reasons.

## Batching

The primary demonstration mode is calendar-month batching:

| Batch | Records |
|---|---|
| Batch 1 | July 1995 records |
| Batch 2 | August 1995 records |

Time-window batching groups records based on parsed timestamps. Record-count
batching is available in runner scripts for command-line testing.

Average batch size is computed as:

```text
average_batch_size = total_records_processed / total_batches
```

## Cross-Pipeline Consistency

To keep results comparable, all pipelines use the same logical parsing pattern:

```text
^(\S+) \S+ \S+ \[(.*?)\] "(\S+) (.*?) (\S+)" (\d{3}) (\S+)
```

Each pipeline extracts the same fields, applies equivalent malformed-record
rules, and computes the same query definitions. This allows Q1, Q2, and Q3
outputs to be compared across Pig, MapReduce, MongoDB, and Hive.

## Troubleshooting

### `hadoop`, `hdfs`, `pig`, or `hive` command not found

Check the environment variables:

```bash
echo $JAVA_HOME
echo $HADOOP_HOME
echo $PIG_HOME
echo $HIVE_HOME
echo $PATH
```

Then re-export them using the commands in the setup section.

### PostgreSQL connection fails

Check that PostgreSQL is running and that the configured database exists:

```bash
pg_isready -h localhost -p 5432
PGPASSWORD=welcome psql -h localhost -U sathish -d nosql_etl_db -c "SELECT 1;"
```

### MongoDB run fails

Start MongoDB:

```bash
sudo systemctl start mongod
```

Check connectivity:

```bash
mongosh --eval "db.runCommand({ ping: 1 })"
```

### Hive metastore or Derby issues

Remove stale local metastore lock files only if no Hive process is running.
Then rerun the Hive setup script if required:

```bash
./pipelines/hive/setup_hive.sh
```

## Notes

- The implementation uses PostgreSQL as the reporting database.
- The relational schema can be ported to MySQL with minor SQL type changes.
- Hadoop-based runners use HDFS and run jobs in local MapReduce mode, so YARN
  is not required for the local demonstration setup.
- The UI backend exposes only the batch modes supported by `backend/server.py`;
  the lower-level scripts support additional CLI modes for testing.
