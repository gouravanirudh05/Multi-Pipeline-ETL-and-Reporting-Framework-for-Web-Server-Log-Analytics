# Multi-Pipeline ETL and Reporting Framework for Web Server Log Analytics

This repository implements the end-semester NoSQL project objective:

- Run equivalent ETL + analytics over NASA HTTP web logs using different execution backends.
- Preserve common parsing/cleaning/query semantics across pipelines.
- Batch process input records (record-count or time-window batching).
- Store aggregated query outputs and run metadata in PostgreSQL.

## Dataset

Use official NASA HTTP logs only:

- `https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz`
- `https://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz`

The pipeline reads raw log text directly. Manual preprocessing outside pipeline logic is not required.

## Implemented Pipelines

| Pipeline | Location | Technology |
|----------|----------|-----------|
| **MongoDB** | `pipelines/mongo/` | Node.js, MongoDB aggregation framework |
| **Pig** | `pig/` | Apache Pig 0.17.0 split `.pig` query scripts, local MapReduce execution over HDFS |
| **MapReduce** | `pipelines/mapreduce/` | Java Hadoop MapReduce, local execution over HDFS |
| **Hive** | `pipelines/hive/` | Apache Hive 3.1.3 split `.hql` scripts, local MapReduce execution over HDFS |

All pipelines support **record-count batching** and **time-window batching**.

## Mandatory Queries Covered

1. **Daily Traffic Summary**: `log_date`, `status_code`, `request_count`, `total_bytes`
2. **Top Requested Resources**: Top 20 by `request_count` with bytes + distinct hosts
3. **Hourly Error Analysis**: `400–599` status range with error rate + distinct error hosts

---

## Prerequisites

| Tool | Version | Required For | Install |
|------|---------|-------------|---------|
| **Java 8** (JDK) | 1.8.x | All Hadoop-based pipelines | `sudo apt install openjdk-8-jdk` |
| **Hadoop** | 3.x | MapReduce pipeline | Already at `~/hadoop` |
| **PostgreSQL** | 12+ | All pipelines (result store) | `sudo apt install postgresql` |
| **psql** | — | MapReduce pipeline | Comes with PostgreSQL |
| **Node.js** | 18+ | MongoDB pipeline | `sudo apt install nodejs npm` |
| **MongoDB** | 4.4+ | MongoDB pipeline only | `sudo apt install mongodb` or `sudo systemctl start mongod` |
| **Python 3** | 3.8+ | Server, Pig orchestrator, Hive orchestrator | System python3 |

## Setup

### 1. PostgreSQL Database

```bash
sudo -u postgres psql -c "CREATE USER sathish WITH PASSWORD 'welcome';"
sudo -u postgres psql -c "CREATE DATABASE nosql_etl_db OWNER sathish;"
```

Tables are auto-created by each pipeline on first run.

### 2. Python Dependencies

```bash
pip install fastapi uvicorn psycopg2-binary
```

### 3. MongoDB Dependencies (for MongoDB pipeline)

```bash
cd pipelines/mongo
npm install
cd ../..
```

### 4. Apache Pig Setup

```bash
# Run the one-time setup script
./pig/setup_pig.sh

# Add to ~/.bashrc:
export PIG_HOME=$HOME/pig
export PATH=$PIG_HOME/bin:$PATH

# Reload:
source ~/.bashrc

# Verify:
pig -version
```

### 5. Apache Hive Setup

```bash
# Run the one-time setup script
./pipelines/hive/setup_hive.sh

# Add to ~/.bashrc:
export HIVE_HOME=$HOME/hive
export PATH=$HIVE_HOME/bin:$PATH

# Reload:
source ~/.bashrc

# Verify:
hive --version
```

### 6. Ensure ~/.bashrc Has All Exports

After all setups, your `~/.bashrc` should include:

```bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

export HADOOP_HOME=~/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

export PIG_HOME=$HOME/pig
export PATH=$PIG_HOME/bin:$PATH

export HIVE_HOME=$HOME/hive
export PATH=$HIVE_HOME/bin:$PATH
```

---

## Running the System

### Docker Run (Portable)

Derby is not a separate service. Hive uses its embedded Derby metastore inside the
app container at `/app/.hive/metastore_db`; PostgreSQL is still the shared result
store used by the UI.

```bash
# 1. Put input logs in the mounted data directory.
mkdir -p data
cp access_log_Jul95 data/

# 2. Build and start Postgres, MongoDB, the backend, and HDFS-only Hadoop.
docker compose up --build
```

The app container starts only HDFS daemons. It intentionally does not start YARN;
Pig, Hive, and MapReduce run with `mapreduce.framework.name=local` while reading
and writing HDFS paths.

Open the UI with `index.html` and use container-visible log paths such as:

```text
/app/data/access_log_Jul95
```

Useful container checks:

```bash
docker compose exec app hdfs dfs -ls /
docker compose exec app hdfs dfs -ls /tmp/nasa-etl
docker compose exec app bash -lc './pig/run.sh "[\"/app/data/access_log_Jul95\"]" records 10000 pig-docker all'
docker compose exec app bash -lc './pipelines/hive/run.sh "[\"/app/data/access_log_Jul95\"]" records 10000 hive-docker all'
docker compose exec app bash -lc './pipelines/mapreduce/run.sh "[\"/app/data/access_log_Jul95\"]" records 10000 mr-docker all'
```

### Web UI (Recommended)

```bash
# 1. Start MongoDB (if using MongoDB pipeline)
sudo systemctl start mongod

# 2. Start the backend server
cd backend
source nosql_env/bin/activate   # if using virtualenv
python3 server.py
# Server starts at http://localhost:5050

# 3. Open the frontend
# Open index.html in any browser:
xdg-open index.html
```

In the UI:
1. Select a pipeline card (Pig / MapReduce / MongoDB / Hive)
2. Enter log file paths (one per line), e.g.:
   ```
   /home/gourav-anirudh/Desktop/Nosql_Final_Project/Multi-Pipeline-ETL-and-Reporting-Framework-for-Web-Server-Log-Analytics/access_log_Jul95
   /home/gourav-anirudh/Desktop/Nosql_Final_Project/Multi-Pipeline-ETL-and-Reporting-Framework-for-Web-Server-Log-Analytics/access_log_Aug95
   ```
3. Select a query (`All queries`, `Q1`, `Q2`, or `Q3`)
4. Choose batch mode: Records or Time window
5. Set batch value (e.g., 10000 records or 60 minutes)
6. Click **Run** — watch real-time logs in the Terminal tab
7. Results and batch metadata appear in the **Results** tab when done

### CLI Runs

```bash
# ── Pig ────────────────────────────────────────────────────────────
./pig/run.sh '["access_log_Jul95"]' records 10000 pig-demo-records
./pig/run.sh '["access_log_Jul95"]' time 3600 pig-demo-time

# ── MapReduce ──────────────────────────────────────────────────────
./pipelines/mapreduce/run.sh '["access_log_Jul95"]' records 10000 mr-demo-records
./pipelines/mapreduce/run.sh '["access_log_Jul95"]' time 3600 mr-demo-time

# ── MongoDB ────────────────────────────────────────────────────────
cd pipelines/mongo
node mongodb_pipeline.js '["../../access_log_Jul95"]' records 10000 mongo-demo-records
cd ../..

# ── Hive ───────────────────────────────────────────────────────────
./pipelines/hive/run.sh '["access_log_Jul95"]' records 10000 hive-demo-records
./pipelines/hive/run.sh '["access_log_Jul95"]' time 3600 hive-demo-time
```

### Batching Modes

| Mode | CLI Arg | Description |
|------|---------|------------|
| **Records** | `records <N>` | Every N raw log lines = 1 batch |
| **Time** | `time <seconds>` | Records grouped by timestamp windows of N seconds |

---

## Architecture

```
index.html (Browser UI)
    ↓ POST /api/run (SSE streaming)
backend/server.py (FastAPI :5050)
    ↓ subprocess
    ├── pig/run.sh                   → hdfs dfs -put → pig etl.pig → PostgreSQL
    ├── pipelines/mapreduce/run.sh   → hdfs dfs -put → hadoop jar → psql → PostgreSQL
    ├── pipelines/mongo/mongodb_pipeline.js → MongoDB → PostgreSQL
    └── pipelines/hive/run.sh        → hdfs dfs -put → hive -f *.hql → PostgreSQL
```

All pipelines write to the same PostgreSQL reporting schema. The evaluator-facing
tables are `run_metadata`, `batch_metadata`, `query_results`, and
`malformed_record_summary`. Query outputs include both `aggregate` rows
(`batch_id = 0`) and `per_batch` rows (real source batch IDs), so the UI can
show final analytics and batch-level processing side by side. The system also
keeps detailed query-specific tables: `etl_runs`, `malformed_records`,
`daily_traffic`, `top_resources`, and `hourly_errors`.
