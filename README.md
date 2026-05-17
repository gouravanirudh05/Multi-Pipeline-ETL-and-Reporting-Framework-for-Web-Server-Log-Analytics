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
| **Pig** | `pig/` | Apache Pig 0.17.0 (local mode), Jython UDFs |
| **MapReduce** | `pipelines/mapreduce/` | Java, Hadoop MapReduce (local mode) |
| **Hive** | `pipelines/hive/` | Apache Hive 3.1.3, HiveQL (local mode) |

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
3. Choose batch mode: Records or Time window
4. Set batch value (e.g., 10000 records or 60 minutes)
5. Click **Run** — watch real-time logs in the Terminal tab
6. Results appear in the **Results** tab when done

### CLI Runs

```bash
# ── Pig ────────────────────────────────────────────────────────────
python3 pig/orchestrator.py '["access_log_Jul95"]' records 10000 pig-demo-records
python3 pig/orchestrator.py '["access_log_Jul95"]' time 3600 pig-demo-time

# ── MapReduce ──────────────────────────────────────────────────────
./pipelines/mapreduce/run.sh '["access_log_Jul95"]' records 10000 mr-demo-records
./pipelines/mapreduce/run.sh '["access_log_Jul95"]' time 3600 mr-demo-time

# ── MongoDB ────────────────────────────────────────────────────────
cd pipelines/mongo
node mongodb_pipeline.js '["../../access_log_Jul95"]' records 10000 mongo-demo-records
cd ../..

# ── Hive ───────────────────────────────────────────────────────────
python3 pipelines/hive/hive_pipeline.py '["access_log_Jul95"]' records 10000 hive-demo-records
python3 pipelines/hive/hive_pipeline.py '["access_log_Jul95"]' time 3600 hive-demo-time
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
    ├── pig/orchestrator.py          → pig -x local → PostgreSQL
    ├── pipelines/mapreduce/run.sh   → hadoop jar   → psql → PostgreSQL
    ├── pipelines/mongo/mongodb_pipeline.js → MongoDB → PostgreSQL
    └── pipelines/hive/hive_pipeline.py     → hive -f → PostgreSQL
```

All pipelines write to the same PostgreSQL tables: `etl_runs`, `daily_traffic`, `top_resources`, `hourly_errors`.
