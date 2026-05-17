# Multi-Pipeline ETL and Reporting Framework for Web Server Log Analytics

This repository implements the end-semester NoSQL project objective:

- Run equivalent ETL + analytics over NASA HTTP web logs using different execution backends.
- Preserve common parsing/cleaning/query semantics across pipelines.
- Batch process input records.
- Store aggregated query outputs and run metadata in PostgreSQL.

## Dataset

Use official NASA HTTP logs only:

- `https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz`
- `https://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz`

The pipeline reads raw log text directly. Manual preprocessing outside pipeline logic is not required.

## Implemented Pipelines

- `pipelines/mongo/` – MongoDB-based ETL + reporting load.
- `pig/` – Apache Pig-based ETL + reporting load.
- `pipelines/mapreduce/` – Java Hadoop MapReduce ETL + reporting load.
- `pipelines/hive/` – HiveQL ETL + reporting load.

The interface supports record-count batching and time-window batching. Record mode follows
the project statement's "records per batch" definition. Time mode groups parsed log records
into sequential non-empty timestamp windows.

## Mandatory Queries Covered

1. Daily traffic summary (`log_date`, `status_code`, `request_count`, `total_bytes`)
2. Top requested resources (top 20 by `request_count` with bytes + distinct hosts)
3. Hourly error analysis (`400–599` status range with error rate + distinct error hosts)

## CLI Runs

Run these from the repository root after PostgreSQL is available.

```bash
python3 pig/orchestrator.py '["/path/to/NASA_access_log_Jul95"]' records 10000 pig-demo-records
python3 pig/orchestrator.py '["/path/to/NASA_access_log_Jul95"]' time 3600 pig-demo-time

./pipelines/mapreduce/run.sh '["/path/to/NASA_access_log_Jul95"]' records 10000 mr-demo-records
./pipelines/mapreduce/run.sh '["/path/to/NASA_access_log_Jul95"]' time 3600 mr-demo-time

python3 pipelines/hive/hive_pipeline.py '["/path/to/NASA_access_log_Jul95"]' records 10000 hive-demo-records
python3 pipelines/hive/hive_pipeline.py '["/path/to/NASA_access_log_Jul95"]' time 3600 hive-demo-time
```

## Mongo Approach

Mongo implementation is in `pipelines/mongo/mongodb_pipeline.js`.
