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

- `mongo/` – MongoDB-based ETL + reporting load.
- `pig/` – Apache Pig-based ETL + reporting load.

## Mandatory Queries Covered

1. Daily traffic summary (`log_date`, `status_code`, `request_count`, `total_bytes`)
2. Top requested resources (top 20 by `request_count` with bytes + distinct hosts)
3. Hourly error analysis (`400–599` status range with error rate + distinct error hosts)

## Pig Approach (newly added)

See `pig/README.md` for setup and run commands.

Quick run:

```bash
cd pig
chmod +x run_pig_pipeline.sh report_from_postgres.sh
PG_CONN="postgresql://<user>:<pass>@<host>:<port>/<db>" ./run_pig_pipeline.sh /path/to/NASA_access_log_Jul95 50000
PG_CONN="postgresql://<user>:<pass>@<host>:<port>/<db>" ./report_from_postgres.sh <run_id>
```

## Mongo Approach

Mongo implementation is in `mongo/mongodb_pipeline.js`.