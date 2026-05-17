# Hive Pipeline

This runner generates HiveQL for the NASA log ETL workload, executes it with `hive`,
and loads final aggregates into PostgreSQL.

```bash
python3 pipelines/hive/hive_pipeline.py '["/path/to/NASA_access_log_Jul95"]' records 10000 <run_uuid>
python3 pipelines/hive/hive_pipeline.py '["/path/to/NASA_access_log_Jul95"]' time 3600 <run_uuid>
```

Environment:

- `HIVE_HOME` or `HIVE_BIN` may be used if `hive` is not on `PATH`.
- PostgreSQL defaults match the app: `PGHOST=127.0.0.1`, `PGPORT=5432`, `PGDATABASE=nosql_etl_db`, `PGUSER=sathish`, `PGPASSWORD=welcome`.
