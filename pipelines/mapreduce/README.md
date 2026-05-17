# Java MapReduce Pipeline

This pipeline runs the NASA log ETL workload with Hadoop MapReduce in local mode.

```bash
./pipelines/mapreduce/run.sh '["/path/to/NASA_access_log_Jul95"]' records 10000 <run_uuid>
./pipelines/mapreduce/run.sh '["/path/to/NASA_access_log_Jul95"]' time 3600 <run_uuid>
```

Arguments:

- `records <N>`: record-count batches, where each batch has up to `N` raw log lines.
- `time <seconds>`: timestamp-window batches, where each non-empty time window becomes one sequential batch.

The Java driver runs MapReduce jobs for metadata, batching, daily traffic, top resources, and hourly errors, then loads final aggregates into PostgreSQL using `psql`. It reads database settings from `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, and `PGPASSWORD`, defaulting to the local app credentials.
