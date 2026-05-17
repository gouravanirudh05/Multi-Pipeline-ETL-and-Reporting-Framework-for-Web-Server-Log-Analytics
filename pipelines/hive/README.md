# Hive Pipeline

This pipeline keeps Hive logic in committed `.hql` files:

- `setup.hql`: shared parsing, cleaning, and batching tables
- `metadata.hql`: batch/malformed-record outputs
- `q1_daily_traffic.hql`: daily traffic summary
- `q2_top_resources.hql`: top requested resources
- `q3_hourly_errors.hql`: hourly error analysis
- `cleanup.hql`: drops the temporary Hive database

`run.sh` stages raw logs into HDFS with `hdfs dfs -put`, executes the selected
Hive scripts with `hive -f` and `mapreduce.framework.name=local`, merges the Hive
output, and loads final aggregates into PostgreSQL. YARN is not required.

```bash
./pipelines/hive/run.sh '["/path/to/NASA_access_log_Jul95"]' records 10000 <run_uuid>
./pipelines/hive/run.sh '["/path/to/NASA_access_log_Jul95"]' time 3600 <run_uuid>
```

Environment:

- `HIVE_HOME` or `HIVE_BIN` may be used if `hive` is not on `PATH`.
- Start Hadoop/HDFS first. The runner uses `hdfs dfs` or `hadoop fs`.
- PostgreSQL defaults match the app: `PGHOST=127.0.0.1`, `PGPORT=5432`, `PGDATABASE=nosql_etl_db`, `PGUSER=sathish`, `PGPASSWORD=welcome`.
