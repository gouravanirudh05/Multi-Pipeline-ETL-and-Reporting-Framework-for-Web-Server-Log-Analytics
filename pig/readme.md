# Pig Pipeline

The Pig pipeline is split into query-specific scripts:

- `common.pig`: shared parsing, cleaning, and batching relations
- `metadata.pig`: batch/malformed-record outputs
- `q1_daily_traffic.pig`: daily traffic summary
- `q2_top_resources.pig`: top requested resources
- `q3_hourly_errors.pig`: hourly error analysis

`pig/run.sh` stages raw logs into HDFS with `hdfs dfs -put`, assembles the selected
Pig scripts, executes them with `mapreduce.framework.name=local`, merges the Pig
output, and loads final TSVs into PostgreSQL. YARN is not required.

```bash
./pig/run.sh '["/path/to/NASA_access_log_Jul95"]' records 10000 pig-demo-records
./pig/run.sh '["/path/to/NASA_access_log_Jul95"]' time 3600 pig-demo-time
```

Environment:

- Use `PIG_HOME`, `PIG_BIN`, or put `pig` on `PATH`.
- Start Hadoop/HDFS first. The runner uses `hdfs dfs` or `hadoop fs`.
- PostgreSQL defaults match the app: `PGHOST=127.0.0.1`, `PGPORT=5432`, `PGDATABASE=nosql_etl_db`, `PGUSER=sathish`, `PGPASSWORD=welcome`.
