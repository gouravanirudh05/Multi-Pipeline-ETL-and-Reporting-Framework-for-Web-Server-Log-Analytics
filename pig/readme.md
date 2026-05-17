# Pig Pipeline

The Pig pipeline uses `pig/etl.pig` for parsing, cleaning, and the three required
analytics queries. `pig/orchestrator.py` handles run metadata, batch counting, and
loading the final Pig outputs into PostgreSQL.

```bash
python3 pig/orchestrator.py '["/path/to/NASA_access_log_Jul95"]' records 10000 pig-demo-records
python3 pig/orchestrator.py '["/path/to/NASA_access_log_Jul95"]' time 3600 pig-demo-time
```

Environment:

- Use `PIG_HOME`, `PIG_BIN`, or put `pig` on `PATH`.
- PostgreSQL defaults match the app: `PGHOST=127.0.0.1`, `PGPORT=5432`, `PGDATABASE=nosql_etl_db`, `PGUSER=sathish`, `PGPASSWORD=welcome`.
