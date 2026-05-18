q1_grouped = GROUP valid BY (batch_id, log_date, status_code);
q1 = FOREACH q1_grouped GENERATE
    group.batch_id AS batch_id,
    group.log_date AS log_date,
    group.status_code AS status_code,
    COUNT(valid) AS request_count,
    SUM(valid.bytes_transferred) AS total_bytes;

q1_aggregate_grouped = GROUP valid BY (log_date, status_code);
q1_aggregate = FOREACH q1_aggregate_grouped GENERATE
    0 AS batch_id,
    group.log_date AS log_date,
    group.status_code AS status_code,
    COUNT(valid) AS request_count,
    SUM(valid.bytes_transferred) AS total_bytes;
q1_all = UNION q1_aggregate, q1;

STORE q1_all INTO '$OUTPUT/q1' USING PigStorage('\t');
