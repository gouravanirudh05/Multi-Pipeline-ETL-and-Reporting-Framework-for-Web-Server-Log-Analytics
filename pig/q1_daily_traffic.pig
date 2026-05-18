q1_grouped = GROUP valid BY (log_date, status_code);
q1 = FOREACH q1_grouped GENERATE
    group.log_date AS log_date,
    group.status_code AS status_code,
    COUNT(valid) AS request_count,
    SUM(valid.bytes_transferred) AS total_bytes;

STORE q1 INTO '$OUTPUT/q1' USING PigStorage('\t');
