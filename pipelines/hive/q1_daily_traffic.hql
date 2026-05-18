USE ${hivevar:DATABASE};

INSERT OVERWRITE DIRECTORY '${hivevar:OUTPUT_DIR}/q1'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT batch_id, log_date, status_code, count(1), sum(bytes_transferred)
FROM valid_logs
GROUP BY batch_id, log_date, status_code
UNION ALL
SELECT 0 AS batch_id, log_date, status_code, count(1), sum(bytes_transferred)
FROM valid_logs
GROUP BY log_date, status_code;
