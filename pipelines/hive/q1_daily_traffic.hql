USE ${hivevar:DATABASE};

INSERT OVERWRITE DIRECTORY '${hivevar:OUTPUT_DIR}/q1'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT log_date, status_code, count(1), sum(bytes_transferred)
FROM valid_logs
GROUP BY log_date, status_code;
